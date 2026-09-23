import type { CompatibilityProfile } from '../../compatibility'

export type Resp2CommandFrame = {
  command: Buffer
  args: Buffer[]
}

export type Resp2CommandDecoderOptions = {
  /**
   * The live `proto-max-bulk-len`, read afresh for every bulk header so a
   * `CONFIG SET` takes effect on the very next command. Required: there is no
   * sensible default here, because the only correct value is the one the
   * server this connection belongs to is currently running with.
   */
  maxBulkLength: () => bigint
  /**
   * The server's compatibility profile, for the framing rules that differ by
   * version (the multibulk element-count bound). Omitted, the decoder applies
   * the 7.0+ rules, matching the default profile.
   */
  profile?: Pick<CompatibilityProfile, 'has'>
}

export class Resp2ParseError extends Error {
  /**
   * The error text exactly as it goes on the wire, as raw bytes.
   *
   * Usually just `message` encoded, but some Redis protocol errors echo an
   * arbitrary byte from the client (`expected '$', got '%c'`), and Redis sends
   * that byte as-is. Carried as a Buffer because a JS string would be
   * re-encoded as UTF-8 on the way out, turning a lone `0xE9` into `0xC3 0xA9`.
   */
  readonly messageBytes: Buffer

  constructor(message: string, messageBytes?: Buffer) {
    super(message)
    this.name = 'Resp2ParseError'
    this.messageBytes = messageBytes ?? Buffer.from(message)
  }
}

/**
 * Redis' ceiling on a multibulk element count (networking.c,
 * `processMultibulkBuffer`): `INT_MAX` since 7.0, `1024*1024` before it. Gated
 * by `protocol.multibulk-count-int-max`.
 */
const MAX_MULTIBULK_COUNT = 2147483647n
const PRE_7_0_MAX_MULTIBULK_COUNT = 1024n * 1024n

const INT64_MIN = -(2n ** 63n)
const INT64_MAX = 2n ** 63n - 1n

/**
 * Redis' `PROTO_INLINE_MAX_SIZE`: how many bytes of an inline request may sit
 * in the buffer with no newline before it is refused.
 */
const INLINE_MAX_SIZE = 64 * 1024

type ParseOutcome =
  | { kind: 'frame'; frame: Resp2CommandFrame; nextIndex: number }
  | { kind: 'skip'; nextIndex: number }
  | { kind: 'incomplete' }

export class Resp2CommandDecoder {
  private buffered = Buffer.alloc(0)
  private readonly maxBulkLength: () => bigint
  private readonly maxMultibulkCount: bigint
  /**
   * Set once a protocol error is raised. A RESP stream cannot be resynchronised
   * after one — Redis' own parser marks the client `CLIENT_CLOSE_AFTER_REPLY`
   * and never reads another command from it — so the decoder is deliberately
   * terminal rather than silently resuming mid-frame.
   */
  private fatalError: Resp2ParseError | null = null

  constructor(options: Resp2CommandDecoderOptions) {
    this.maxBulkLength = options.maxBulkLength
    this.maxMultibulkCount =
      options.profile?.has('protocol.multibulk-count-int-max') === false
        ? PRE_7_0_MAX_MULTIBULK_COUNT
        : MAX_MULTIBULK_COUNT
  }

  /**
   * Append freshly-read bytes; call {@link next} to drain complete frames.
   *
   * Ignored once the decoder has raised a protocol error — see
   * {@link fatalError}. The connection is being torn down at that point, and
   * buffering more of a stream we can no longer frame would be pointless.
   */
  push(chunk: Buffer): void {
    if (this.fatalError) {
      return
    }
    this.buffered = Buffer.concat([this.buffered, chunk])
  }

  /**
   * Take the next complete command frame off the buffer, or `null` when more
   * bytes are needed.
   *
   * Pull-based rather than "decode the whole chunk at once" because the bulk
   * limit is live: the caller runs each frame before asking for the next, so a
   * `CONFIG SET proto-max-bulk-len` applies to everything parsed after it, even
   * when it arrived in the same TCP read as the commands that follow it.
   *
   * @throws {Resp2ParseError} on a malformed frame. Frames already returned
   * stay valid — real Redis answers the good commands that preceded the bad
   * one, then reports the protocol error and closes the connection. **This is
   * terminal**: the decoder keeps the error and every later `next()` re-throws
   * it, so a caller that does not close the connection cannot accidentally
   * resume framing from the middle of a frame it never consumed.
   */
  next(): Resp2CommandFrame | null {
    if (this.fatalError) {
      throw this.fatalError
    }

    try {
      while (this.buffered.length > 0) {
        const outcome = this.parseFrame(0)
        if (outcome.kind === 'incomplete') {
          return null
        }

        this.buffered = this.buffered.subarray(outcome.nextIndex)
        if (outcome.kind === 'frame') {
          return outcome.frame
        }
      }
    } catch (err) {
      if (err instanceof Resp2ParseError) {
        this.fatalError = err
        this.buffered = Buffer.alloc(0)
      }
      throw err
    }

    return null
  }

  private parseFrame(index: number): ParseOutcome {
    const prefix = this.buffered[index]
    if (prefix === undefined) {
      return { kind: 'incomplete' }
    }

    if (prefix === 0x2a) {
      return this.parseArrayFrame(index)
    }

    return this.parseInlineFrame(index)
  }

  private parseArrayFrame(index: number): ParseOutcome {
    const header = readLine(this.buffered, index + 1)
    if (!header) {
      return { kind: 'incomplete' }
    }

    // Redis bounds the element count as well as each bulk, but only from above:
    // `ll <= 0` (including `*-5`) is skipped like `*0`, never an error. The
    // bound is version-specific — see `maxMultibulkCount`.
    const count = parseLength(header.line, 'multibulk')
    if (count > this.maxMultibulkCount) {
      throw new Resp2ParseError('Protocol error: invalid multibulk length')
    }

    if (count <= 0n) {
      return { kind: 'skip', nextIndex: header.nextIndex }
    }

    const items: Buffer[] = []
    let cursor = header.nextIndex

    const elementCount = Number(count)
    for (let i = 0; i < elementCount; i++) {
      const prefix = this.buffered[cursor]
      if (prefix === undefined) {
        return { kind: 'incomplete' }
      }

      if (prefix !== 0x24) {
        throw unexpectedBulkPrefix(prefix)
      }

      const bulkHeader = readLine(this.buffered, cursor + 1)
      if (!bulkHeader) {
        return { kind: 'incomplete' }
      }

      // Redis' primary `proto-max-bulk-len` enforcement point: the header is
      // judged before a single byte of the payload is read, so an oversized
      // argument to *any* command is refused at parse time rather than by the
      // command that would have received it (networking.c,
      // `processMultibulkBuffer`). `>` and not `>=` — a bulk exactly the size
      // of the limit is accepted. Verified on redis 6.2.24, 7.2.16 and 8.0.6.
      const length = parseLength(bulkHeader.line, 'bulk')
      if (length < 0n || length > this.maxBulkLength()) {
        throw new Resp2ParseError('Protocol error: invalid bulk length')
      }

      // Redis reads exactly `length` bytes and then skips two *without looking
      // at them*: a wrong terminator is not a protocol error, so
      // `*1\r\n$3\r\nfooXX` dispatches `foo`. Verified on redis 6.2.24,
      // 7.0.15, 7.2.4 and 8.0, and valkey 8.0.0 / 9.0.0.
      //
      // Known Valkey divergence, not modelled: Valkey patch releases on every
      // current line (7.2.14, 8.0.11, 9.0.6) do check the terminator and refuse
      // with `Protocol error: invalid CRLF in request`, then close. Only the
      // `x.0.0` releases skip it, and a `VersionGate` has one minimum per flavor,
      // so it cannot express a check backported across branches. The Valkey
      // presets are 8.0.0 and 9.0.0, which match this path.
      const valueStart = bulkHeader.nextIndex
      const valueEnd = valueStart + Number(length)
      const lineEnd = valueEnd + 2
      if (this.buffered.length < lineEnd) {
        return { kind: 'incomplete' }
      }

      items.push(Buffer.from(this.buffered.subarray(valueStart, valueEnd)))
      cursor = lineEnd
    }

    const [command, ...args] = items
    return {
      kind: 'frame',
      frame: { command, args },
      nextIndex: cursor,
    }
  }

  private parseInlineFrame(index: number): ParseOutcome {
    // An inline request ends at `\n`; a `\r` just before it is optional
    // (networking.c, `processInlineBuffer`).
    const newline = this.buffered.indexOf(0x0a, index)
    if (newline === -1) {
      // Redis' 64KB inline cap bounds an *unterminated* buffer, not a line's
      // length: it is checked only when the buffer holds no newline at all.
      // Deterministic cases verified on redis 6.2.24, 7.0.15, 8.0 and valkey
      // 7.2.14: 64KB + 1 unterminated bytes are refused, exactly 64KB waits, and
      // 64KB followed later by `a\r\n` is served. A >64KB line sent in one
      // write depends on how the server's reads split it: 6.2 and 8.0 read 16KB
      // at a time and refuse a 100KB line, while 7.0 and valkey 7.2 serve it.
      // Here the outcome likewise depends on Node's read chunks (typically
      // 64KB): a 100KB line in one write is served, because its newline arrives
      // in the second chunk, before the buffer ever holds >64KB without one.
      if (this.buffered.length - index > INLINE_MAX_SIZE) {
        throw new Resp2ParseError('Protocol error: too big inline request')
      }
      return { kind: 'incomplete' }
    }

    const lineEnd =
      newline > index && this.buffered[newline - 1] === 0x0d
        ? newline - 1
        : newline
    const parts = parseInlineArguments(this.buffered.subarray(index, lineEnd))

    if (parts.length === 0) {
      return { kind: 'skip', nextIndex: newline + 1 }
    }

    const [command, ...args] = parts
    return {
      kind: 'frame',
      frame: { command, args },
      nextIndex: newline + 1,
    }
  }
}

function parseInlineArguments(line: Buffer): Buffer[] {
  const source = line.toString()
  const result: Buffer[] = []
  let cursor = 0

  while (cursor < source.length) {
    while (cursor < source.length && isInlineWhitespace(source[cursor]!)) {
      cursor += 1
    }

    if (cursor >= source.length) {
      break
    }

    const quote = source[cursor]
    if (quote === '"' || quote === "'") {
      const parsed = parseQuotedInlineArgument(source, cursor, quote)
      result.push(parsed.value)
      cursor = parsed.nextIndex
      continue
    }

    const start = cursor
    while (cursor < source.length && !isInlineWhitespace(source[cursor]!)) {
      cursor += 1
    }
    result.push(Buffer.from(source.slice(start, cursor)))
  }

  return result
}

function parseQuotedInlineArgument(
  source: string,
  index: number,
  quote: string,
): { value: Buffer; nextIndex: number } {
  const bytes: number[] = []
  let cursor = index + 1

  while (cursor < source.length) {
    const char = source[cursor]!
    if (char === quote) {
      cursor += 1
      if (cursor < source.length && !isInlineWhitespace(source[cursor]!)) {
        throw new Resp2ParseError(
          'Protocol error: unbalanced quotes in request',
        )
      }
      return { value: Buffer.from(bytes), nextIndex: cursor }
    }

    if (char === '\\') {
      const parsed = parseInlineEscape(source, cursor)
      bytes.push(...parsed.bytes)
      cursor = parsed.nextIndex
      continue
    }

    bytes.push(char.charCodeAt(0))
    cursor += 1
  }

  throw new Resp2ParseError('Protocol error: unbalanced quotes in request')
}

function parseInlineEscape(
  source: string,
  index: number,
): { bytes: number[]; nextIndex: number } {
  const escaped = source[index + 1]
  if (escaped === undefined) {
    return { bytes: ['\\'.charCodeAt(0)], nextIndex: index + 1 }
  }

  if (
    escaped === 'x' &&
    isHexDigit(source[index + 2]) &&
    isHexDigit(source[index + 3])
  ) {
    return {
      bytes: [Number.parseInt(source.slice(index + 2, index + 4), 16)],
      nextIndex: index + 4,
    }
  }

  const replacements: Record<string, number> = {
    n: 0x0a,
    r: 0x0d,
    t: 0x09,
    b: 0x08,
    a: 0x07,
  }
  return {
    bytes: [replacements[escaped] ?? escaped.charCodeAt(0)],
    nextIndex: index + 2,
  }
}

function isInlineWhitespace(char: string): boolean {
  return char === ' ' || char === '\t'
}

function isHexDigit(char: string | undefined): boolean {
  return char !== undefined && /^[0-9a-fA-F]$/.test(char)
}

/**
 * Redis' `Protocol error: expected '$', got '%c'`, echoing the offending byte.
 *
 * The byte is echoed raw — a `0xE9` goes out as the single byte `0xE9`, not as
 * its two-byte UTF-8 form — except CR and LF, which Redis' error-reply path
 * rewrites to a space. Verified byte-for-byte on redis 6.2.24, 7.2.16 and
 * 8.0.6: `*1\r\n\xE9x\r\n` answers `got '\xE9'` and `*2\r\n%3\r\nfoo\r\n`
 * answers `got '%'`, both followed by a close.
 */
function unexpectedBulkPrefix(prefix: number): Resp2ParseError {
  const shown = prefix === 0x0d || prefix === 0x0a ? 0x20 : prefix
  const head = "Protocol error: expected '$', got '"
  return new Resp2ParseError(
    `${head}${String.fromCharCode(shown)}'`,
    Buffer.concat([Buffer.from(head), Buffer.from([shown]), Buffer.from("'")]),
  )
}

function readLine(
  buffer: Buffer,
  index: number,
): { line: Buffer; nextIndex: number } | null {
  const lineEnd = buffer.indexOf('\r\n', index)
  if (lineEnd === -1) {
    return null
  }

  return {
    line: buffer.subarray(index, lineEnd),
    nextIndex: lineEnd + 2,
  }
}

/**
 * Parse a multibulk count or bulk length the way Redis' `string2ll` does: a
 * canonical signed decimal within int64 range. No leading zeros, no `+`, and
 * no `-0`, so `*01`, `*-05`, `*-0`, `$04` and `$+4` are all protocol errors.
 * Verified on redis 6.2.24 and 8.0 and valkey 7.2.14.
 */
function parseLength(value: Buffer, kind: string): bigint {
  const raw = value.toString('latin1')
  if (!/^(0|-?[1-9]\d*)$/.test(raw)) {
    throw new Resp2ParseError(`Protocol error: invalid ${kind} length`)
  }

  const parsed = BigInt(raw)
  if (parsed < INT64_MIN || parsed > INT64_MAX) {
    throw new Resp2ParseError(`Protocol error: invalid ${kind} length`)
  }

  return parsed
}
