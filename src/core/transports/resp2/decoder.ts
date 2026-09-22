export type Resp2CommandFrame = {
  command: Buffer
  args: Buffer[]
}

export type Resp2CommandDecoderOptions = {
  /**
   * The live `proto-max-bulk-len`, read afresh for every bulk header so a
   * `CONFIG SET` takes effect on the very next command. Defaults to Redis'
   * compiled-in 512MB when the decoder is used without a server behind it.
   */
  maxBulkLength?: () => bigint
}

export class Resp2ParseError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'Resp2ParseError'
  }
}

type ParseOutcome =
  | { kind: 'frame'; frame: Resp2CommandFrame; nextIndex: number }
  | { kind: 'skip'; nextIndex: number }
  | { kind: 'incomplete' }

/**
 * Redis' compiled-in default for `proto-max-bulk-len`: 512MB. Duplicated from
 * `RedisServerState` rather than imported, so the transport layer keeps no
 * dependency on server state — the real value arrives through
 * {@link Resp2CommandDecoderOptions.maxBulkLength}.
 */
const DEFAULT_PROTO_MAX_BULK_LEN = 536870912n

export class Resp2CommandDecoder {
  private buffered = Buffer.alloc(0)
  private readonly maxBulkLength: () => bigint

  constructor(options?: Resp2CommandDecoderOptions) {
    this.maxBulkLength =
      options?.maxBulkLength ?? (() => DEFAULT_PROTO_MAX_BULK_LEN)
  }

  /** Append freshly-read bytes; call {@link next} to drain complete frames. */
  push(chunk: Buffer): void {
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
   * one, then reports the protocol error and closes the connection.
   */
  next(): Resp2CommandFrame | null {
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

    const count = parseLength(header.line, 'multibulk')
    if (count < -1) {
      throw new Resp2ParseError('Protocol error: invalid multibulk length')
    }

    if (count <= 0) {
      return { kind: 'skip', nextIndex: header.nextIndex }
    }

    const items: Buffer[] = []
    let cursor = header.nextIndex

    for (let i = 0; i < count; i++) {
      const prefix = this.buffered[cursor]
      if (prefix === undefined) {
        return { kind: 'incomplete' }
      }

      if (prefix !== 0x24) {
        throw new Resp2ParseError('Protocol error: expected bulk string')
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
      if (length < 0 || BigInt(length) > this.maxBulkLength()) {
        throw new Resp2ParseError('Protocol error: invalid bulk length')
      }

      const valueStart = bulkHeader.nextIndex
      const valueEnd = valueStart + length
      const lineEnd = valueEnd + 2
      if (this.buffered.length < lineEnd) {
        return { kind: 'incomplete' }
      }

      if (
        this.buffered[valueEnd] !== 0x0d ||
        this.buffered[valueEnd + 1] !== 0x0a
      ) {
        throw new Resp2ParseError('Protocol error: bulk string not terminated')
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
    const line = readLine(this.buffered, index)
    if (!line) {
      return { kind: 'incomplete' }
    }

    const parts = parseInlineArguments(line.line)

    if (parts.length === 0) {
      return { kind: 'skip', nextIndex: line.nextIndex }
    }

    const [command, ...args] = parts
    return {
      kind: 'frame',
      frame: { command, args },
      nextIndex: line.nextIndex,
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

function parseLength(value: Buffer, kind: string): number {
  const raw = value.toString()
  if (!/^-?\d+$/.test(raw)) {
    throw new Resp2ParseError(`Protocol error: invalid ${kind} length`)
  }

  const parsed = Number(raw)
  if (!Number.isSafeInteger(parsed)) {
    throw new Resp2ParseError(`Protocol error: invalid ${kind} length`)
  }

  return parsed
}
