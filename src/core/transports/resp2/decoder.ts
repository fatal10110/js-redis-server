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
   * version (the multibulk element-count bound, how a header line is found,
   * how inline quotes end). Omitted, the decoder applies the Redis 7.0+ rules,
   * matching the default profile.
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
 * Redis' `PROTO_INLINE_MAX_SIZE` (64KB on every supported version): how many
 * bytes may sit in the buffer, counted from the start of the request being
 * parsed, while the line that request needs has not been found. It bounds the
 * inline request line and, despite the name, the `*<count>` and `$<length>`
 * header lines of a multibulk request too.
 */
const INLINE_MAX_SIZE = 64 * 1024

const CR = 0x0d
const LF = 0x0a
const NUL = 0x00

const EMPTY = Buffer.alloc(0)

/** The most buffer capacity kept around once everything buffered is parsed. */
const RETAINED_CAPACITY = 1024 * 1024

/** A multibulk request whose elements have not all arrived yet. */
type PendingMultibulk = {
  /** Elements still to read. Redis' `c->multibulklen`. */
  remaining: number
  items: Buffer[]
  /**
   * The length from the current element's `$` header once it has been read,
   * `-1` while that header is still to come. Redis' `c->bulklen`.
   */
  bulkLength: number
}

/**
 * Incremental RESP2 request parser, modelled on Redis' `processInputBuffer`.
 *
 * The parser keeps the same state Redis keeps on the client — a read position
 * into the query buffer, plus the element count and current bulk length of a
 * multibulk request still in flight — so each byte is examined once however
 * the request is split across reads. Re-parsing an incomplete request from its
 * first byte on every chunk made a large pipelined multibulk quadratic (#505).
 */
export class Resp2CommandDecoder {
  /** Holds the unread bytes in `[start, end)`; capacity beyond `end` is spare. */
  private storage: Buffer = EMPTY
  /** The parse position: Redis' `c->qb_pos`. */
  private start = 0
  private end = 0
  private pending: PendingMultibulk | null = null
  /**
   * Where the current line search resumes, so a line arriving a few bytes at a
   * time is not rescanned from its start on every chunk. Only valid while
   * {@link scanOrigin} is the position the search started from; any move of
   * the parse position invalidates it.
   */
  private scanOrigin = -1
  private scanFrom = 0
  private readonly maxBulkLength: () => bigint
  private readonly maxMultibulkCount: bigint
  private readonly headerScanPastNul: boolean
  private readonly inlineAdjacentQuotes: boolean
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
    this.headerScanPastNul =
      options.profile?.has('protocol.header-scan-past-nul') === true
    this.inlineAdjacentQuotes =
      options.profile?.has('protocol.inline-adjacent-quotes') === true
  }

  /**
   * Append freshly-read bytes; call {@link next} to drain complete frames.
   *
   * Amortised O(chunk): the chunk is copied in (so the caller may reuse it),
   * the buffer grows geometrically, and only the unread tail is ever moved —
   * never everything the connection has sent since the request began.
   *
   * Ignored once the decoder has raised a protocol error — see
   * {@link fatalError}. The connection is being torn down at that point, and
   * buffering more of a stream we can no longer frame would be pointless.
   */
  push(chunk: Buffer): void {
    if (this.fatalError || chunk.length === 0) {
      return
    }

    if (this.end + chunk.length > this.storage.length) {
      this.reallocate(this.end - this.start + chunk.length)
    }
    chunk.copy(this.storage, this.end)
    this.end += chunk.length
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
      return this.parseNext()
    } catch (err) {
      if (err instanceof Resp2ParseError) {
        this.fatalError = err
        this.storage = EMPTY
        this.start = 0
        this.end = 0
        this.pending = null
      }
      throw err
    }
  }

  private parseNext(): Resp2CommandFrame | null {
    for (;;) {
      if (this.pending) {
        const frame = this.continueMultibulk(this.pending)
        if (frame) {
          this.pending = null
        }
        this.releaseIfDrained()
        return frame
      }

      if (this.start === this.end) {
        this.releaseIfDrained()
        return null
      }

      // Redis decides the request type from the first byte alone
      // (`processInputBuffer`): `*` is multibulk, anything else inline.
      const outcome =
        this.storage[this.start] === 0x2a
          ? this.beginMultibulk()
          : this.parseInline()
      if (outcome === 'incomplete') {
        return null
      }
      if (outcome !== 'skip' && outcome !== 'started') {
        this.releaseIfDrained()
        return outcome
      }
    }
  }

  /**
   * Read a multibulk request's `*<count>` header line (networking.c,
   * `processMultibulkBuffer`, the `c->multibulklen == 0` branch).
   */
  private beginMultibulk(): 'incomplete' | 'skip' | 'started' {
    const lineEnd = this.findHeaderLineEnd()
    if (lineEnd === -1) {
      if (this.end - this.start > INLINE_MAX_SIZE) {
        throw new Resp2ParseError('Protocol error: too big mbulk count string')
      }
      return 'incomplete'
    }

    // Redis bounds the element count as well as each bulk, but only from above:
    // `ll <= 0` (including `*-5`) is skipped like `*0`, never an error. The
    // bound is version-specific — see `maxMultibulkCount`.
    const count = parseLength(
      this.storage.subarray(this.start + 1, lineEnd),
      'multibulk',
    )
    if (count > this.maxMultibulkCount) {
      throw new Resp2ParseError('Protocol error: invalid multibulk length')
    }

    // The byte after the CR is skipped without being looked at, exactly as
    // Redis does (`qb_pos = newline - querybuf + 2`): `*1\rX` reads as `*1\r\n`.
    this.advanceTo(lineEnd + 2)
    if (count <= 0n) {
      return 'skip'
    }

    this.pending = { remaining: Number(count), items: [], bulkLength: -1 }
    return 'started'
  }

  /**
   * Read as many elements of the pending multibulk as are buffered (the
   * `while (c->multibulklen)` loop of `processMultibulkBuffer`).
   */
  private continueMultibulk(
    pending: PendingMultibulk,
  ): Resp2CommandFrame | null {
    while (pending.remaining > 0) {
      if (pending.bulkLength === -1) {
        const lineEnd = this.findHeaderLineEnd()
        if (lineEnd === -1) {
          // Checked before the `$`: a buffer of 64KB+ with no CR is refused
          // with this error even when its first byte is not `$`.
          if (this.end - this.start > INLINE_MAX_SIZE) {
            throw new Resp2ParseError(
              'Protocol error: too big bulk count string',
            )
          }
          return null
        }

        const prefix = this.storage[this.start]!
        if (prefix !== 0x24) {
          throw unexpectedBulkPrefix(prefix)
        }

        // Redis' primary `proto-max-bulk-len` enforcement point: the header is
        // judged before a single byte of the payload is read, so an oversized
        // argument to *any* command is refused at parse time rather than by the
        // command that would have received it (networking.c,
        // `processMultibulkBuffer`). `>` and not `>=` — a bulk exactly the size
        // of the limit is accepted. Verified on redis 6.2.24, 7.2.16 and 8.0.6.
        const length = parseLength(
          this.storage.subarray(this.start + 1, lineEnd),
          'bulk',
        )
        if (length < 0n || length > this.maxBulkLength()) {
          throw new Resp2ParseError('Protocol error: invalid bulk length')
        }

        this.advanceTo(lineEnd + 2)
        pending.bulkLength = Number(length)
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
      if (this.end - this.start < pending.bulkLength + 2) {
        return null
      }

      const valueStart = this.start
      const valueEnd = valueStart + pending.bulkLength
      pending.items.push(
        Buffer.from(this.storage.subarray(valueStart, valueEnd)),
      )
      this.advanceTo(valueEnd + 2)
      pending.bulkLength = -1
      pending.remaining -= 1
    }

    const [command, ...args] = pending.items
    return { command: command!, args }
  }

  /**
   * Parse one inline request (networking.c, `processInlineBuffer`).
   */
  private parseInline(): Resp2CommandFrame | 'skip' | 'incomplete' {
    // An inline request ends at `\n`; a `\r` just before it is optional. Every
    // version finds it with `strchr`, so a NUL byte ahead of the `\n` hides it.
    const newline = this.findLineEnd(LF, true)
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
      if (this.end - this.start > INLINE_MAX_SIZE) {
        throw new Resp2ParseError('Protocol error: too big inline request')
      }
      return 'incomplete'
    }

    const lineEnd =
      newline > this.start && this.storage[newline - 1] === CR
        ? newline - 1
        : newline
    const parts = splitInlineArguments(
      this.storage.subarray(this.start, lineEnd),
      this.inlineAdjacentQuotes,
    )
    if (parts === null) {
      throw new Resp2ParseError('Protocol error: unbalanced quotes in request')
    }

    this.advanceTo(newline + 1)
    if (parts.length === 0) {
      return 'skip'
    }

    const [command, ...args] = parts
    return { command: command!, args }
  }

  /**
   * Find the CR ending the header line at the parse position, or `-1` while the
   * line is incomplete.
   *
   * Mirrors the header-line test in `processMultibulkBuffer`: the line runs to
   * the first CR, and the CR only counts once one more byte is buffered after
   * it. That byte is assumed to be the LF and is never checked.
   */
  private findHeaderLineEnd(): number {
    const cr = this.findLineEnd(CR, !this.headerScanPastNul)
    if (cr === -1 || cr > this.end - 2) {
      return -1
    }
    return cr
  }

  /**
   * The first `target` byte at or after the parse position, or `-1`.
   *
   * With `stopAtNul`, behaves like the C `strchr` Redis uses on its
   * NUL-terminated query buffer: a NUL byte ends the search, so a target past
   * it is not found. Without it, like Valkey 8.1+'s `memchr` over the buffered
   * length.
   */
  private findLineEnd(target: number, stopAtNul: boolean): number {
    if (this.scanOrigin !== this.start) {
      this.scanOrigin = this.start
      this.scanFrom = this.start
    }

    // A NUL already found ends every later search of this line, too.
    if (stopAtNul && this.storage[this.scanFrom] === NUL) {
      return -1
    }

    const view = this.storage.subarray(0, this.end)
    const hit = view.indexOf(target, this.scanFrom)
    if (stopAtNul) {
      const limit = hit === -1 ? this.end : hit
      const nul = view.subarray(this.scanFrom, limit).indexOf(NUL)
      if (nul !== -1) {
        this.scanFrom += nul
        return -1
      }
    }

    this.scanFrom = hit === -1 ? this.end : hit
    return hit
  }

  private advanceTo(position: number): void {
    this.start = position
    this.scanOrigin = -1
  }

  /**
   * Rewind a fully consumed buffer, and let go of a large one so a big request
   * does not keep its memory pinned until the connection closes.
   */
  private releaseIfDrained(): void {
    if (this.start !== this.end) {
      return
    }
    if (this.storage.length > RETAINED_CAPACITY) {
      this.storage = EMPTY
    }
    this.start = 0
    this.end = 0
    this.scanOrigin = -1
  }

  /**
   * Move the unread bytes to the front of a fresh buffer with room for at
   * least `needed` bytes. Doubling keeps the total copying linear in the bytes
   * pushed.
   */
  private reallocate(needed: number): void {
    const next = Buffer.allocUnsafe(Math.max(needed * 2, 4096))
    this.storage.copy(next, 0, this.start, this.end)
    const shift = this.start
    this.storage = next
    this.end -= shift
    this.start = 0
    if (this.scanOrigin !== -1) {
      this.scanOrigin -= shift
      this.scanFrom -= shift
    }
  }
}

/** C `isspace` in the C locale, which Redis never changes for `LC_CTYPE`. */
function isSpace(byte: number): boolean {
  return byte === 0x20 || (byte >= 0x09 && byte <= 0x0d)
}

function isHexDigit(byte: number): boolean {
  return (
    (byte >= 0x30 && byte <= 0x39) ||
    (byte >= 0x41 && byte <= 0x46) ||
    (byte >= 0x61 && byte <= 0x66)
  )
}

function hexDigitValue(byte: number): number {
  if (byte <= 0x39) {
    return byte - 0x30
  }
  return (byte | 0x20) - 0x61 + 10
}

const INLINE_ESCAPES: Record<number, number> = {
  0x6e /* n */: 0x0a,
  0x72 /* r */: 0x0d,
  0x74 /* t */: 0x09,
  0x62 /* b */: 0x08,
  0x61 /* a */: 0x07,
}

/**
 * Split an inline request line into arguments, byte for byte like Redis'
 * `sdssplitargs` (sds.c), or return `null` for unbalanced quotes.
 *
 * The rules, all verified on redis-server 7.0.15 and unchanged in sds.c from
 * 6.2 through 8.0:
 *  - Leading blanks are skipped with `isspace`: space, `\t`, `\n`, `\v`, `\f`
 *    and `\r`.
 *  - An unquoted argument ends only at space, `\t`, `\n` or `\r`; `\v` and `\f`
 *    inside one are kept (`a\vb` is one argument).
 *  - A quote anywhere in an argument opens a quoted section: `foo"bar baz"` is
 *    the single argument `foobar baz`.
 *  - Inside double quotes `\xHH`, `\n`, `\r`, `\t`, `\b`, `\a` and `\<c>` are
 *    escapes; inside single quotes only `\'` is.
 *  - A closing quote must be followed by `isspace` or the end of the line, else
 *    the request is refused. With `adjacentQuotes` (Valkey 9.0's `sdsparsearg`)
 *    it simply ends the quoted section and the argument continues.
 *
 * The line never holds a NUL byte: the `strchr` that found its newline would
 * have stopped there. The end of the line plays the part of C's terminator.
 */
function splitInlineArguments(
  line: Buffer,
  adjacentQuotes: boolean,
): Buffer[] | null {
  const length = line.length
  const at = (index: number): number => (index < length ? line[index]! : NUL)
  const output = Buffer.allocUnsafe(length)
  const result: Buffer[] = []
  let p = 0
  let written = 0

  for (;;) {
    while (p < length && isSpace(line[p]!)) {
      p += 1
    }
    if (p >= length) {
      return result
    }

    const argumentStart = written
    let inDouble = false
    let inSingle = false
    let done = false

    while (!done) {
      const byte = at(p)
      if (inDouble) {
        if (
          byte === 0x5c &&
          at(p + 1) === 0x78 &&
          isHexDigit(at(p + 2)) &&
          isHexDigit(at(p + 3))
        ) {
          output[written++] =
            hexDigitValue(at(p + 2)) * 16 + hexDigitValue(at(p + 3))
          p += 3
        } else if (byte === 0x5c && p + 1 < length) {
          p += 1
          const escaped = line[p]!
          output[written++] = INLINE_ESCAPES[escaped] ?? escaped
        } else if (byte === 0x22) {
          if (adjacentQuotes) {
            inDouble = false
          } else {
            if (p + 1 < length && !isSpace(line[p + 1]!)) {
              return null
            }
            done = true
          }
        } else if (p >= length) {
          return null
        } else {
          output[written++] = byte
        }
      } else if (inSingle) {
        if (byte === 0x5c && at(p + 1) === 0x27) {
          p += 1
          output[written++] = 0x27
        } else if (byte === 0x27) {
          if (adjacentQuotes) {
            inSingle = false
          } else {
            if (p + 1 < length && !isSpace(line[p + 1]!)) {
              return null
            }
            done = true
          }
        } else if (p >= length) {
          return null
        } else {
          output[written++] = byte
        }
      } else if (
        p >= length ||
        byte === 0x20 ||
        byte === LF ||
        byte === CR ||
        byte === 0x09
      ) {
        done = true
      } else if (byte === 0x22) {
        inDouble = true
      } else if (byte === 0x27) {
        inSingle = true
      } else {
        output[written++] = byte
      }

      if (p < length) {
        p += 1
      }
    }

    result.push(output.subarray(argumentStart, written))
  }
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
  const shown = prefix === CR || prefix === LF ? 0x20 : prefix
  const head = "Protocol error: expected '$', got '"
  return new Resp2ParseError(
    `${head}${String.fromCharCode(shown)}'`,
    Buffer.concat([Buffer.from(head), Buffer.from([shown]), Buffer.from("'")]),
  )
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
