import type { RedisValue } from './redis-value'
import type { RespVersion } from './resp-encoder'

/**
 * Native JS value a {@link RedisValue} decodes to — the shape a real client
 * hands back to application code.
 */
export type NativeRedisReply =
  | string
  | number
  | bigint
  | boolean
  | Buffer
  | null
  | NativeRedisReply[]
  | { [key: string]: NativeRedisReply }

/**
 * Everything {@link decodeRedisValue} needs: the points where two socketless
 * clients over this pipeline legitimately disagree about how to present a reply
 * ({@link ClientDecodeOptions}), plus the connection's negotiated RESP version.
 * Everything else — bulk strings, nulls — decodes identically, so it lives in
 * {@link decodeRedisValue} once.
 */
export type DecodeRedisValueOptions = ClientDecodeOptions & {
  /**
   * RESP version the connection served this reply under. Some replies are
   * shaped by the protocol rather than by the client: `flat-pairs`
   * (WITHSCORES / WITHVALUES) is a flat `[k, v, k, v, …]` array on RESP2 and
   * `[[k, v], …]` tuples on RESP3, so a RESP3 consumer iterating
   * `for (const [field, value] of reply)` must not be handed a flat array.
   *
   * Mirrors `encodeRedisValue`'s `{ version }`, and is read per reply: `HELLO`
   * switches it mid-connection. (ioredis is RESP2-only, so a real ioredis only
   * ever sees the RESP2 shapes.)
   */
  version: RespVersion
}

/**
 * The half of {@link DecodeRedisValueOptions} that belongs to the *client*
 * rather than to the connection. A client pins these once; `version` comes off
 * its session at decode time.
 */
export type ClientDecodeOptions = {
  /**
   * How an integer reply that arrived as a `bigint` is narrowed.
   *  - `'always'`: node-redis parses a RESP2 `:` with plain JS number
   *    arithmetic, so the reply is a `number` — precision loss past 2^53
   *    included. Only RESP3's `(` BIG_NUMBER yields a bigint there.
   *  - `'when-safe'`: widen to `bigint` past `Number.MAX_SAFE_INTEGER`. This is
   *    the *less* faithful option — no real client does it, they all lose
   *    precision past 2^53 — and it is a deliberate lossless-over-faithful
   *    choice for the socketless client, whose callers read replies directly
   *    rather than comparing against a real client's output.
   */
  narrowBigInt: 'always' | 'when-safe'
  /**
   * Shape of a `push` reply.
   *  - `'items'`: just the payload items; the type tag is consumed by the
   *    client's own push routing (node-redis).
   *  - `'tagged'`: `[name, ...items]` — the RESP2 wire shape, so a raw
   *    push-mode consumer sees what a real client would read off the socket.
   */
  pushShape: 'items' | 'tagged'
  /** Builds the error thrown for an `error` reply, from its on-the-wire text. */
  error: (text: string, code?: string) => Error
  /** Return `Buffer`s for bulk-string/verbatim replies instead of utf8 strings. */
  returnBuffers?: boolean
}

/**
 * The full on-the-wire error text a real client sees — `<CODE> <message>`
 * (e.g. `MOVED 1234 host:port`, `WRONGTYPE Operation …`), not just the detail.
 */
export function redisErrorText(value: {
  code?: string
  message: string
}): string {
  return value.code ? `${value.code} ${value.message}` : value.message
}

/**
 * Decode a {@link RedisValue} into the native JS reply a client would surface.
 * `error` replies are thrown, not returned, because that is what a client does
 * with them; {@link DecodeRedisValueOptions.error} decides the class.
 */
export function decodeRedisValue(
  value: RedisValue,
  options: DecodeRedisValueOptions,
): NativeRedisReply {
  const decode = (item: RedisValue) => decodeRedisValue(item, options)

  switch (value.kind) {
    case 'simple-string':
      return value.value
    case 'bulk-string':
      if (value.value === null) {
        return null
      }
      return options.returnBuffers ? value.value : value.value.toString('utf8')
    case 'verbatim':
      return options.returnBuffers ? value.value : value.value.toString('utf8')
    case 'integer':
      if (typeof value.value !== 'bigint') {
        return value.value
      }
      if (options.narrowBigInt === 'always' || isSafeBigInt(value.value)) {
        return Number(value.value)
      }
      return value.value
    case 'double':
      return value.value
    case 'boolean':
      return value.value
    case 'big-number':
      return value.value
    case 'array':
    case 'set':
      return value.items.map(decode)
    case 'push':
      return options.pushShape === 'tagged'
        ? [value.name, ...value.items.map(decode)]
        : value.items.map(decode)
    case 'map':
    case 'map-pairs': {
      const out: { [key: string]: NativeRedisReply } = {}
      for (const [key, val] of value.entries) {
        out[decodeRedisKey(key)] = decode(val)
      }
      return out
    }
    case 'flat-pairs':
      // `[[k, v], …]` on RESP3, flat `[k, v, k, v, …]` on RESP2 — the same
      // split `encodeRedisValue` puts on the wire.
      if (options.version === 3) {
        return value.entries.map(([key, val]) => [decode(key), decode(val)])
      }
      return value.entries.flatMap(([key, val]) => [decode(key), decode(val)])
    case 'null':
    case 'null-array':
      return null
    case 'error':
      throw options.error(redisErrorText(value), value.code)
  }
}

/** Map keys are always plain strings, regardless of `returnBuffers`. */
export function decodeRedisKey(value: RedisValue): string {
  switch (value.kind) {
    case 'simple-string':
      return value.value
    case 'bulk-string':
      return value.value === null ? '' : value.value.toString('utf8')
    case 'verbatim':
      return value.value.toString('utf8')
    case 'integer':
    case 'double':
    case 'big-number':
      return String(value.value)
    case 'boolean':
      return String(value.value)
    default:
      return ''
  }
}

/** Coerce a client-supplied command argument to its wire `Buffer`. */
export function toRedisArgument(arg: string | number | Buffer): Buffer {
  if (Buffer.isBuffer(arg)) {
    return arg
  }
  return Buffer.from(typeof arg === 'number' ? String(arg) : arg)
}

function isSafeBigInt(value: bigint): boolean {
  return (
    value >= BigInt(Number.MIN_SAFE_INTEGER) &&
    value <= BigInt(Number.MAX_SAFE_INTEGER)
  )
}
