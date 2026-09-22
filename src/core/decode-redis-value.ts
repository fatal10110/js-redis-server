import type { RedisValue } from './redis-value'
import { formatRedisDouble, type RespVersion } from './resp-encoder'

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
   * RESP version the connection served this reply under. RESP2 has no map,
   * double, boolean, big-number or pair type, so these reply kinds are shaped
   * by the protocol rather than by the client, and all of them derive from
   * this one bit:
   *
   *  - `map` — flat `[k, v, k, v, …]` on RESP2, an object on RESP3.
   *  - `map-pairs` — `[[k, v], …]` on RESP2, an object on RESP3.
   *  - `flat-pairs` (WITHSCORES / WITHVALUES) — flat `[k, v, k, v, …]` on
   *    RESP2, `[[k, v], …]` tuples on RESP3, so a RESP3 consumer iterating
   *    `for (const [field, value] of reply)` must not be handed a flat array.
   *  - `double` — the bulk string Redis formats on RESP2, a JS number on
   *    RESP3.
   *  - `big-number` — the digits as a bulk string on RESP2, a `bigint` on
   *    RESP3.
   *  - `boolean` — the `:1` / `:0` integer on RESP2, a JS boolean on RESP3.
   *
   * Every other kind decodes the same way at both versions. Each of the above
   * matches what `encodeRedisValue` puts on the wire at that version, and
   * therefore what a real client reads back off it. Mirrors
   * `encodeRedisValue`'s `{ version }`, and is read per reply: `HELLO` switches
   * it mid-connection. (ioredis is RESP2-only, so a real ioredis only ever sees
   * the RESP2 shapes.)
   *
   * A *curated* client method is a separate matter: node-redis' own `hGetAll` /
   * `configGet` `transformReply` builds its object from the RESP2 flat array as
   * readily as from the RESP3 map, so those return an object on both protocols.
   * Such a method decodes through {@link decodeRedisMapEntries} rather than
   * letting this switch reach it. See #414.
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
    case 'double': {
      // RESP3's `,` is the only double on the wire. RESP2 sends the same
      // number as a bulk string, so that is what a client reads back — a
      // ZSCORE is `"2.5"` there, not `2.5`.
      if (options.version === 3) {
        return value.value
      }
      const text = formatRedisDouble(value.value)
      return options.returnBuffers ? Buffer.from(text) : text
    }
    case 'boolean':
      // RESP2 has no boolean: the encoder writes the `:1` / `:0` integer, so
      // that is the number a client reads back. Only RESP3's `#t` / `#f` is a
      // JS boolean. (Reachable through Lua's `redis.setresp(3)`.)
      return options.version === 3 ? value.value : value.value ? 1 : 0
    case 'big-number': {
      // Same split as `double`: RESP3's `(` is the only big number on the
      // wire, and RESP2 sends the digits as a bulk string.
      if (options.version === 3) {
        return value.value
      }
      const digits = value.value.toString()
      return options.returnBuffers ? Buffer.from(digits) : digits
    }
    case 'array':
    case 'set':
      return value.items.map(decode)
    case 'push':
      return options.pushShape === 'tagged'
        ? [value.name, ...value.items.map(decode)]
        : value.items.map(decode)
    case 'map':
    case 'map-pairs':
      // Only RESP3's `%` is read back as an object. RESP2 has no map type, so
      // the encoder flattens a `map` into one array and writes a `map-pairs`
      // as an array of two-element arrays — and that is what a client sees.
      if (options.version === 3) {
        return decodeRedisMapEntries(value.entries, options)
      }
      if (value.kind === 'map') {
        return value.entries.flatMap(([key, val]) => [decode(key), decode(val)])
      }
      return value.entries.map(([key, val]) => [decode(key), decode(val)])
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

/**
 * Build the plain object a `map` / `map-pairs` reply's entries describe.
 *
 * {@link decodeRedisValue} uses this for the RESP3 shape, and a **curated**
 * client method calls it directly to stay protocol-independent: node-redis'
 * `hGetAll` / `configGet` `transformReply` assembles this object itself, from
 * the RESP2 flat array as readily as from the RESP3 map, so those methods
 * return an object on both protocols while the raw `sendCommand` path follows
 * the protocol. See #414.
 */
export function decodeRedisMapEntries(
  entries: readonly [RedisValue, RedisValue][],
  options: DecodeRedisValueOptions,
): { [key: string]: NativeRedisReply } {
  const out: { [key: string]: NativeRedisReply } = {}
  for (const [key, value] of entries) {
    out[decodeRedisKey(key)] = decodeRedisValue(value, options)
  }
  return out
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
    case 'double':
      // The Redis spelling, not JavaScript's: `inf`, not `Infinity`.
      return formatRedisDouble(value.value)
    case 'integer':
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
