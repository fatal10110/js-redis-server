export type RedisValue =
  | { kind: 'simple-string'; value: string }
  | { kind: 'bulk-string'; value: Buffer | null }
  | { kind: 'integer'; value: number | bigint }
  /**
   * `text`, when set, is the reply's exact spelling (a command whose Redis
   * reply is not `addReplyDouble()`, e.g. GEO coordinates); otherwise the
   * encoder spells `value` per profile with `formatRedisDouble`.
   */
  | { kind: 'double'; value: number; text?: string }
  | { kind: 'boolean'; value: boolean }
  | { kind: 'big-number'; value: bigint }
  | { kind: 'verbatim'; format: string; value: Buffer }
  | { kind: 'array'; items: RedisValue[] }
  | { kind: 'set'; items: RedisValue[] }
  | { kind: 'map'; entries: [RedisValue, RedisValue][] }
  | { kind: 'map-pairs'; entries: [RedisValue, RedisValue][] }
  | { kind: 'flat-pairs'; entries: [RedisValue, RedisValue][] }
  | { kind: 'push'; name: string; items: RedisValue[] }
  | { kind: 'null' }
  | { kind: 'null-array' }
  // `messageBytes` is set only when the body must reach the wire byte for
  // byte — an error that echoes a token the client sent (see
  // `unknownSubcommandError` in src/core/subcommand-errors.ts), whose bytes need not
  // be valid UTF-8. `message` is always the readable form of the same body.
  | {
      kind: 'error'
      message: string
      messageBytes?: Buffer
      code?: string
    }

export const RedisValue = {
  simpleString: (value: string): RedisValue => ({
    kind: 'simple-string',
    value,
  }),
  bulkString: (value: Buffer | null): RedisValue => ({
    kind: 'bulk-string',
    value,
  }),
  integer: (value: number | bigint): RedisValue => ({ kind: 'integer', value }),
  double: (value: number, text?: string): RedisValue =>
    text === undefined
      ? { kind: 'double', value }
      : { kind: 'double', value, text },
  boolean: (value: boolean): RedisValue => ({ kind: 'boolean', value }),
  bigNumber: (value: bigint): RedisValue => ({ kind: 'big-number', value }),
  verbatim: (format: string, value: Buffer): RedisValue => ({
    kind: 'verbatim',
    format,
    value,
  }),
  array: (items: RedisValue[]): RedisValue => ({ kind: 'array', items }),
  set: (items: RedisValue[]): RedisValue => ({ kind: 'set', items }),
  map: (entries: [RedisValue, RedisValue][]): RedisValue => ({
    kind: 'map',
    entries,
  }),
  mapPairs: (entries: [RedisValue, RedisValue][]): RedisValue => ({
    kind: 'map-pairs',
    entries,
  }),
  // Like map-pairs in RESP3 (an array of [k, v] pairs), but a *flat* array
  // [k, v, k, v, ...] in RESP2 — the shape sorted-set WITHSCORES /
  // HRANDFIELD WITHVALUES replies use (flat on RESP2, nested pairs on RESP3).
  flatPairs: (entries: [RedisValue, RedisValue][]): RedisValue => ({
    kind: 'flat-pairs',
    entries,
  }),
  push: (name: string, items: RedisValue[]): RedisValue => ({
    kind: 'push',
    name,
    items,
  }),
  null: (): RedisValue => ({ kind: 'null' }),
  nullArray: (): RedisValue => ({ kind: 'null-array' }),
  error: (message: string | Buffer, code?: string): RedisValue =>
    typeof message === 'string'
      ? { kind: 'error', message, code }
      : {
          kind: 'error',
          message: message.toString(),
          messageBytes: message,
          code,
        },
}
