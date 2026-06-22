export type RedisValue =
  | { kind: 'simple-string'; value: string }
  | { kind: 'bulk-string'; value: Buffer | null }
  | { kind: 'integer'; value: number | bigint }
  | { kind: 'double'; value: number }
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
  | { kind: 'error'; message: string; code?: string }

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
  double: (value: number): RedisValue => ({ kind: 'double', value }),
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
  error: (message: string, code?: string): RedisValue => ({
    kind: 'error',
    message,
    code,
  }),
}
