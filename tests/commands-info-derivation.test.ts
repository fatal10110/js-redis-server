import { describe, test } from 'node:test'
import assert from 'node:assert'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import {
  defineCommand,
  type CommandDefinition,
} from '../src/core/command-definition'
import { schemaKeyRange } from '../src/core/command-schema'
import { RedisResult } from '../src/core/redis-result'
import type { RedisValue } from '../src/core/redis-value'
import { createRedisCommandExecutor } from '../src/internal'
import { createRedisSessionHarness } from './core-session-test-helpers'

type KeyLayout = [number, number, number, number]

function commandInfoLayouts(value: RedisValue): Map<string, KeyLayout> {
  assert.strictEqual(value.kind, 'array')
  const layouts = new Map<string, KeyLayout>()
  const collect = (entry: RedisValue) => {
    assert.strictEqual(entry.kind, 'array')
    const [name, arity, , first, last, step, , , , subcommands] = entry.items
    const number = (item: RedisValue) => {
      assert.strictEqual(item.kind, 'integer')
      return Number(item.value)
    }
    assert.strictEqual(name.kind, 'bulk-string')
    layouts.set(String(name.value), [
      number(arity),
      number(first),
      number(last),
      number(step),
    ])
    assert.strictEqual(subcommands.kind, 'array')
    subcommands.items.forEach(collect)
  }
  value.items.forEach(collect)
  return layouts
}

async function commandInfo(
  session: ReturnType<typeof createRedisSessionHarness>['session'],
  args: string[],
): Promise<Map<string, KeyLayout>> {
  const result = await session.execute(
    'command',
    args.map(arg => Buffer.from(arg)),
  )
  return commandInfoLayouts(result.value)
}

describe('COMMAND INFO derivation (#370)', () => {
  test('a command whose schema has no layout reports arity -1', async () => {
    // A hand-built schema, as a /core consumer may still write one.
    const legacy = defineCommand({
      name: 'legacycmd',
      schema: {
        parse: (input: readonly Buffer[], index: number) => ({
          value: input.slice(index),
          nextIndex: input.length,
        }),
      },
      flags: ['readonly'],
      keys: () => [],
      execute: () => RedisResult.ok(),
    }) as CommandDefinition<unknown>
    const { session } = createRedisSessionHarness({ extraCommands: [legacy] })

    const info = await commandInfo(session, ['INFO', 'legacycmd'])
    assert.deepStrictEqual(info.get('legacycmd'), [-1, 0, 0, 0])

    // The whole-table form still renders every command.
    const all = await commandInfo(session, [])
    assert.deepStrictEqual(all.get('legacycmd'), [-1, 0, 0, 0])
    assert.deepStrictEqual(all.get('get'), [2, 1, 1, 1])

    const reply = await session.execute('legacycmd', [Buffer.from('x')])
    assert.deepStrictEqual(reply, RedisResult.ok())
  })

  test('every command and subcommand matches the Redis 8.0 snapshot', async () => {
    // Captured from real Redis 8.0 by scripts/capture-command-info-fixture.ts.
    const snapshot = JSON.parse(
      readFileSync(
        resolve(__dirname, 'fixtures/command-info-redis-8.0.json'),
        'utf8',
      ),
    ) as Record<string, KeyLayout>
    const { session } = createRedisSessionHarness()

    const all = await commandInfo(session, [])
    assert.deepStrictEqual(
      Object.fromEntries([...all].sort(([a], [b]) => a.localeCompare(b))),
      snapshot,
    )
  })
})

// One valid invocation (arguments after the command name) per command with a
// key range. Keys are spelled k1, k2, ... so they cannot collide with values.
const SAMPLES: Record<string, string[]> = {
  watch: ['k1', 'k2'],
  get: ['k1'],
  set: ['k1', 'v'],
  mget: ['k1', 'k2'],
  append: ['k1', 'v'],
  strlen: ['k1'],
  incr: ['k1'],
  decr: ['k1'],
  incrby: ['k1', '5'],
  decrby: ['k1', '5'],
  incrbyfloat: ['k1', '1.5'],
  getset: ['k1', 'v'],
  getdel: ['k1'],
  setnx: ['k1', 'v'],
  setex: ['k1', '10', 'v'],
  psetex: ['k1', '10', 'v'],
  mset: ['k1', 'v1', 'k2', 'v2'],
  msetnx: ['k1', 'v1', 'k2', 'v2'],
  getrange: ['k1', '0', '1'],
  substr: ['k1', '0', '1'],
  setrange: ['k1', '0', 'v'],
  getex: ['k1', 'EX', '10'],
  setbit: ['k1', '0', '1'],
  getbit: ['k1', '0'],
  bitcount: ['k1'],
  bitpos: ['k1', '1'],
  bitop: ['AND', 'k1', 'k2', 'k3'],
  bitfield: ['k1', 'GET', 'u8', '0'],
  bitfield_ro: ['k1', 'GET', 'u8', '0'],
  pfadd: ['k1', 'a'],
  pfcount: ['k1', 'k2'],
  pfmerge: ['k1', 'k2', 'k3'],
  del: ['k1', 'k2'],
  unlink: ['k1', 'k2'],
  exists: ['k1', 'k2'],
  touch: ['k1', 'k2'],
  type: ['k1'],
  ttl: ['k1'],
  pttl: ['k1'],
  expiretime: ['k1'],
  pexpiretime: ['k1'],
  expire: ['k1', '10'],
  pexpire: ['k1', '10'],
  persist: ['k1'],
  expireat: ['k1', '10'],
  pexpireat: ['k1', '10'],
  rename: ['k1', 'k2'],
  renamenx: ['k1', 'k2'],
  move: ['k1', '1'],
  copy: ['k1', 'k2'],
  sort: ['k1'],
  sort_ro: ['k1'],
  hscan: ['k1', '0'],
  sscan: ['k1', '0'],
  zscan: ['k1', '0'],
  hset: ['k1', 'f', 'v'],
  hsetnx: ['k1', 'f', 'v'],
  hget: ['k1', 'f'],
  hdel: ['k1', 'f'],
  hgetdel: ['k1', 'FIELDS', '1', 'f'],
  hgetex: ['k1', 'FIELDS', '1', 'f'],
  hsetex: ['k1', 'FIELDS', '1', 'f', 'v'],
  hpersist: ['k1', 'FIELDS', '1', 'f'],
  hexpire: ['k1', '100', 'FIELDS', '1', 'f'],
  hpexpire: ['k1', '100', 'FIELDS', '1', 'f'],
  hexpireat: ['k1', '100', 'FIELDS', '1', 'f'],
  hpexpireat: ['k1', '100', 'FIELDS', '1', 'f'],
  httl: ['k1', 'FIELDS', '1', 'f'],
  hpttl: ['k1', 'FIELDS', '1', 'f'],
  hmset: ['k1', 'f', 'v'],
  hmget: ['k1', 'f'],
  hgetall: ['k1'],
  hkeys: ['k1'],
  hvals: ['k1'],
  hrandfield: ['k1'],
  hlen: ['k1'],
  hexists: ['k1', 'f'],
  hincrby: ['k1', 'f', '1'],
  hincrbyfloat: ['k1', 'f', '1.5'],
  hstrlen: ['k1', 'f'],
  lpush: ['k1', 'a'],
  rpush: ['k1', 'a'],
  lpop: ['k1'],
  rpop: ['k1'],
  llen: ['k1'],
  lrange: ['k1', '0', '-1'],
  lindex: ['k1', '0'],
  linsert: ['k1', 'BEFORE', 'p', 'e'],
  lset: ['k1', '0', 'v'],
  lrem: ['k1', '0', 'v'],
  ltrim: ['k1', '0', '1'],
  lpushx: ['k1', 'a'],
  rpushx: ['k1', 'a'],
  rpoplpush: ['k1', 'k2'],
  lpos: ['k1', 'e'],
  lmove: ['k1', 'k2', 'LEFT', 'RIGHT'],
  blmove: ['k1', 'k2', 'LEFT', 'RIGHT', '0'],
  blpop: ['k1', 'k2', '0'],
  brpop: ['k1', 'k2', '0'],
  sadd: ['k1', 'm'],
  srem: ['k1', 'm'],
  scard: ['k1'],
  smembers: ['k1'],
  sismember: ['k1', 'm'],
  smismember: ['k1', 'm'],
  spop: ['k1'],
  srandmember: ['k1'],
  sdiff: ['k1', 'k2'],
  sinter: ['k1', 'k2'],
  sunion: ['k1', 'k2'],
  smove: ['k1', 'k2', 'm'],
  sdiffstore: ['k1', 'k2', 'k3'],
  sinterstore: ['k1', 'k2', 'k3'],
  sunionstore: ['k1', 'k2', 'k3'],
  zadd: ['k1', '1', 'm'],
  zrem: ['k1', 'm'],
  zcard: ['k1'],
  zrank: ['k1', 'm'],
  zrevrank: ['k1', 'm'],
  zscore: ['k1', 'm'],
  zmscore: ['k1', 'm'],
  zrandmember: ['k1'],
  zincrby: ['k1', '1', 'm'],
  zrange: ['k1', '0', '-1'],
  zrangestore: ['k1', 'k2', '0', '-1'],
  zrevrange: ['k1', '0', '-1'],
  zrangebyscore: ['k1', '-inf', '+inf'],
  zrevrangebyscore: ['k1', '+inf', '-inf'],
  zremrangebyscore: ['k1', '0', '1'],
  zremrangebyrank: ['k1', '0', '1'],
  zcount: ['k1', '0', '1'],
  zrangebylex: ['k1', '-', '+'],
  zrevrangebylex: ['k1', '+', '-'],
  zlexcount: ['k1', '-', '+'],
  zremrangebylex: ['k1', '-', '+'],
  zpopmin: ['k1'],
  zpopmax: ['k1'],
  bzpopmin: ['k1', 'k2', '0'],
  bzpopmax: ['k1', 'k2', '0'],
  zunionstore: ['k1', '2', 'k2', 'k3'],
  zinterstore: ['k1', '2', 'k2', 'k3'],
  zdiffstore: ['k1', '2', 'k2', 'k3'],
  geoadd: ['k1', '13.36', '38.11', 'm'],
  geopos: ['k1', 'm'],
  geodist: ['k1', 'a', 'b'],
  geohash: ['k1', 'm'],
  geosearch: ['k1', 'FROMLONLAT', '0', '0', 'BYRADIUS', '1', 'km'],
  geosearchstore: ['k1', 'k2', 'FROMLONLAT', '0', '0', 'BYRADIUS', '1', 'km'],
  georadius: ['k1', '0', '0', '1', 'km'],
  georadius_ro: ['k1', '0', '0', '1', 'km'],
  georadiusbymember: ['k1', 'm', '1', 'km'],
  georadiusbymember_ro: ['k1', 'm', '1', 'km'],
  ssubscribe: ['k1', 'k2'],
  sunsubscribe: ['k1', 'k2'],
  spublish: ['k1', 'message'],
  xadd: ['k1', '*', 'f', 'v'],
  xlen: ['k1'],
  xrange: ['k1', '-', '+'],
  xrevrange: ['k1', '+', '-'],
  xdel: ['k1', '1-1'],
  xtrim: ['k1', 'MAXLEN', '10'],
  xack: ['k1', 'g', '1-1'],
  xpending: ['k1', 'g'],
  xclaim: ['k1', 'g', 'c', '0', '1-1'],
  xautoclaim: ['k1', 'g', 'c', '0', '0'],
  xsetid: ['k1', '1-1'],
}

describe('schema key range agrees with keys(args) (#370)', () => {
  const executor = createRedisCommandExecutor()

  for (const definition of executor.getCommandDefinitions()) {
    const range = schemaKeyRange(definition.schema)
    if (range.firstKey === 0) {
      continue
    }

    test(definition.name, () => {
      const sample = SAMPLES[definition.name]
      assert.ok(sample, `add a sample invocation for ${definition.name}`)

      const argv = [definition.name, ...sample]
      const keys = executor
        .plan(
          definition.name,
          sample.map(arg => Buffer.from(arg)),
        )
        .keys.map(key => key.toString())
      const last =
        range.lastKey < 0 ? argv.length + range.lastKey : range.lastKey
      assert.ok(last >= range.firstKey, 'sample too short for the key range')

      for (let i = range.firstKey; i <= last; i += range.keyStep) {
        assert.ok(
          keys.includes(argv[i]),
          `position ${i} (${argv[i]}) is in the key range but not in keys(args): ${JSON.stringify(keys)}`,
        )
      }
    })
  }
})
