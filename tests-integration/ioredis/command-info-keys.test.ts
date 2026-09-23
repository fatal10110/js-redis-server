import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import {
  type CompatibilitySpec,
  Resp2Server,
  RedisServerState,
  createRedisCommandExecutor,
} from '../../src/internal'

const testRunner = new TestRunner()

// `[arity, first key, last key, key step]` as real Redis 8.0 reports them.
type KeyLayout = [number, number, number, number]

const EXPECTED: Record<string, KeyLayout> = {
  lpush: [-3, 1, 1, 1],
  zadd: [-4, 1, 1, 1],
  hset: [-4, 1, 1, 1],
  get: [2, 1, 1, 1],
  set: [-3, 1, 1, 1],
  mget: [-2, 1, -1, 1],
  del: [-2, 1, -1, 1],
  eval: [-3, 0, 0, 0],
  xadd: [-5, 1, 1, 1],
  // Pairs, two fixed keys, keys before a trailing argument, a key after a
  // leading argument, a key before a numkeys block, and a keyless command.
  mset: [-3, 1, -1, 2],
  rename: [3, 1, 2, 1],
  blpop: [-3, 1, -2, 1],
  bitop: [-4, 2, -1, 1],
  zunionstore: [-4, 1, 1, 1],
  xread: [-4, 0, 0, 0],
  ping: [-1, 0, 0, 0],
}

type CommandInfoReply = [
  string,
  number,
  string[],
  number,
  number,
  number,
  ...unknown[],
]

function layoutOf(info: CommandInfoReply): KeyLayout {
  return [info[1], info[3], info[4], info[5]]
}

async function startMock(compatibility?: CompatibilitySpec): Promise<{
  client: Redis
  close: () => Promise<void>
}> {
  const state = new RedisServerState({ compatibility })
  const executor = createRedisCommandExecutor({ compatibility: state.profile })
  const server = new Resp2Server({ server: state, executor })
  await server.listen(0)
  const client = new Redis({ port: server.getPort(), lazyConnect: true })
  await client.connect()
  return {
    client,
    close: async () => {
      client.disconnect()
      await server.close()
    },
  }
}

describe(`COMMAND INFO arity and key positions (${testRunner.getBackendName()})`, () => {
  let redis: Redis

  before(async () => {
    redis = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('reports arity and first/last/step keys like Redis', async () => {
    const names = Object.keys(EXPECTED)
    const infos = (await redis.command('INFO', ...names)) as CommandInfoReply[]

    assert.strictEqual(infos.length, names.length)
    for (const [i, name] of names.entries()) {
      assert.strictEqual(infos[i][0], name)
      assert.deepStrictEqual(layoutOf(infos[i]), EXPECTED[name], name)
    }
  })

  test('COMMAND (no subcommand) agrees with COMMAND INFO', async () => {
    const all = (await redis.command()) as CommandInfoReply[]
    const byName = new Map(all.map(info => [info[0], info]))

    for (const [name, expected] of Object.entries(EXPECTED)) {
      const info = byName.get(name)
      assert.ok(info, `${name} missing from COMMAND`)
      assert.deepStrictEqual(layoutOf(info), expected, name)
    }
  })

  test('GEOPOS and GEOHASH accept a key alone (arity -2)', async () => {
    const key = `geo:${randomKey()}`
    assert.deepStrictEqual(await redis.geopos(key), [])
    assert.deepStrictEqual(await redis.geohash(key), [])

    await redis.geoadd(key, 13.361389, 38.115556, 'Palermo')
    assert.deepStrictEqual(await redis.geopos(key), [])
    assert.deepStrictEqual(await redis.geohash(key), [])
    assert.deepStrictEqual(await redis.geohash(key, 'Palermo'), ['sqc8b49rny0'])
  })

  test(
    'every mock command and subcommand matches real Redis',
    { skip: testRunner.backend !== 'real' },
    async () => {
      // Sweep: the in-process mock at the default profile (redis-8.0) against
      // the real server, over every command both of them know — top-level
      // entries (`COMMAND INFO client|list` included) and the subcommand
      // entries nested inside each container's reply.
      const mock = await startMock()
      try {
        const names = (await mock.client.command('LIST')) as string[]
        const mockInfos = (await mock.client.command(
          'INFO',
          ...names,
        )) as CommandInfoReply[]
        const realInfos = (await redis.command(
          'INFO',
          ...names,
        )) as (CommandInfoReply | null)[]

        const mismatches: string[] = []
        const compare = (
          name: string,
          mockInfo: CommandInfoReply,
          realInfo: CommandInfoReply,
        ) => {
          const mockLayout = layoutOf(mockInfo)
          const realLayout = layoutOf(realInfo)
          if (mockLayout.join() !== realLayout.join()) {
            mismatches.push(`${name}: mock ${mockLayout} real ${realLayout}`)
          }
        }

        let nestedCompared = 0
        for (const [i, name] of names.entries()) {
          const real = realInfos[i]
          if (!real) {
            continue
          }

          compare(name, mockInfos[i], real)
          const realSubcommands = new Map(
            (real[9] as CommandInfoReply[]).map(sub => [sub[0], sub]),
          )
          for (const sub of mockInfos[i][9] as CommandInfoReply[]) {
            const realSub = realSubcommands.get(sub[0])
            if (realSub) {
              nestedCompared++
              compare(`${sub[0]} (nested)`, sub, realSub)
            }
          }
        }

        assert.deepStrictEqual(mismatches, [])
        assert.ok(nestedCompared > 0, 'no nested subcommand was compared')
      } finally {
        await mock.close()
      }
    },
  )

  // Arities that differ by version, as real 6.2.14 / 7.0.15 / 7.2.4 report
  // them. Each parser enforces the arity its profile reports.
  const GATED_ARITY: Record<
    string,
    Partial<Record<CompatibilitySpec & string, number>>
  > = {
    expire: { 'redis-6.2': 3, 'redis-7.0': -3, 'redis-7.2': -3 },
    pexpire: { 'redis-6.2': 3, 'redis-7.0': -3, 'redis-7.2': -3 },
    expireat: { 'redis-6.2': 3, 'redis-7.0': -3, 'redis-7.2': -3 },
    pexpireat: { 'redis-6.2': 3, 'redis-7.0': -3, 'redis-7.2': -3 },
    zrank: { 'redis-6.2': 3, 'redis-7.0': 3, 'redis-7.2': -3 },
    zrevrank: { 'redis-6.2': 3, 'redis-7.0': 3, 'redis-7.2': -3 },
    xsetid: { 'redis-6.2': 3, 'redis-7.0': -3, 'redis-7.2': -3 },
    'command|getkeys': { 'redis-7.0': -4, 'redis-7.2': -3 },
    'command|getkeysandflags': { 'redis-7.0': -4, 'redis-7.2': -3 },
  }

  for (const profile of ['redis-6.2', 'redis-7.0', 'redis-7.2'] as const) {
    test(
      `version-gated arities and their parsers on ${profile}`,
      { skip: testRunner.backend === 'real' },
      async () => {
        const mock = await startMock(profile)
        try {
          const names = Object.keys(GATED_ARITY).filter(
            name => GATED_ARITY[name][profile] !== undefined,
          )
          const infos = (await mock.client.command(
            'INFO',
            ...names,
          )) as CommandInfoReply[]
          for (const [i, name] of names.entries()) {
            assert.strictEqual(
              infos[i][1],
              GATED_ARITY[name][profile],
              `${name} arity on ${profile}`,
            )
          }

          const reply = (promise: Promise<unknown>) =>
            promise.then(
              value => JSON.stringify(value),
              (err: Error) => err.message,
            )
          const arityError = (command: string) =>
            `ERR wrong number of arguments for '${command}' command`
          const redis62 = profile === 'redis-6.2'
          const redis72 = profile === 'redis-7.2'

          await mock.client.set('k', 'v')
          assert.strictEqual(
            await reply(mock.client.call('EXPIRE', 'k', '10', 'foo')),
            redis62 ? arityError('expire') : 'ERR Unsupported option foo',
          )
          assert.strictEqual(
            await reply(mock.client.expire('k', 10, 'NX')),
            redis62 ? arityError('expire') : '1',
          )

          await mock.client.zadd('z', 1, 'm')
          for (const command of ['ZRANK', 'ZREVRANK']) {
            assert.strictEqual(
              await reply(mock.client.call(command, 'z', 'm', 'WITHSCORE')),
              redis72 ? '[0,"1"]' : arityError(command.toLowerCase()),
            )
          }

          await mock.client.xadd('s', '1-1', 'f', 'v')
          assert.strictEqual(
            await reply(
              mock.client.call('XSETID', 's', '2-0', 'ENTRIESADDED', '5'),
            ),
            redis62 ? arityError('xsetid') : '"OK"',
          )

          if (!redis62) {
            assert.strictEqual(
              await reply(mock.client.command('GETKEYS', 'GET')),
              redis72
                ? 'ERR Invalid number of arguments specified for command'
                : arityError('command|getkeys'),
            )
            assert.strictEqual(
              await reply(mock.client.command('GETKEYS', 'GET', 'a', 'b')),
              'ERR Invalid number of arguments specified for command',
            )
            assert.strictEqual(
              await reply(mock.client.command('GETKEYS', 'GET', 'k')),
              '["k"]',
            )
          }
        } finally {
          await mock.close()
        }
      },
    )
  }
})
