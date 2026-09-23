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

type CommandInfoReply = [string, number, string[], number, number, number]

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
    'every mock command matches real Redis',
    { skip: testRunner.backend !== 'real' },
    async () => {
      // Sweep: the in-process mock at the default profile (redis-8.0) against
      // the real server, over every command both of them know.
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
        for (const [i, name] of names.entries()) {
          const real = realInfos[i]
          if (!real) {
            continue
          }

          const mockLayout = layoutOf(mockInfos[i])
          const realLayout = layoutOf(real)
          if (mockLayout.join() !== realLayout.join()) {
            mismatches.push(`${name}: mock ${mockLayout} real ${realLayout}`)
          }
        }

        assert.deepStrictEqual(mismatches, [])
      } finally {
        await mock.close()
      }
    },
  )

  test(
    'EXPIRE family reports the fixed pre-7.0 arity on older profiles',
    { skip: testRunner.backend === 'real' },
    async () => {
      const names = ['expire', 'pexpire', 'expireat', 'pexpireat']
      for (const [profile, arity] of [
        ['redis-6.2', 3],
        ['redis-7.0', -3],
      ] as const) {
        const mock = await startMock(profile)
        try {
          const infos = (await mock.client.command(
            'INFO',
            ...names,
          )) as CommandInfoReply[]
          for (const [i, name] of names.entries()) {
            assert.deepStrictEqual(
              layoutOf(infos[i]),
              [arity, 1, 1, 1],
              `${name} on ${profile}`,
            )
          }

          // The reported arity is the one the parser enforces.
          const reply =
            arity === 3
              ? /wrong number of arguments for 'expire' command/
              : /^1$/
          await mock.client.set('k', 'v')
          const result = await mock.client
            .expire('k', 10, 'NX')
            .then(String, (err: Error) => err.message)
          assert.match(result, reply, profile)
        } finally {
          await mock.close()
        }
      }
    },
  )
})
