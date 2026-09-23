import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import clusterKeySlot from 'cluster-key-slot'
import {
  createClient,
  createCluster,
  type RedisClientType,
  type RedisClusterType,
} from 'redis'
import {
  createInMemoryRedis,
  createNodeRedisMock,
  type InMemoryRedis,
  type InMemoryRedisClient,
  type NodeRedisMockClient,
  type NodeRedisMockCluster,
} from '../../src/index'
import { FACADE_DEFAULT_PROTOCOL } from '../socketless/known-gaps'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

/**
 * Reply-shape parity for the socketless node-redis-shaped clients (#412).
 *
 * The facade (`createNodeRedisMock`, standalone and `NodeRedisMockCluster`)
 * and the in-memory client (`createInMemoryRedis`) hand-decode replies instead
 * of going through a wire parser, so they re-implement per client what the
 * real node-redis does — the place #385 (RESP3 flat pairs) hid. Here each
 * command runs through the socketless client *and* through the real node-redis
 * client against the TCP backend, at RESP2 and RESP3, and the decoded replies
 * must be deep-equal. On the real backend that makes real Redis + real
 * node-redis the oracle.
 *
 * Needs the TCP backend for its reference client, so it is skipped on
 * `TEST_BACKEND=socketless` (which runs the ioredis/node-redis suites
 * themselves against the socketless clients instead).
 */
const testRunner = new TestRunner()
const skip =
  testRunner.backend === 'socketless'
    ? 'needs a TCP backend for the reference node-redis client'
    : false

type Resp = 2 | 3
const PROTOCOLS: readonly Resp[] = [2, 3]

/** Setup commands, then the probe whose decoded reply is compared. */
type Case = { name: string; setup: string[][]; probe: string[] }

/**
 * The reply-shape surface: every kind the protocol version decides. `k` must
 * return a fresh key namespace per call — each case owns its keys, so running
 * a case twice against one reference server never collides.
 */
function shapeCases(k: () => (suffix: string) => string): Case[] {
  const zadd = (z: string) => ['ZADD', z, '1', 'a', '2.5', 'b']
  const xadd = (s: string) => ['XADD', s, '1-1', 'f', 'v']
  const build = (
    name: string,
    make: (key: (suffix: string) => string) => Omit<Case, 'name'>,
  ): Case => ({ name, ...make(k()) })
  return [
    build('ZRANGE WITHSCORES', key => ({
      setup: [zadd(key('z'))],
      probe: ['ZRANGE', key('z'), '0', '-1', 'WITHSCORES'],
    })),
    build('ZSCORE', key => ({
      setup: [zadd(key('z'))],
      probe: ['ZSCORE', key('z'), 'b'],
    })),
    build('ZINCRBY', key => ({
      setup: [zadd(key('z'))],
      probe: ['ZINCRBY', key('z'), '0.25', 'a'],
    })),
    build('ZPOPMIN with count', key => ({
      setup: [zadd(key('z'))],
      probe: ['ZPOPMIN', key('z'), '2'],
    })),
    build('ZRANDMEMBER WITHSCORES', key => ({
      setup: [['ZADD', key('z'), '3', 'only']],
      probe: ['ZRANDMEMBER', key('z'), '1', 'WITHSCORES'],
    })),
    build('HGETALL', key => ({
      setup: [['HSET', key('h'), 'f1', 'v1', 'f2', 'v2']],
      probe: ['HGETALL', key('h')],
    })),
    build('HRANDFIELD WITHVALUES', key => ({
      setup: [['HSET', key('h'), 'f1', 'v1']],
      probe: ['HRANDFIELD', key('h'), '1', 'WITHVALUES'],
    })),
    build('CONFIG GET', () => ({
      setup: [],
      probe: ['CONFIG', 'GET', 'databases'],
    })),
    build('XRANGE', key => ({
      setup: [xadd(key('s'))],
      probe: ['XRANGE', key('s'), '-', '+'],
    })),
    build('XREAD', key => ({
      setup: [xadd(key('s'))],
      probe: ['XREAD', 'COUNT', '1', 'STREAMS', key('s'), '0'],
    })),
    build('SMEMBERS', key => ({
      setup: [['SADD', key('set'), 'm']],
      probe: ['SMEMBERS', key('set')],
    })),
    build('GET of a missing key', key => ({
      setup: [],
      probe: ['GET', key('missing')],
    })),
    build('ZPOPMIN without count', key => ({
      setup: [zadd(key('z'))],
      probe: ['ZPOPMIN', key('z')],
    })),
    build('ZMSCORE with a missing member', key => ({
      setup: [zadd(key('z'))],
      probe: ['ZMSCORE', key('z'), 'b', 'nope', 'a'],
    })),
    build('inf scores', key => ({
      setup: [['ZADD', key('z'), '+inf', 'hi', '-inf', 'lo', '0', 'mid']],
      probe: ['ZRANGE', key('z'), '0', '-1', 'WITHSCORES'],
    })),
    build('ZSCORE of an inf score', key => ({
      setup: [['ZADD', key('z'), '-inf', 'lo']],
      probe: ['ZSCORE', key('z'), 'lo'],
    })),
    build('ZRANK WITHSCORE', key => ({
      setup: [zadd(key('z'))],
      probe: ['ZRANK', key('z'), 'b', 'WITHSCORE'],
    })),
    build('WRONGTYPE error', key => ({
      setup: [['SET', key('str'), 'v']],
      probe: ['HGETALL', key('str')],
    })),
  ]
}

/** Commands of the cases above that write, so a cluster client routes them to a master. */
const WRITES = new Set([
  'ZADD',
  'HSET',
  'XADD',
  'SADD',
  'SET',
  'ZINCRBY',
  'ZPOPMIN',
])

type Outcome = { reply: unknown } | { error: string; errorClass: string }

/** Decoded reply, or the thrown error's message and class. */
async function outcome(run: () => Promise<unknown>): Promise<Outcome> {
  try {
    return { reply: await run() }
  } catch (err) {
    const error = err as Error
    return { error: error.message, errorClass: error.constructor.name }
  }
}

/**
 * `createInMemoryRedis()` is not node-redis-shaped for errors: it throws its
 * own documented `RedisCommandError` (node-redis throws `SimpleError`). So an
 * error must carry node-redis' message in that class; a reply must be equal.
 */
function assertInMemoryParity(mem: Outcome, ref: Outcome): void {
  if ('error' in ref) {
    assert.deepStrictEqual(mem, {
      error: ref.error,
      errorClass: 'RedisCommandError',
    })
    return
  }
  assert.deepStrictEqual(mem, ref)
}

function withTimeout<T>(promise: Promise<T>, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out: ${what}`)), 2000)
    }),
  ]).finally(() => clearTimeout(timer))
}

describe(
  `socketless client reply-shape parity (${testRunner.getBackendName()})`,
  { skip },
  () => {
    const references: { destroy(): void }[] = []
    const facades: { destroy(): void }[] = []
    const inMemory: InMemoryRedis[] = []
    let port: number

    before(async () => {
      if (skip) {
        return
      }
      port = await testRunner.setupRawStandalone()
    })

    after(async () => {
      for (const client of [...references, ...facades]) {
        try {
          client.destroy()
        } catch {
          // already closed
        }
      }
      for (const instance of inMemory) {
        instance.close()
      }
      await testRunner.cleanup()
    })

    async function reference(RESP: Resp): Promise<RedisClientType> {
      const client = createClient({
        url: `redis://127.0.0.1:${port}`,
        RESP,
      }) as RedisClientType
      client.on('error', () => {})
      await client.connect()
      references.push(client)
      return client
    }

    async function facade(RESP: Resp): Promise<NodeRedisMockClient> {
      const client = (await createNodeRedisMock()) as NodeRedisMockClient
      facades.push(client)
      if (RESP === 3) {
        await client.sendCommand(['HELLO', '3'])
      }
      return client
    }

    async function inMemoryClient(
      RESP: Resp,
      instance?: InMemoryRedis,
    ): Promise<InMemoryRedisClient> {
      if (!instance) {
        instance = await createInMemoryRedis()
        inMemory.push(instance)
      }
      const client = instance.connect()
      if (RESP === 3) {
        await client.command('HELLO', 3)
      }
      return client
    }

    /** The MULTI/EXEC body: nested replies of every protocol-decided kind. */
    function transaction(RESP: Resp): string[][] {
      const tag = `{parity-multi:${RESP}:${randomKey()}}`
      return [
        ['ZADD', `${tag}:z`, '1', 'a', '2.5', 'b'],
        ['ZRANGE', `${tag}:z`, '0', '-1', 'WITHSCORES'],
        ['HSET', `${tag}:h`, 'f', 'v'],
        ['HGETALL', `${tag}:h`],
        ['XADD', `${tag}:s`, '1-1', 'f', 'v'],
        ['XRANGE', `${tag}:s`, '-', '+'],
        ['ZSCORE', `${tag}:z`, 'b'],
      ]
    }

    test(
      'default protocol: createNodeRedisMock() answers like a default node-redis client',
      { todo: FACADE_DEFAULT_PROTOCOL },
      async () => {
        // No RESP option and no HELLO on either side: what a user gets out of
        // the box. node-redis 6 negotiates RESP3; the facade stays on RESP2.
        const ref = createClient({
          url: `redis://127.0.0.1:${port}`,
        }) as RedisClientType
        ref.on('error', () => {})
        await ref.connect()
        references.push(ref)
        const mock = (await createNodeRedisMock()) as NodeRedisMockClient
        facades.push(mock)

        const tag = `{parity-default:${randomKey()}}`
        for (const args of [
          ['ZADD', `${tag}:z`, '1', 'a', '2.5', 'b'],
          ['HSET', `${tag}:h`, 'f', 'v'],
        ]) {
          await ref.sendCommand(args)
          await mock.sendCommand(args)
        }
        for (const probe of [
          ['ZSCORE', `${tag}:z`, 'b'],
          ['HGETALL', `${tag}:h`],
          ['ZRANGE', `${tag}:z`, '0', '-1', 'WITHSCORES'],
        ]) {
          assert.deepStrictEqual(
            await outcome(() => mock.sendCommand(probe)),
            await outcome(() => ref.sendCommand(probe)),
            probe[0],
          )
        }
      },
    )

    for (const RESP of PROTOCOLS) {
      describe(`RESP${RESP}`, () => {
        const namespace = () => {
          const tag = `{parity:${RESP}:${randomKey()}}`
          return (suffix: string) => `${tag}:${suffix}`
        }
        const cases = shapeCases(namespace).map(c => c.name)
        for (const [index, name] of cases.entries()) {
          const fresh = () => shapeCases(namespace)[index]

          test(`${name}: createNodeRedisMock().sendCommand matches node-redis`, async () => {
            const { setup, probe } = fresh()
            const [ref, mock] = [await reference(RESP), await facade(RESP)]
            for (const args of setup) {
              await ref.sendCommand(args)
              await mock.sendCommand(args)
            }
            assert.deepStrictEqual(
              await outcome(() => mock.sendCommand(probe)),
              await outcome(() => ref.sendCommand(probe)),
            )
          })

          test(`${name}: createInMemoryRedis() matches node-redis`, async () => {
            const { setup, probe } = fresh()
            const [ref, mem] = [
              await reference(RESP),
              await inMemoryClient(RESP),
            ]
            for (const args of setup) {
              await ref.sendCommand(args)
              await mem.command(args[0], ...args.slice(1))
            }
            assertInMemoryParity(
              await outcome(() => mem.command(probe[0], ...probe.slice(1))),
              await outcome(() => ref.sendCommand(probe)),
            )
          })
        }

        test('MULTI/EXEC nests the per-command reply shapes: createNodeRedisMock()', async () => {
          const [ref, mock] = [await reference(RESP), await facade(RESP)]
          const queued = transaction(RESP)
          const refMulti = ref.multi()
          const mockMulti = mock.multi()
          for (const args of queued) {
            refMulti.addCommand(args)
            mockMulti.addCommand(args)
          }
          assert.deepStrictEqual(await mockMulti.exec(), await refMulti.exec())
        })

        test('MULTI/EXEC nests the per-command reply shapes: createInMemoryRedis()', async () => {
          const [ref, mem] = [await reference(RESP), await inMemoryClient(RESP)]
          const queued = transaction(RESP)
          const refMulti = ref.multi()
          await mem.command('MULTI')
          for (const args of queued) {
            refMulti.addCommand(args)
            assert.strictEqual(
              await mem.command(args[0], ...args.slice(1)),
              'QUEUED',
            )
          }
          assert.deepStrictEqual(
            await mem.command('EXEC'),
            await refMulti.exec(),
          )
        })

        test('pub/sub delivers the same (message, channel) pushes', async () => {
          const [ref, mock] = [await reference(RESP), await facade(RESP)]
          const channel = `parity-pubsub:${RESP}:${randomKey()}`
          const received = { ref: [] as string[][], mock: [] as string[][] }

          const refSub = ref.duplicate()
          refSub.on('error', () => {})
          await refSub.connect()
          references.push(refSub)
          await refSub.subscribe(channel, (message, ch) => {
            received.ref.push([message, ch])
          })
          const mockSub = await mock.duplicate()
          facades.push(mockSub)
          // duplicate() does not carry the protocol over (a real node-redis
          // duplicate re-handshakes with its RESP option), so say it again.
          if (RESP === 3) {
            await mockSub.sendCommand(['HELLO', '3'])
          }
          await mockSub.subscribe(channel, (message, ch) => {
            received.mock.push([message, ch])
          })

          assert.strictEqual(await mock.publish(channel, 'hello'), 1)
          assert.strictEqual(await ref.publish(channel, 'hello'), 1)
          await waitFor(
            () => received.ref.length === 1 && received.mock.length === 1,
          )
          assert.deepStrictEqual(received.mock, received.ref)
        })

        test('pub/sub: createInMemoryRedis() pushes the tagged message frame', async () => {
          const ref = await reference(RESP)
          const instance = await createInMemoryRedis()
          inMemory.push(instance)
          const [publisher, subscriber] = [
            await inMemoryClient(RESP, instance),
            await inMemoryClient(RESP, instance),
          ]
          const channel = `parity-pubsub-mem:${RESP}:${randomKey()}`

          const refReceived: string[][] = []
          const refSub = ref.duplicate()
          refSub.on('error', () => {})
          await refSub.connect()
          references.push(refSub)
          await refSub.subscribe(channel, (message, ch) => {
            refReceived.push([message, ch])
          })
          await subscriber.command('SUBSCRIBE', channel)

          assert.strictEqual(
            await publisher.command('PUBLISH', channel, 'hi'),
            1,
          )
          assert.strictEqual(await ref.publish(channel, 'hi'), 1)
          const pushes = subscriber.pushes()[Symbol.asyncIterator]()
          const { value: push } = await withTimeout(
            pushes.next(),
            'in-memory pub/sub push',
          )
          await waitFor(() => refReceived.length === 1)
          const [[message, ch]] = refReceived
          // The in-memory client keeps the push's type tag (see its
          // pushShape); node-redis hands its listener only the payload.
          assert.deepStrictEqual(push, ['message', ch, message])
          await pushes.return?.()
        })
      })
    }
  },
)

describe(
  `NodeRedisMockCluster routing and reply-shape parity (${testRunner.getBackendName()})`,
  { skip },
  () => {
    const clients: { destroy(): void }[] = []
    let ports: number[]
    /**
     * One hash tag per slot range of both clusters' actual slot maps (the
     * reference's and the facade's), so every master of each serves a case.
     */
    let tags: string[]

    before(async () => {
      if (skip) {
        return
      }
      await testRunner.setupNodeRedisCluster()
      ports = testRunner.getClusterPorts()
      const ref = await reference(2)
      const mock = await facade(2)
      tags = tagsCoveringRanges([
        ...slotRanges(
          await ref.sendCommand(undefined, true, ['CLUSTER', 'SLOTS']),
        ),
        ...slotRanges(await mock.sendCommand(['CLUSTER', 'SLOTS'])),
      ])
    })

    after(async () => {
      for (const client of clients) {
        try {
          client.destroy()
        } catch {
          // already closed
        }
      }
      await testRunner.cleanup()
    })

    async function reference(RESP: Resp): Promise<RedisClusterType> {
      const cluster = createCluster({
        rootNodes: ports.map(p => ({ url: `redis://127.0.0.1:${p}` })),
        RESP,
      }) as RedisClusterType
      cluster.on('error', () => {})
      await cluster.connect()
      clients.push(cluster)
      return cluster
    }

    async function facade(RESP: Resp): Promise<NodeRedisMockCluster> {
      const cluster = (await createNodeRedisMock({
        cluster: { masters: 3 },
      })) as NodeRedisMockCluster
      clients.push(cluster)
      if (RESP === 3) {
        await cluster.sendCommand(['HELLO', '3'])
      }
      return cluster
    }

    for (const RESP of PROTOCOLS) {
      describe(`RESP${RESP}`, () => {
        test('keyed replies match on every master', async () => {
          const [ref, mock] = [await reference(RESP), await facade(RESP)]
          for (const tag of tags) {
            const cases = shapeCases(() => {
              const prefix = `{${tag}}:parity:${RESP}:${randomKey()}`
              return (suffix: string) => `${prefix}:${suffix}`
            }).filter(c => c.probe[0] !== 'CONFIG')
            for (const { name, setup, probe } of cases) {
              const key = probe[0] === 'XREAD' ? probe[4] : probe[1]
              for (const args of setup) {
                await ref.sendCommand(args[1], false, args)
                await mock.sendCommand(args)
              }
              assert.deepStrictEqual(
                await outcome(() => mock.sendCommand(probe)),
                await outcome(() =>
                  ref.sendCommand(key, !WRITES.has(probe[0]), probe),
                ),
                `${name} on slot ${clusterKeySlot(key)}`,
              )
            }
          }
        })

        test('a multi-key command spanning slots is refused with CROSSSLOT', async () => {
          const [ref, mock] = [await reference(RESP), await facade(RESP)]
          const args = ['MSET', `{${tags[0]}}:x`, '1', `{${tags[1]}}:y`, '2']
          assert.deepStrictEqual(
            await outcome(() => mock.sendCommand(args)),
            await outcome(() => ref.sendCommand(args[1], false, args)),
          )
        })

        test('EVAL routes by its declared keys', async () => {
          const [ref, mock] = [await reference(RESP), await facade(RESP)]
          for (const tag of tags) {
            const key = `{${tag}}:parity-eval:${RESP}:${randomKey()}`
            const script =
              "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
            assert.deepStrictEqual(
              await mock.eval(script, { keys: [key], arguments: ['v'] }),
              await ref.eval(script, { keys: [key], arguments: ['v'] }),
            )
          }
        })
      })
    }
  },
)

/** `[start, end]` of every range in a CLUSTER SLOTS reply. */
function slotRanges(reply: unknown): [number, number][] {
  assert.ok(Array.isArray(reply), 'CLUSTER SLOTS reply')
  return reply.map(range => [Number(range[0]), Number(range[1])])
}

/** One hash tag whose slot falls inside each of `ranges`. */
function tagsCoveringRanges(ranges: [number, number][]): string[] {
  const tags: string[] = []
  for (const [start, end] of ranges) {
    for (let i = 0; ; i++) {
      const tag = `parity-${start}-${i}`
      const slot = clusterKeySlot(tag)
      if (slot >= start && slot <= end) {
        tags.push(tag)
        break
      }
    }
  }
  return tags
}

async function waitFor(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 2000
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error('timed out waiting for pub/sub delivery')
    }
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}
