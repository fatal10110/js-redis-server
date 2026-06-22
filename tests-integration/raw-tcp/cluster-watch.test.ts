import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { connectToRawSlotOwner, expectReply } from './helpers'

/**
 * WATCH/UNWATCH dirty-tracking semantics over bare sockets on a cluster node.
 *
 * These drive raw, interleaved WATCH/MULTI/EXEC — often across two connections
 * to the same slot owner so one session can dirty the other's watch — which a
 * client's transaction API can't express (ioredis `.watch()`/`.multi()` build a
 * client-side pipeline). Moved out of the ioredis suite, where they used
 * `.call(...)`. The typed-API WATCH tests (client behavior) stay in
 * `ioredis/watch.test.ts`.
 */
const testRunner = new TestRunner()

describe(`Raw TCP cluster WATCH semantics (${testRunner.getBackendName()})`, () => {
  let ports: number[]
  const connections: RawRedisConnection[] = []

  before(async () => {
    ports = await testRunner.setupRawCluster()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  async function ownerOf(key: string): Promise<RawRedisConnection> {
    const conn = await connectToRawSlotOwner(ports, key)
    connections.push(conn)
    return conn
  }

  test('UNWATCH clears watched keys so the transaction still runs', async () => {
    const key = `watch-unwatch:${randomKey()}`
    const directConn = await ownerOf(key)
    const otherConn = await ownerOf(key)

    await expectReply(directConn, ['SET', key, 'initial'], '+OK\r\n')
    await expectReply(directConn, ['WATCH', key], '+OK\r\n')
    await expectReply(directConn, ['UNWATCH'], '+OK\r\n')

    // Mutate the (no-longer-watched) key from the other connection.
    await expectReply(otherConn, ['SET', key, 'modified'], '+OK\r\n')

    await expectReply(directConn, ['MULTI'], '+OK\r\n')
    await expectReply(directConn, ['SET', key, 'transactional'], '+QUEUED\r\n')
    // Succeeds because we unwatched.
    await expectReply(directConn, ['EXEC'], '*1\r\n+OK\r\n')
  })

  test('DISCARD clears watched keys so the next transaction still runs', async () => {
    const key = `watch-discard:${randomKey()}`
    const directConn = await ownerOf(key)
    const otherConn = await ownerOf(key)

    await expectReply(directConn, ['SET', key, 'initial'], '+OK\r\n')
    await expectReply(directConn, ['WATCH', key], '+OK\r\n')

    await expectReply(directConn, ['MULTI'], '+OK\r\n')
    await expectReply(directConn, ['SET', key, 'first'], '+QUEUED\r\n')
    await expectReply(directConn, ['DISCARD'], '+OK\r\n')

    await expectReply(otherConn, ['SET', key, 'modified'], '+OK\r\n')

    // Watches were cleared by DISCARD, so this transaction is not dirtied.
    await expectReply(directConn, ['MULTI'], '+OK\r\n')
    await expectReply(directConn, ['SET', key, 'second'], '+QUEUED\r\n')
    await expectReply(directConn, ['EXEC'], '*1\r\n+OK\r\n')
  })

  // Each case: a no-op mutation on a watched key must NOT dirty the WATCH, so
  // the following MULTI/EXEC runs (returns the queued OK array).
  const noopCases: Array<{
    name: string
    seed: [string[], string][]
    mutate: [string[], string]
  }> = [
    {
      name: 'HDEL missing field',
      seed: [[['HSET', 'K', 'field', 'value'], ':1\r\n']],
      mutate: [['HDEL', 'K', 'missing'], ':0\r\n'],
    },
    {
      name: 'HSETNX existing field',
      seed: [[['HSET', 'K', 'field', 'value'], ':1\r\n']],
      mutate: [['HSETNX', 'K', 'field', 'other'], ':0\r\n'],
    },
    {
      name: 'LREM missing value',
      seed: [[['RPUSH', 'K', 'one', 'two'], ':2\r\n']],
      mutate: [['LREM', 'K', '0', 'missing'], ':0\r\n'],
    },
    {
      name: 'SREM missing member',
      seed: [[['SADD', 'K', 'member'], ':1\r\n']],
      mutate: [['SREM', 'K', 'missing'], ':0\r\n'],
    },
    {
      name: 'ZREM missing member',
      seed: [[['ZADD', 'K', '1', 'member'], ':1\r\n']],
      mutate: [['ZREM', 'K', 'missing'], ':0\r\n'],
    },
    {
      name: 'ZADD XX missing member',
      seed: [[['ZADD', 'K', '1', 'member'], ':1\r\n']],
      mutate: [['ZADD', 'K', 'XX', '2', 'missing'], ':0\r\n'],
    },
    {
      name: 'XDEL missing id',
      seed: [[['XADD', 'K', '1-0', 'field', 'value'], '$3\r\n1-0\r\n']],
      mutate: [['XDEL', 'K', '2-0'], ':0\r\n'],
    },
    {
      name: 'XTRIM no removed entries',
      seed: [[['XADD', 'K', '1-0', 'field', 'value'], '$3\r\n1-0\r\n']],
      mutate: [['XTRIM', 'K', 'MAXLEN', '10'], ':0\r\n'],
    },
  ]

  test('no-op collection mutations do not dirty a watched key', async () => {
    for (const item of noopCases) {
      const key = `watch-noop:${randomKey()}`
      const conn = await ownerOf(key)
      const sub = (args: string[]): string[] =>
        args.map(a => (a === 'K' ? key : a))

      for (const [args, reply] of item.seed) {
        await expectReply(conn, sub(args), reply)
      }
      await expectReply(conn, ['WATCH', key], '+OK\r\n')
      await expectReply(conn, sub(item.mutate[0]), item.mutate[1])

      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['SET', key, 'after'], '+QUEUED\r\n')
      // Not dirtied: EXEC runs the queue.
      await expectReply(conn, ['EXEC'], '*1\r\n+OK\r\n')
    }
  })

  test('RENAME key key self-rename is a no-op and does not dirty a watched key', async () => {
    const key = `watch-selfrename:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['SET', key, 'value'], '+OK\r\n')
    await expectReply(conn, ['WATCH', key], '+OK\r\n')

    await expectReply(conn, ['RENAME', key, key], '+OK\r\n')
    await expectReply(conn, ['GET', key], '$5\r\nvalue\r\n')

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', key, 'after'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*1\r\n+OK\r\n')
    await expectReply(conn, ['GET', key], '$5\r\nafter\r\n')
  })

  test('RENAME key key on a missing key still errors with no such key', async () => {
    const key = `watch-selfrename-missing:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(conn, ['RENAME', key, key], '-ERR no such key\r\n')
  })

  test('RENAME with wrong number of arguments errors', async () => {
    const key = `watch-selfrename-arity:${randomKey()}`
    const conn = await ownerOf(key)

    await expectReply(
      conn,
      ['RENAME', key],
      "-ERR wrong number of arguments for 'rename' command\r\n",
    )
  })

  // Each case: an identical store-write to the watched destination DOES dirty
  // it (it rewrites the key), so the following MULTI/EXEC aborts with a nil.
  const storeCases: Array<{
    name: string
    seed: [string[], string][]
    store: [string[], string]
  }> = [
    {
      name: 'SINTERSTORE identical set',
      seed: [
        [['SADD', 'S', 'a', 'b', 'c'], ':3\r\n'],
        [['SINTERSTORE', 'K', 'S'], ':3\r\n'],
      ],
      store: [['SINTERSTORE', 'K', 'S'], ':3\r\n'],
    },
    {
      name: 'ZINTERSTORE identical sorted set',
      seed: [
        [['ZADD', 'S', '1', 'a', '2', 'b'], ':2\r\n'],
        [['ZINTERSTORE', 'K', '1', 'S'], ':2\r\n'],
      ],
      store: [['ZINTERSTORE', 'K', '1', 'S'], ':2\r\n'],
    },
  ]

  test('identical store writes dirty a watched destination key', async () => {
    for (const item of storeCases) {
      // Shared hash tag keeps destination and source in the same slot.
      const key = `{watch-store:${randomKey()}}`
      const source = `${key}:source`
      const conn = await ownerOf(key)
      const sub = (args: string[]): string[] =>
        args.map(a => (a === 'K' ? key : a === 'S' ? source : a))

      for (const [args, reply] of item.seed) {
        await expectReply(conn, sub(args), reply)
      }
      await expectReply(conn, ['WATCH', key], '+OK\r\n')
      await expectReply(conn, sub(item.store[0]), item.store[1])

      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['SET', key, 'after'], '+QUEUED\r\n')
      // Dirtied by the store rewrite: EXEC aborts with a nil array.
      await expectReply(conn, ['EXEC'], '*-1\r\n')
    }
  })
})
