import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply, send } from './helpers'

/**
 * The STORE commands write their result with `setKey()` (#524): it replaces a
 * destination of any type, clears its TTL and dirties a WATCH on it, in one
 * write. Only an empty result deletes the destination. The same on every
 * profile (redis 6.2 through 8.0, valkey 8.0 and 9.0).
 */
const testRunner = new TestRunner()

const PALERMO = ['13.361389', '38.115556']

type Store = {
  name: string
  // The command writing `dst` from the sources of tag `t`, and its reply.
  args: (dst: string, t: string) => string[]
  reply: string
  type: 'set' | 'zset'
  // The destination's contents read back as a RESP2 array.
  read: (dst: string) => string[]
  contents: string
}

const SMEMBERS = (dst: string) => ['SMEMBERS', dst]
const ZRANGE = (dst: string) => ['ZRANGE', dst, '0', '-1', 'WITHSCORES']

const STORES: Store[] = [
  {
    name: 'SUNIONSTORE',
    args: (dst, t) => ['SUNIONSTORE', dst, `${t}:a`, `${t}:a2`],
    reply: ':3\r\n',
    type: 'set',
    read: SMEMBERS,
    contents: '*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n',
  },
  {
    name: 'SINTERSTORE',
    args: (dst, t) => ['SINTERSTORE', dst, `${t}:a`, `${t}:a2`],
    reply: ':1\r\n',
    type: 'set',
    read: SMEMBERS,
    contents: '*1\r\n$1\r\n2\r\n',
  },
  {
    name: 'SDIFFSTORE',
    args: (dst, t) => ['SDIFFSTORE', dst, `${t}:a`, `${t}:a2`],
    reply: ':1\r\n',
    type: 'set',
    read: SMEMBERS,
    contents: '*1\r\n$1\r\n1\r\n',
  },
  {
    name: 'ZUNIONSTORE',
    args: (dst, t) => ['ZUNIONSTORE', dst, '2', `${t}:z`, `${t}:z2`],
    reply: ':2\r\n',
    type: 'zset',
    read: ZRANGE,
    contents: '*4\r\n$1\r\nx\r\n$1\r\n1\r\n$1\r\ny\r\n$1\r\n7\r\n',
  },
  {
    name: 'ZINTERSTORE',
    args: (dst, t) => ['ZINTERSTORE', dst, '2', `${t}:z`, `${t}:z2`],
    reply: ':1\r\n',
    type: 'zset',
    read: ZRANGE,
    contents: '*2\r\n$1\r\ny\r\n$1\r\n7\r\n',
  },
  {
    name: 'ZDIFFSTORE',
    args: (dst, t) => ['ZDIFFSTORE', dst, '2', `${t}:z`, `${t}:z2`],
    reply: ':1\r\n',
    type: 'zset',
    read: ZRANGE,
    contents: '*2\r\n$1\r\nx\r\n$1\r\n1\r\n',
  },
  {
    name: 'ZRANGESTORE',
    args: (dst, t) => ['ZRANGESTORE', dst, `${t}:z`, '0', '0'],
    reply: ':1\r\n',
    type: 'zset',
    read: ZRANGE,
    contents: '*2\r\n$1\r\nx\r\n$1\r\n1\r\n',
  },
  {
    name: 'GEOSEARCHSTORE',
    args: (dst, t) => [
      'GEOSEARCHSTORE',
      dst,
      `${t}:g`,
      'FROMMEMBER',
      'Palermo',
      'BYRADIUS',
      '10',
      'km',
    ],
    reply: ':1\r\n',
    type: 'zset',
    read: dst => ['ZRANGE', dst, '0', '-1'],
    contents: '*1\r\n$7\r\nPalermo\r\n',
  },
  {
    name: 'GEORADIUS STORE',
    args: (dst, t) => [
      'GEORADIUS',
      `${t}:g`,
      ...PALERMO,
      '10',
      'km',
      'STORE',
      dst,
    ],
    reply: ':1\r\n',
    type: 'zset',
    read: dst => ['ZRANGE', dst, '0', '-1'],
    contents: '*1\r\n$7\r\nPalermo\r\n',
  },
  {
    name: 'GEORADIUSBYMEMBER STOREDIST',
    args: (dst, t) => [
      'GEORADIUSBYMEMBER',
      `${t}:g`,
      'Palermo',
      '10',
      'km',
      'STOREDIST',
      dst,
    ],
    reply: ':1\r\n',
    type: 'zset',
    read: dst => ['ZRANGE', dst, '0', '-1', 'WITHSCORES'],
    contents: '*2\r\n$7\r\nPalermo\r\n$1\r\n0\r\n',
  },
]

// Each destination type, written by the command creating it.
const DESTINATIONS: Record<string, (dst: string) => string[]> = {
  string: dst => ['SET', dst, 'v'],
  list: dst => ['RPUSH', dst, 'v'],
  hash: dst => ['HSET', dst, 'f', 'v'],
  set: dst => ['SADD', dst, 'v'],
  zset: dst => ['ZADD', dst, '1', 'v'],
  stream: dst => ['XADD', dst, '*', 'f', 'v'],
}

describe(`Raw TCP STORE over a destination of any type (${testRunner.getBackendName()}, ${activeProfile})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(conn)
    return conn
  }

  // Sources a={1,2} a2={2,3}, z={x:1,y:2} z2={y:5}, g={Palermo}, under one
  // hash tag so a cluster-aware server would accept them together.
  async function sources(conn: RawRedisConnection): Promise<string> {
    const t = `{store-overwrite:${randomKey()}}`
    await send(conn, ['SADD', `${t}:a`, '1', '2'])
    await send(conn, ['SADD', `${t}:a2`, '2', '3'])
    await send(conn, ['ZADD', `${t}:z`, '1', 'x', '2', 'y'])
    await send(conn, ['ZADD', `${t}:z2`, '5', 'y'])
    await send(conn, ['GEOADD', `${t}:g`, ...PALERMO, 'Palermo'])
    return t
  }

  for (const store of STORES) {
    test(`${store.name} replaces a destination of any type and clears its TTL`, async () => {
      const conn = await connect()
      const t = await sources(conn)
      for (const [type, create] of Object.entries(DESTINATIONS)) {
        const dst = `${t}:dst-${type}`
        await send(conn, create(dst))
        await expectReply(conn, ['PEXPIRE', dst, '100000'], ':1\r\n')
        await expectReply(conn, store.args(dst, t), store.reply)
        await expectReply(conn, ['TYPE', dst], `+${store.type}\r\n`)
        await expectReply(conn, ['TTL', dst], ':-1\r\n')
        await expectReply(conn, store.read(dst), store.contents)
      }
    })

    test(`${store.name} over a watched destination makes EXEC answer nil`, async () => {
      const watcher = await connect()
      const writer = await connect()
      const t = await sources(writer)
      for (const type of ['missing', 'string', 'set', 'zset']) {
        const dst = `${t}:dst-${type}`
        if (type !== 'missing') await send(writer, DESTINATIONS[type](dst))
        await expectReply(watcher, ['WATCH', dst], '+OK\r\n')
        await expectReply(writer, store.args(dst, t), store.reply)
        await expectReply(watcher, ['MULTI'], '+OK\r\n')
        await expectReply(watcher, ['PING'], '+QUEUED\r\n')
        await expectReply(watcher, ['EXEC'], '*-1\r\n')
      }
    })
  }

  test('an empty result deletes the destination, whatever its type', async () => {
    const conn = await connect()
    const t = await sources(conn)
    const missing = `${t}:missing`
    for (const [type, create] of Object.entries(DESTINATIONS)) {
      const dst = `${t}:dst-${type}`
      await send(conn, create(dst))
      await expectReply(conn, ['SUNIONSTORE', dst, missing], ':0\r\n')
      await expectReply(conn, ['EXISTS', dst], ':0\r\n')
      await send(conn, create(dst))
      await expectReply(conn, ['ZUNIONSTORE', dst, '1', missing], ':0\r\n')
      await expectReply(conn, ['EXISTS', dst], ':0\r\n')
    }
  })

  test('a wrong-type source leaves the destination untouched', async () => {
    const conn = await connect()
    const t = await sources(conn)
    const dst = `${t}:dst`
    const WRONGTYPE =
      '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
    await send(conn, ['SET', dst, 'v'])
    await send(conn, ['SET', `${t}:str`, 'v'])
    await expectReply(
      conn,
      ['SUNIONSTORE', dst, `${t}:a`, `${t}:str`],
      WRONGTYPE,
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', dst, '2', `${t}:z`, `${t}:str`],
      WRONGTYPE,
    )
    await expectReply(conn, ['GET', dst], '$1\r\nv\r\n')
  })

  test('inside MULTI and from a script, a STORE replaces a string', async () => {
    const conn = await connect()
    const t = await sources(conn)
    const dst = `${t}:dst`
    await send(conn, ['SET', dst, 'v'])
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SUNIONSTORE', dst, `${t}:a`], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*1\r\n:2\r\n')
    await expectReply(conn, ['TYPE', dst], '+set\r\n')

    for (const [script, type] of [
      ["return redis.pcall('ZUNIONSTORE',KEYS[1],1,KEYS[2])", 'zset'],
      ["return redis.pcall('SINTERSTORE',KEYS[1],KEYS[2])", 'set'],
    ]) {
      await send(conn, ['SET', dst, 'v'])
      const source = type === 'zset' ? `${t}:z` : `${t}:a`
      await expectReply(conn, ['EVAL', script, '2', dst, source], ':2\r\n')
      await expectReply(conn, ['TYPE', dst], `+${type}\r\n`)
    }
  })

  test('a STORE whose destination is also a source', async () => {
    const conn = await connect()
    const t = await sources(conn)
    await expectReply(
      conn,
      ['SUNIONSTORE', `${t}:a`, `${t}:a`, `${t}:a2`],
      ':3\r\n',
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', `${t}:z`, '2', `${t}:z`, `${t}:z2`],
      ':2\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGE', `${t}:z`, '0', '-1', 'WITHSCORES'],
      '*4\r\n$1\r\nx\r\n$1\r\n1\r\n$1\r\ny\r\n$1\r\n7\r\n',
    )
  })
})
