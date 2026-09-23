import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createClient, RedisClientType } from 'redis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

// node-redis twin of ioredis/key/keyspace-notification-names.test.ts: the
// keyspace-event names real Redis publishes for commands whose event is named
// after the underlying operation — blocking / multi-key / move-style pops
// (#446), XGROUP subcommands (#381), the 8.x hash-field commands, lazily
// purged hash fields, and STORE targets (#486 review).

const testRunner = new TestRunner()

describe(`Keyspace notification names (node-redis, ${testRunner.getBackendName()})`, () => {
  let port: number
  const clients: RedisClientType[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const client of clients) client.destroy()
    clients.length = 0
    await testRunner.cleanup()
  })

  test('BLPOP / BRPOP publish lpop / rpop (#446)', async () => {
    const l1 = randomKey()
    const l2 = randomKey()
    const events = await capture({ l1, l2 }, async actor => {
      await actor.rPush(l1, 'a')
      assert.deepStrictEqual(await actor.blPop(l1, 1), {
        key: l1,
        element: 'a',
      })
      await actor.rPush(l2, ['a', 'b'])
      assert.deepStrictEqual(await actor.brPop(l2, 1), {
        key: l2,
        element: 'b',
      })
    })
    assert.deepStrictEqual(events, [
      'l1:rpush',
      'l1:lpop',
      'l1:del',
      'l2:rpush',
      'l2:rpop',
    ])
  })

  test('a blocked pop served by a push publishes the push, then the pop (#446)', async () => {
    const list = randomKey()
    const zset = randomKey()
    const events = await capture({ list, zset }, async actor => {
      const listBlocker = await connect()
      const listServed = listBlocker.brPop(list, 0)
      await settle()
      await actor.lPush(list, 'a')
      assert.deepStrictEqual(await listServed, { key: list, element: 'a' })

      const zsetBlocker = await connect()
      const zsetServed = zsetBlocker.bzPopMin(zset, 0)
      await settle()
      await actor.zAdd(zset, { score: 1, value: 'm' })
      assert.deepStrictEqual(await zsetServed, {
        key: zset,
        value: 'm',
        score: 1,
      })
    })
    assert.deepStrictEqual(events, [
      'list:lpush',
      'list:rpop',
      'list:del',
      'zset:zadd',
      'zset:zpopmin',
      'zset:del',
    ])
  })

  test('LMOVE / BLMOVE / RPOPLPUSH publish the destination push, then the source pop (#446)', async () => {
    const src = randomKey()
    const dst = randomKey()
    const same = randomKey()
    const events = await capture({ src, dst, same }, async actor => {
      await actor.rPush(src, ['a', 'b', 'c'])
      await actor.rPush(dst, 'x')
      assert.strictEqual(await actor.lMove(src, dst, 'LEFT', 'RIGHT'), 'a')
      assert.strictEqual(await actor.rPopLPush(src, dst), 'c')
      assert.strictEqual(await actor.blMove(src, dst, 'RIGHT', 'LEFT', 1), 'b')
      // Same key: a rotation — never emptied, so never deleted.
      await actor.rPush(same, 'a')
      assert.strictEqual(await actor.lMove(same, same, 'LEFT', 'RIGHT'), 'a')
    })
    assert.deepStrictEqual(events, [
      'src:rpush',
      'dst:rpush',
      'dst:rpush',
      'src:lpop',
      'dst:lpush',
      'src:rpop',
      'dst:lpush',
      'src:rpop',
      'src:del',
      'same:rpush',
      'same:rpush',
      'same:lpop',
    ])
  })

  test('LMPOP / BLMPOP publish lpop / rpop by direction (#446)', async () => {
    const l1 = randomKey()
    const l2 = randomKey()
    const events = await capture({ l1, l2 }, async actor => {
      await actor.rPush(l1, 'a')
      assert.deepStrictEqual(await actor.lmPop(l1, 'LEFT'), [l1, ['a']])
      await actor.rPush(l2, ['a', 'b'])
      assert.deepStrictEqual(await actor.blmPop(1, l2, 'RIGHT'), [l2, ['b']])
    })
    assert.deepStrictEqual(events, [
      'l1:rpush',
      'l1:lpop',
      'l1:del',
      'l2:rpush',
      'l2:rpop',
    ])
  })

  test('ZMPOP / BZMPOP / BZPOPMIN / BZPOPMAX publish zpopmin / zpopmax (#446)', async () => {
    const z1 = randomKey()
    const z2 = randomKey()
    const z3 = randomKey()
    const z4 = randomKey()
    const events = await capture({ z1, z2, z3, z4 }, async actor => {
      await actor.zAdd(z1, { score: 1, value: 'a' })
      assert.deepStrictEqual(await actor.zmPop(z1, 'MIN'), {
        key: z1,
        members: [{ value: 'a', score: 1 }],
      })
      await actor.zAdd(z2, [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
      ])
      assert.deepStrictEqual(await actor.bzmPop(1, z2, 'MAX'), {
        key: z2,
        members: [{ value: 'b', score: 2 }],
      })
      await actor.zAdd(z3, { score: 1, value: 'a' })
      assert.deepStrictEqual(await actor.bzPopMin(z3, 1), {
        key: z3,
        value: 'a',
        score: 1,
      })
      await actor.zAdd(z4, [
        { score: 1, value: 'a' },
        { score: 2, value: 'b' },
      ])
      assert.deepStrictEqual(await actor.bzPopMax(z4, 1), {
        key: z4,
        value: 'b',
        score: 2,
      })
    })
    assert.deepStrictEqual(events, [
      'z1:zadd',
      'z1:zpopmin',
      'z1:del',
      'z2:zadd',
      'z2:zpopmax',
      'z3:zadd',
      'z3:zpopmin',
      'z3:del',
      'z4:zadd',
      'z4:zpopmax',
    ])
  })

  test('SMOVE publishes srem on the source and sadd on the destination (#446)', async () => {
    const src = randomKey()
    const dst = randomKey()
    const same = randomKey()
    const events = await capture({ src, dst, same }, async actor => {
      await actor.sAdd(src, 'a')
      assert.strictEqual(await actor.sMove(src, dst, 'a'), 1)
      // Same key: membership check only, nothing published.
      await actor.sAdd(same, 'a')
      assert.strictEqual(await actor.sMove(same, same, 'a'), 1)
    })
    assert.deepStrictEqual(events, [
      'src:sadd',
      'src:srem',
      'src:del',
      'dst:sadd',
      'same:sadd',
    ])
  })

  test('8.x hash-field commands publish hdel / hexpire / hpersist', async () => {
    const getdel = randomKey()
    const getexPast = randomKey()
    const getexEx = randomKey()
    const getexPersist = randomKey()
    const pexpire = randomKey()
    const pexpireatPast = randomKey()
    const expireZero = randomKey()
    const setexPx = randomKey()
    const setexPast = randomKey()
    const events = await capture(
      {
        getdel,
        getexPast,
        getexEx,
        getexPersist,
        pexpire,
        pexpireatPast,
        expireZero,
        setexPx,
        setexPast,
      },
      async actor => {
        await actor.hSet(getdel, 'f', 'v')
        await actor.hGetDel(getdel, 'f')
        await actor.hSet(getexPast, 'f', 'v')
        await actor.hGetEx(getexPast, 'f', {
          expiration: { type: 'PXAT', value: 1 },
        })
        await actor.hSet(getexEx, 'f', 'v')
        await actor.hGetEx(getexEx, 'f', {
          expiration: { type: 'EX', value: 100 },
        })
        await actor.hSet(getexPersist, 'f', 'v')
        await actor.hExpire(getexPersist, 'f', 100)
        await actor.hGetEx(getexPersist, 'f', { expiration: 'PERSIST' })
        await actor.hSet(pexpire, 'f', 'v')
        await actor.hpExpire(pexpire, 'f', 100000)
        await actor.hSet(pexpireatPast, { f: 'v', g: 'w' })
        await actor.hpExpireAt(pexpireatPast, 'f', 1)
        await actor.hSet(expireZero, 'f', 'v')
        await actor.hExpire(expireZero, 'f', 0)
        await actor.hSetEx(
          setexPx,
          { f: 'v' },
          { expiration: { type: 'PX', value: 100000 } },
        )
        await actor.hSet(setexPast, 'g', 'w')
        await actor.hSetEx(
          setexPast,
          { f: 'v' },
          { expiration: { type: 'PXAT', value: 1 } },
        )
      },
    )
    assert.deepStrictEqual(events, [
      'getdel:hset',
      'getdel:hdel',
      'getdel:del',
      'getexPast:hset',
      'getexPast:hdel',
      'getexPast:del',
      'getexEx:hset',
      'getexEx:hexpire',
      'getexPersist:hset',
      'getexPersist:hexpire',
      'getexPersist:hpersist',
      'pexpire:hset',
      'pexpire:hexpire',
      'pexpireatPast:hset',
      'pexpireatPast:hdel',
      'expireZero:hset',
      'expireZero:hdel',
      'expireZero:del',
      'setexPx:hset',
      'setexPx:hexpire',
      'setexPast:hset',
      'setexPast:hset',
      'setexPast:hdel',
    ])
  })

  test('expired hash fields are published as hexpired, never as the reading command', async () => {
    // Real Redis drops expired fields by active expiry (`hexpired`, then `del`
    // once the hash is empty); a read afterwards publishes nothing. The mock
    // drops them on the next access — the read must still not publish its
    // own name (hgetall, hlen, hscan, httl).
    const whole = randomKey()
    const partial = randomKey()
    const events = await capture({ whole, partial }, async actor => {
      await actor.hSet(whole, 'f', 'v')
      await actor.hpExpire(whole, 'f', 50)
      await actor.hSet(partial, { f: 'v', g: 'w' })
      await actor.hpExpire(partial, 'f', 50)
      await new Promise(resolve => setTimeout(resolve, 500))

      assert.deepStrictEqual({ ...(await actor.hGetAll(whole)) }, {})
      assert.strictEqual(await actor.hLen(whole), 0)
      assert.deepStrictEqual({ ...(await actor.hGetAll(partial)) }, { g: 'w' })
      assert.strictEqual(await actor.hLen(partial), 1)
      assert.deepStrictEqual(await actor.hScan(partial, '0'), {
        cursor: '0',
        entries: [{ field: 'g', value: 'w' }],
      })
      assert.deepStrictEqual(await actor.hTTL(partial, 'f'), [-2])
    })
    // Per key: real Redis' active expiry does not order keys deterministically.
    assert.deepStrictEqual(
      events.filter(event => event.startsWith('whole:')),
      ['whole:hset', 'whole:hexpire', 'whole:hexpired', 'whole:del'],
    )
    assert.deepStrictEqual(
      events.filter(event => event.startsWith('partial:')),
      ['partial:hset', 'partial:hexpire', 'partial:hexpired'],
    )
  })

  test('XGROUP subcommands publish xgroup-<subcommand> (#381)', async () => {
    const stream = randomKey()
    const created = randomKey()
    const events = await capture({ stream, created }, async actor => {
      await actor.xAdd(stream, '1-1', { f: 'v' })
      await actor.xGroupCreate(stream, 'g', '0')
      assert.strictEqual(await actor.xGroupCreateConsumer(stream, 'g', 'c'), 1)
      assert.strictEqual(await actor.xGroupCreateConsumer(stream, 'g', 'c'), 0)
      await actor.xGroupSetId(stream, 'g', '0')
      assert.strictEqual(await actor.xGroupDelConsumer(stream, 'g', 'c'), 0)
      assert.strictEqual(
        await actor.xGroupDelConsumer(stream, 'g', 'missing'),
        0,
      )
      assert.strictEqual(await actor.xGroupDestroy(stream, 'g'), 1)
      assert.strictEqual(await actor.xGroupDestroy(stream, 'g'), 0)
      await actor.xGroupCreate(created, 'g', '$', { MKSTREAM: true })
    })
    assert.deepStrictEqual(events, [
      'stream:xadd',
      'stream:xgroup-create',
      'stream:xgroup-createconsumer',
      'stream:xgroup-setid',
      'stream:xgroup-delconsumer',
      'stream:xgroup-destroy',
      'created:xgroup-create',
    ])
  })

  test('XREADGROUP / XCLAIM / XAUTOCLAIM publish xgroup-createconsumer for a new consumer', async () => {
    const stream = randomKey()
    const events = await capture({ stream }, async actor => {
      await actor.xAdd(stream, '1-1', { f: 'v' })
      await actor.xGroupCreate(stream, 'g', '0')
      await actor.xReadGroup('g', 'reader', { key: stream, id: '>' })
      // An existing consumer publishes nothing.
      await actor.xReadGroup('g', 'reader', { key: stream, id: '>' })
      await actor.xClaim(stream, 'g', 'claimer', 0, '1-1')
      await actor.xAutoClaim(stream, 'g', 'autoclaimer', 0, '0')
      await actor.xAutoClaim(stream, 'g', 'autoclaimer', 0, '0')
    })
    assert.deepStrictEqual(events, [
      'stream:xadd',
      'stream:xgroup-create',
      'stream:xgroup-createconsumer',
      'stream:xgroup-createconsumer',
      'stream:xgroup-createconsumer',
    ])
  })

  test('a STORE over an existing destination publishes one event', async () => {
    const sortSrc = randomKey()
    const sortDst = randomKey()
    const zSrc = randomKey()
    const zDst = randomKey()
    const events = await capture(
      { sortSrc, sortDst, zSrc, zDst },
      async actor => {
        await actor.rPush(sortSrc, ['3', '1'])
        await actor.rPush(sortDst, 'x')
        assert.strictEqual(await actor.sortStore(sortSrc, sortDst), 2)
        assert.deepStrictEqual(await actor.lRange(sortDst, 0, -1), ['1', '3'])
        await actor.zAdd(zSrc, { score: 1, value: 'p' })
        await actor.zAdd(zDst, { score: 1, value: 'x' })
        assert.strictEqual(await actor.zRangeStore(zDst, zSrc, 0, -1), 1)
        assert.deepStrictEqual(await actor.zRange(zDst, 0, -1), ['p'])
      },
    )
    assert.deepStrictEqual(events, [
      'sortSrc:rpush',
      'sortDst:rpush',
      'sortDst:sortstore',
      'zSrc:zadd',
      'zDst:zadd',
      'zDst:zrangestore',
    ])
  })

  /**
   * Run `steps` with keyevent notifications on, then return the events
   * published for the named keys, in order, as `<name>:<event>`.
   */
  async function capture(
    keys: Record<string, string>,
    steps: (actor: RedisClientType) => Promise<void>,
  ): Promise<string[]> {
    const actor = await connect()
    const subscriber = await connect()
    await actor.configSet('notify-keyspace-events', 'KEA')

    const names = new Map(
      Object.entries(keys).map(([name, key]) => [key, name]),
    )
    const events: string[] = []
    const sentinel = randomKey()
    let sentinelSeen!: () => void
    const seen = new Promise<void>(resolve => {
      sentinelSeen = resolve
    })
    await subscriber.pSubscribe('__keyevent@0__:*', (key, channel) => {
      const event = channel.slice('__keyevent@0__:'.length)
      if (event === 'set' && key === sentinel) sentinelSeen()
      const name = names.get(key)
      if (name) events.push(`${name}:${event}`)
    })
    await settle()

    await steps(actor)

    // Delivery to one subscriber is ordered: once this sentinel arrives,
    // everything published before it has too.
    await actor.set(sentinel, 'v')
    await seen
    const published = events.slice()
    await actor.del([sentinel, ...Object.values(keys)])
    return published
  }

  async function connect(): Promise<RedisClientType> {
    const client = createClient({
      url: `redis://127.0.0.1:${port}`,
    }) as RedisClientType
    client.on('error', () => {})
    await client.connect()
    clients.push(client)
    return client
  }
})

function settle(): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, 50))
}
