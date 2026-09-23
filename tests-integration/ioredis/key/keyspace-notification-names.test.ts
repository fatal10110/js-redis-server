import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

// Pins the keyspace-event *names* real Redis publishes for commands whose
// event is named after the underlying operation rather than the command:
// blocking / multi-key / move-style pops (#446), XGROUP subcommands (#381),
// the 8.x hash-field commands, lazily purged hash fields, and STORE targets
// (#486 review).

const testRunner = new TestRunner()

describe(`Keyspace notification names (${testRunner.getBackendName()})`, () => {
  let port: number
  const clients: Redis[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const client of clients) client.disconnect()
    clients.length = 0
    await testRunner.cleanup()
  })

  test('BLPOP / BRPOP publish lpop / rpop (#446)', async () => {
    const l1 = randomKey()
    const l2 = randomKey()
    const events = await capture({ l1, l2 }, async actor => {
      await actor.rpush(l1, 'a')
      assert.deepStrictEqual(await actor.blpop(l1, 1), [l1, 'a'])
      await actor.rpush(l2, 'a', 'b')
      assert.deepStrictEqual(await actor.brpop(l2, 1), [l2, 'b'])
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
      const listServed = listBlocker.brpop(list, 0)
      await settle()
      await actor.lpush(list, 'a')
      assert.deepStrictEqual(await listServed, [list, 'a'])

      const zsetBlocker = await connect()
      const zsetServed = zsetBlocker.bzpopmin(zset, 0)
      await settle()
      await actor.zadd(zset, 1, 'm')
      assert.deepStrictEqual(await zsetServed, [zset, 'm', '1'])
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
      await actor.rpush(src, 'a', 'b', 'c')
      await actor.rpush(dst, 'x')
      assert.strictEqual(await actor.lmove(src, dst, 'LEFT', 'RIGHT'), 'a')
      assert.strictEqual(await actor.rpoplpush(src, dst), 'c')
      assert.strictEqual(await actor.blmove(src, dst, 'RIGHT', 'LEFT', 1), 'b')
      // Same key: a rotation — never emptied, so never deleted.
      await actor.rpush(same, 'a')
      assert.strictEqual(await actor.lmove(same, same, 'LEFT', 'RIGHT'), 'a')
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
      await actor.rpush(l1, 'a')
      assert.deepStrictEqual(await actor.lmpop(1, l1, 'LEFT'), [l1, ['a']])
      await actor.rpush(l2, 'a', 'b')
      assert.deepStrictEqual(await actor.blmpop(1, 1, l2, 'RIGHT'), [l2, ['b']])
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
      await actor.zadd(z1, 1, 'a')
      assert.deepStrictEqual(await actor.zmpop(1, z1, 'MIN'), [
        z1,
        [['a', '1']],
      ])
      await actor.zadd(z2, 1, 'a', 2, 'b')
      assert.deepStrictEqual(await actor.bzmpop(1, 1, z2, 'MAX'), [
        z2,
        [['b', '2']],
      ])
      await actor.zadd(z3, 1, 'a')
      assert.deepStrictEqual(await actor.bzpopmin(z3, 1), [z3, 'a', '1'])
      await actor.zadd(z4, 1, 'a', 2, 'b')
      assert.deepStrictEqual(await actor.bzpopmax(z4, 1), [z4, 'b', '2'])
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
      await actor.sadd(src, 'a')
      assert.strictEqual(await actor.smove(src, dst, 'a'), 1)
      // Same key: membership check only, nothing published.
      await actor.sadd(same, 'a')
      assert.strictEqual(await actor.smove(same, same, 'a'), 1)
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
        await actor.hset(getdel, 'f', 'v')
        await actor.hgetdel(getdel, 'FIELDS', 1, 'f')
        await actor.hset(getexPast, 'f', 'v')
        await actor.hgetex(getexPast, 'PXAT', 1, 'FIELDS', 1, 'f')
        await actor.hset(getexEx, 'f', 'v')
        await actor.hgetex(getexEx, 'EX', 100, 'FIELDS', 1, 'f')
        await actor.hset(getexPersist, 'f', 'v')
        await actor.hexpire(getexPersist, 100, 'FIELDS', 1, 'f')
        await actor.hgetex(getexPersist, 'PERSIST', 'FIELDS', 1, 'f')
        await actor.hset(pexpire, 'f', 'v')
        await actor.hpexpire(pexpire, 100000, 'FIELDS', 1, 'f')
        await actor.hset(pexpireatPast, 'f', 'v', 'g', 'w')
        await actor.hpexpireat(pexpireatPast, 1, 'FIELDS', 1, 'f')
        await actor.hset(expireZero, 'f', 'v')
        await actor.hexpire(expireZero, 0, 'FIELDS', 1, 'f')
        await actor.hsetex(setexPx, 'PX', 100000, 'FIELDS', 1, 'f', 'v')
        await actor.hset(setexPast, 'g', 'w')
        await actor.hsetex(setexPast, 'PXAT', 1, 'FIELDS', 1, 'f', 'v')
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
      await actor.hset(whole, 'f', 'v')
      await actor.hpexpire(whole, 50, 'FIELDS', 1, 'f')
      await actor.hset(partial, 'f', 'v', 'g', 'w')
      await actor.hpexpire(partial, 50, 'FIELDS', 1, 'f')
      await new Promise(resolve => setTimeout(resolve, 500))

      assert.deepStrictEqual(await actor.hgetall(whole), {})
      assert.strictEqual(await actor.hlen(whole), 0)
      assert.deepStrictEqual(await actor.hgetall(partial), { g: 'w' })
      assert.strictEqual(await actor.hlen(partial), 1)
      assert.deepStrictEqual(await actor.hscan(partial, 0), ['0', ['g', 'w']])
      assert.deepStrictEqual(await actor.httl(partial, 'FIELDS', 1, 'f'), [-2])
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

  test('expired hash fields are removed by active expiry, with no access to the key', async () => {
    // Real Redis' active expiry drops expired fields on its own timer: a
    // subscriber gets `hexpired` (and `del` once the hash is empty) without
    // anyone touching the key, and EXISTS then reports it gone.
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    await subscriber.psubscribe('__keyevent@0__:*')
    await settle()

    const whole = randomKey()
    const partial = randomKey()
    const events: string[] = []
    let settled!: () => void
    const bothExpired = new Promise<void>(resolve => {
      settled = resolve
    })
    subscriber.on('pmessage', (_pattern, channel: string, key: string) => {
      const name = key === whole ? 'whole' : key === partial ? 'partial' : null
      if (!name) return
      events.push(`${name}:${channel.slice('__keyevent@0__:'.length)}`)
      if (events.includes('whole:del') && events.includes('partial:hexpired')) {
        settled()
      }
    })

    await actor.hset(whole, 'f', 'v', 'g', 'w')
    await actor.hpexpire(whole, 50, 'FIELDS', 2, 'f', 'g')
    await actor.hset(partial, 'f', 'v', 'g', 'w')
    await actor.hpexpire(partial, 50, 'FIELDS', 1, 'f')
    await withTimeout(bothExpired, 3000, `no active field expiry: ${events}`)

    assert.deepStrictEqual(
      events.filter(event => event.startsWith('whole:')),
      ['whole:hset', 'whole:hexpire', 'whole:hexpired', 'whole:del'],
    )
    assert.deepStrictEqual(
      events.filter(event => event.startsWith('partial:')),
      ['partial:hset', 'partial:hexpire', 'partial:hexpired'],
    )
    assert.strictEqual(await actor.exists(whole), 0)
    assert.deepStrictEqual(await actor.hgetall(partial), { g: 'w' })
    await actor.del(partial)
  })

  test('XGROUP subcommands publish xgroup-<subcommand> (#381)', async () => {
    const stream = randomKey()
    const created = randomKey()
    const events = await capture({ stream, created }, async actor => {
      await actor.xadd(stream, '1-1', 'f', 'v')
      await actor.xgroup('CREATE', stream, 'g', '0')
      assert.strictEqual(
        await actor.xgroup('CREATECONSUMER', stream, 'g', 'c'),
        1,
      )
      assert.strictEqual(
        await actor.xgroup('CREATECONSUMER', stream, 'g', 'c'),
        0,
      )
      await actor.xgroup('SETID', stream, 'g', '0')
      assert.strictEqual(await actor.xgroup('DELCONSUMER', stream, 'g', 'c'), 0)
      assert.strictEqual(
        await actor.xgroup('DELCONSUMER', stream, 'g', 'missing'),
        0,
      )
      assert.strictEqual(await actor.xgroup('DESTROY', stream, 'g'), 1)
      assert.strictEqual(await actor.xgroup('DESTROY', stream, 'g'), 0)
      await actor.xgroup('CREATE', created, 'g', '$', 'MKSTREAM')
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
      await actor.xadd(stream, '1-1', 'f', 'v')
      await actor.xgroup('CREATE', stream, 'g', '0')
      await actor.xreadgroup('GROUP', 'g', 'reader', 'STREAMS', stream, '>')
      // An existing consumer publishes nothing.
      await actor.xreadgroup('GROUP', 'g', 'reader', 'STREAMS', stream, '>')
      await actor.xclaim(stream, 'g', 'claimer', 0, '1-1')
      await actor.xautoclaim(stream, 'g', 'autoclaimer', 0, '0')
      await actor.xautoclaim(stream, 'g', 'autoclaimer', 0, '0')
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
        await actor.rpush(sortSrc, '3', '1')
        await actor.rpush(sortDst, 'x')
        assert.strictEqual(await actor.sort(sortSrc, 'STORE', sortDst), 2)
        assert.deepStrictEqual(await actor.lrange(sortDst, 0, -1), ['1', '3'])
        await actor.zadd(zSrc, 1, 'p')
        await actor.zadd(zDst, 1, 'x')
        assert.strictEqual(await actor.zrangestore(zDst, zSrc, 0, -1), 1)
        assert.deepStrictEqual(await actor.zrange(zDst, 0, -1), ['p'])
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
    steps: (actor: Redis) => Promise<void>,
  ): Promise<string[]> {
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', 'KEA')
    await subscriber.psubscribe('__keyevent@0__:*')
    await settle()

    const names = new Map(
      Object.entries(keys).map(([name, key]) => [key, name]),
    )
    const events: string[] = []
    subscriber.on('pmessage', (_pattern, channel: string, key: string) => {
      const name = names.get(key)
      if (name)
        events.push(`${name}:${channel.slice('__keyevent@0__:'.length)}`)
    })

    await steps(actor)

    // Delivery to one subscriber is ordered: once this sentinel arrives,
    // everything published before it has too.
    const sentinel = randomKey()
    const seen = new Promise<void>(resolve => {
      subscriber.on('pmessage', (_pattern, channel: string, key: string) => {
        if (channel === '__keyevent@0__:set' && key === sentinel) resolve()
      })
    })
    await actor.set(sentinel, 'v')
    await seen
    const published = events.slice()
    await actor.del(sentinel, ...Object.values(keys))
    return published
  }

  async function connect(): Promise<Redis> {
    const client = new Redis({ host: '127.0.0.1', port, lazyConnect: true })
    await client.connect()
    clients.push(client)
    return client
  }
})

function settle(): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, 50))
}

async function withTimeout(
  promise: Promise<void>,
  ms: number,
  message: string,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), ms)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}
