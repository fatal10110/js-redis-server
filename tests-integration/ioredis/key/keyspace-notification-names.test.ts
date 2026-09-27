import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

// Pins the keyspace-event *names* real Redis publishes for commands whose
// event is named after the underlying operation rather than the command:
// blocking / multi-key / move-style pops (#446), XGROUP subcommands (#381),
// the 8.x hash-field commands, lazily purged hash fields, STORE targets
// (#486 review), TTLs set by a write (#380), and MOVE / COPY ... DB (#445).

const testRunner = new TestRunner()

describe(`Keyspace notification names (${testRunner.getBackendName()})`, () => {
  // Every client is a duplicate() of one standalone client: a new connection to
  // the same server (mock/real), or to the same in-memory keyspace (socketless).
  let base: Redis
  const clients: Redis[] = []

  before(async () => {
    base = await testRunner.setupIoredisStandalone()
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

  test('SET EX|PX|EXAT / SETEX / PSETEX publish set, then expire (#380)', async () => {
    const ex = randomKey()
    const pxGet = randomKey()
    const exat = randomKey()
    const keepttl = randomKey()
    const nx = randomKey()
    const setex = randomKey()
    const psetex = randomKey()
    const events = await capture(
      { ex, pxGet, exat, keepttl, nx, setex, psetex },
      async actor => {
        await actor.set(ex, 'v', 'EX', 100)
        assert.strictEqual(
          await actor.set(pxGet, 'v', 'PX', 100000, 'GET'),
          null,
        )
        const at = Math.floor(Date.now() / 1000) + 100
        await actor.set(exat, 'v', 'EXAT', at)
        await actor.set(keepttl, 'v', 'EX', 100)
        // KEEPTTL keeps the TTL rather than setting one: no expire.
        await actor.set(keepttl, 'w', 'KEEPTTL')
        assert.strictEqual(await actor.set(nx, 'v', 'EX', 100, 'NX'), 'OK')
        // NX refused: nothing is written, so nothing is published.
        assert.strictEqual(await actor.set(nx, 'w', 'EX', 100, 'NX'), null)
        await actor.setex(setex, 100, 'v')
        await actor.psetex(psetex, 100000, 'v')
      },
    )
    assert.deepStrictEqual(events, [
      'ex:set',
      'ex:expire',
      'pxGet:set',
      'pxGet:expire',
      'exat:set',
      'exat:expire',
      'keepttl:set',
      'keepttl:expire',
      'keepttl:set',
      'nx:set',
      'nx:expire',
      'setex:set',
      'setex:expire',
      'psetex:set',
      'psetex:expire',
    ])
  })

  test('GETEX publishes expire / persist / del, never getex (#380)', async () => {
    const key = randomKey()
    const past = randomKey()
    const events = await capture({ key, past }, async actor => {
      await actor.set(key, 'v')
      assert.strictEqual(await actor.getex(key, 'EX', 100), 'v')
      assert.strictEqual(
        await actor.getex(key, 'PXAT', Date.now() + 100000),
        'v',
      )
      assert.strictEqual(await actor.getex(key, 'PERSIST'), 'v')
      // No TTL left to remove, and no option at all: nothing changes.
      assert.strictEqual(await actor.getex(key, 'PERSIST'), 'v')
      assert.strictEqual(await actor.getex(key), 'v')
      // A time already past deletes the key there and then.
      await actor.set(past, 'v')
      assert.strictEqual(await actor.getex(past, 'EXAT', 1), 'v')
      assert.strictEqual(await actor.exists(past), 0)
    })
    assert.deepStrictEqual(events, [
      'key:set',
      'key:expire',
      'key:expire',
      'key:persist',
      'past:set',
      'past:del',
    ])
  })

  test('MOVE publishes move_from on the source database, then move_to on the target (#445)', async () => {
    const moved = randomKey()
    const hash = randomKey()
    const taken = randomKey()
    const events = await capture({ moved, hash, taken }, async actor => {
      await actor.set(taken, 'v')
      await actor.select(1)
      await actor.set(moved, 'v')
      assert.strictEqual(await actor.move(moved, 0), 1)
      // Any type, and a TTL carried over: still move_from / move_to only.
      await actor.hset(hash, 'f', 'v')
      await actor.expire(hash, 100)
      assert.strictEqual(await actor.move(hash, 0), 1)
      // The target already holds the key: nothing moves or is published.
      await actor.set(taken, 'v')
      assert.strictEqual(await actor.move(taken, 0), 0)
      await actor.del(taken)
    })
    assert.deepStrictEqual(events, [
      'taken:set',
      'moved@1:set',
      'moved@1:move_from',
      'moved:move_to',
      'hash@1:hset',
      'hash@1:expire',
      'hash@1:move_from',
      'hash:move_to',
      'taken@1:set',
      'taken@1:del',
    ])
  })

  test('COPY ... DB publishes copy_to on the target database (#445)', async () => {
    const src = randomKey()
    const copied = randomKey()
    const here = randomKey()
    const hereCopy = randomKey()
    const events = await capture(
      { src, copied, here, hereCopy },
      async actor => {
        await actor.select(1)
        await actor.set(src, 'v', 'EX', 100)
        assert.strictEqual(await actor.copy(src, copied, 'DB', 0), 1)
        // The destination exists: refused without REPLACE, and REPLACE
        // overwrites it with no del of its own.
        assert.strictEqual(await actor.copy(src, copied, 'DB', 0), 0)
        assert.strictEqual(await actor.copy(src, copied, 'DB', 0, 'REPLACE'), 1)
        await actor.del(src)
        // DB naming the selected database is the same as no DB at all.
        await actor.select(0)
        await actor.set(here, 'v')
        assert.strictEqual(await actor.copy(here, hereCopy, 'DB', 0), 1)
      },
    )
    assert.deepStrictEqual(events, [
      'src@1:set',
      'src@1:expire',
      'copied:copy_to',
      'copied:copy_to',
      'src@1:del',
      'here:set',
      'hereCopy:copy_to',
    ])
  })

  test('expire, move_from / move_to and copy_to are generic (g) events (#380, #445)', async () => {
    const run = (flags: string) => {
      const str = randomKey()
      const hash = randomKey()
      const copied = randomKey()
      return capture(
        { str, hash, copied },
        async actor => {
          await actor.set(str, 'v', 'EX', 100)
          await actor.select(1)
          await actor.hset(hash, 'f', 'v')
          assert.strictEqual(await actor.move(hash, 0), 1)
          await actor.select(0)
          assert.strictEqual(await actor.copy(hash, copied, 'DB', 1), 1)
          await actor.select(1)
          await actor.del(copied)
        },
        flags,
      )
    }
    // Neither the value's type ($, h) nor the command decides the class.
    assert.deepStrictEqual(await run('KE$h'), ['str:set', 'hash@1:hset'])
    assert.deepStrictEqual(await run('KEg$'), [
      'str:set',
      'str:expire',
      'hash@1:move_from',
      'hash:move_to',
      'copied@1:copy_to',
      'copied@1:del',
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
   * published for the named keys, in order, as `<name>:<event>` — or
   * `<name>@<db>:<event>` for a database other than 0. `steps` may SELECT;
   * the named keys are deleted from database 0 afterwards. `flags` must
   * include `E` and `$`, which the closing sentinel `set` is published under.
   */
  async function capture(
    keys: Record<string, string>,
    steps: (actor: Redis) => Promise<void>,
    flags = 'KEA',
  ): Promise<string[]> {
    const actor = await connect()
    const subscriber = await connect()
    await actor.config('SET', 'notify-keyspace-events', flags)
    await subscriber.psubscribe('__keyevent@*__:*')
    await settle()

    const names = new Map(
      Object.entries(keys).map(([name, key]) => [key, name]),
    )
    const events: string[] = []
    subscriber.on('pmessage', (_pattern, channel: string, key: string) => {
      const name = names.get(key)
      const match = /^__keyevent@(\d+)__:(.*)$/.exec(channel)
      if (!name || !match) return
      const [, db, event] = match
      events.push(db === '0' ? `${name}:${event}` : `${name}@${db}:${event}`)
    })

    await steps(actor)
    await actor.select(0)

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
    const client = base.duplicate({ lazyConnect: true })
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
