import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { ErrorReply, MultiErrorReply, WatchError } from 'redis'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
  type NodeRedisMockCluster,
} from '../../src/client-mocks/node-redis-mock'

// These are unit tests because the facade IS the client surface — there is no
// separate client library transformation to exercise (unlike the ioredis path).

describe('createNodeRedisMock (standalone)', () => {
  const openClients: NodeRedisMockClient[] = []

  afterEach(async () => {
    while (openClients.length > 0) {
      const client = openClients.pop()
      await client?.quit()
    }
  })

  async function makeClient(): Promise<NodeRedisMockClient> {
    const client = (await createNodeRedisMock()) as NodeRedisMockClient
    openClients.push(client)
    return client
  }

  test('string round-trip via curated methods', async () => {
    const client = await makeClient()
    assert.strictEqual(await client.set('k', 'v'), 'OK')
    assert.strictEqual(await client.get('k'), 'v')
    assert.strictEqual(await client.get('missing'), null)
    assert.strictEqual(await client.exists('k'), 1)
    assert.strictEqual(await client.del('k'), 1)
    assert.strictEqual(await client.exists('k'), 0)
  })

  test('incr returns a number', async () => {
    const client = await makeClient()
    assert.strictEqual(await client.incr('counter'), 1)
    assert.strictEqual(await client.incr('counter'), 2)
  })

  test('expire / ttl', async () => {
    const client = await makeClient()
    await client.set('e', 'v')
    // node-redis returns the raw integer (1), not a boolean.
    assert.strictEqual(await client.expire('e', 1000), 1)
    const ttl = await client.ttl('e')
    assert.ok(ttl > 0 && ttl <= 1000)
  })

  test('hash methods return node-redis shapes', async () => {
    const client = await makeClient()
    assert.strictEqual(await client.hSet('h', 'f1', 'v1'), 1)
    assert.strictEqual(await client.hGet('h', 'f1'), 'v1')
    assert.deepStrictEqual(await client.hGetAll('h'), { f1: 'v1' })
  })

  test('list methods', async () => {
    const client = await makeClient()
    assert.strictEqual(await client.rPush('l', 'a'), 1)
    assert.strictEqual(await client.lPush('l', 'z'), 2)
    assert.deepStrictEqual(await client.lRange('l', 0, -1), ['z', 'a'])
  })

  test('set methods', async () => {
    const client = await makeClient()
    assert.strictEqual(await client.sAdd('s', 'a'), 1)
    await client.sAdd('s', 'b')
    assert.deepStrictEqual(
      new Set(await client.sMembers('s')),
      new Set(['a', 'b']),
    )
  })

  test('zset methods', async () => {
    const client = await makeClient()
    assert.strictEqual(await client.zAdd('z', { score: 1, value: 'a' }), 1)
    await client.zAdd('z', [
      { score: 2, value: 'b' },
      { score: 3, value: 'c' },
    ])
    assert.deepStrictEqual(await client.zRange('z', 0, -1), ['a', 'b', 'c'])
  })

  test('sendCommand fallback decodes generic replies', async () => {
    const client = await makeClient()
    assert.strictEqual(await client.sendCommand(['SET', 'g', '1']), 'OK')
    assert.strictEqual(await client.sendCommand(['GET', 'g']), '1')
    // Unknown-to-the-facade command still works via the generic escape hatch.
    assert.strictEqual(await client.sendCommand(['STRLEN', 'g']), 1)
  })

  test('WRONGTYPE error surfaces with Redis wording', async () => {
    const client = await makeClient()
    await client.set('str', 'v')
    await assert.rejects(
      () => client.lPush('str', 'x'),
      (err: unknown) => {
        assert.ok(err instanceof Error)
        assert.match(err.message, /WRONGTYPE/)
        return true
      },
    )
  })

  test('multi / exec runs queued commands and returns replies', async () => {
    const client = await makeClient()
    const results = await client.multi().set('m', 'myValue').get('m').exec()
    assert.deepStrictEqual(results, ['OK', 'myValue'])
  })

  test('multi / discard cancels the queue', async () => {
    const client = await makeClient()
    const multi = client.multi()
    multi.set('d', 'v')
    await multi.discard()
    assert.strictEqual(await client.get('d'), null)
  })

  test('watch makes exec abort on conflicting write', async () => {
    const client = await makeClient()
    // A second connection over the SAME keyspace (duplicate shares state).
    const other = await client.duplicate()
    openClients.push(other)
    await client.set('w', 'orig')
    await client.watch('w')
    // A concurrent write on the watched key from another connection.
    await other.set('w', 'changed')
    const multi = client.multi()
    multi.set('w', 'fromTxn')
    // node-redis throws WatchError on a watch-aborted EXEC (never returns null).
    await assert.rejects(
      () => multi.exec(),
      (err: unknown) => err instanceof WatchError,
    )
    assert.strictEqual(await client.get('w'), 'changed')
  })

  test('exec aggregates per-command errors into MultiErrorReply', async () => {
    const client = await makeClient()
    await client.set('s', 'notAnInteger')
    const multi = client.multi()
    multi.set('ok', 'v') // succeeds
    multi.incr('s') // errors: value is not an integer
    await assert.rejects(
      () => multi.exec(),
      (err: unknown) => {
        assert.ok(err instanceof MultiErrorReply)
        assert.deepStrictEqual(err.errorIndexes, [1])
        assert.strictEqual(err.replies[0], 'OK')
        assert.ok(err.replies[1] instanceof ErrorReply)
        return true
      },
    )
  })

  test('pub/sub delivers messages to the subscribe callback', async () => {
    const publisher = await makeClient()
    // Subscriber shares the publisher's broker via duplicate().
    const subscriber = await publisher.duplicate()
    openClients.push(subscriber)
    const channel = 'news'

    let resolveMessage: (value: { message: string; channel: string }) => void
    const received = new Promise<{ message: string; channel: string }>(
      resolve => {
        resolveMessage = resolve
      },
    )
    await subscriber.subscribe(channel, (message: string, ch: string) => {
      resolveMessage({ message, channel: ch })
    })

    assert.strictEqual(await publisher.publish(channel, 'hello'), 1)

    const got = await received
    assert.deepStrictEqual(got, { message: 'hello', channel })
  })

  test('pSubscribe delivers pattern messages', async () => {
    const publisher = await makeClient()
    const subscriber = await publisher.duplicate()
    openClients.push(subscriber)

    let resolveMessage: (value: { message: string; channel: string }) => void
    const received = new Promise<{ message: string; channel: string }>(
      resolve => {
        resolveMessage = resolve
      },
    )
    await subscriber.pSubscribe('news.*', (message: string, ch: string) => {
      resolveMessage({ message, channel: ch })
    })

    await publisher.publish('news.sports', 'goal')

    const got = await received
    assert.deepStrictEqual(got, { message: 'goal', channel: 'news.sports' })
  })

  test('duplicate() yields an independent client sharing state', async () => {
    const client = await makeClient()
    await client.set('shared', 'v')
    const dup = await client.duplicate()
    openClients.push(dup)
    assert.strictEqual(await dup.get('shared'), 'v')
  })

  test('quit() tears down the session (no further commands)', async () => {
    const client = (await createNodeRedisMock()) as NodeRedisMockClient
    await client.set('x', '1')
    await client.quit()
    await assert.rejects(() => client.get('x'))
  })
})

describe('createNodeRedisMock (cluster)', () => {
  let cluster: NodeRedisMockCluster | undefined

  afterEach(async () => {
    await cluster?.quit()
    cluster = undefined
  })

  test('routes keyed commands to the owning node by slot', async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster

    // Different keys land on different slots/nodes but all work transparently.
    await cluster.set('alpha', '1')
    await cluster.set('beta', '2')
    await cluster.set('gamma', '3')

    assert.strictEqual(await cluster.get('alpha'), '1')
    assert.strictEqual(await cluster.get('beta'), '2')
    assert.strictEqual(await cluster.get('gamma'), '3')
  })

  test('hash-tagged keys co-locate on the same node', async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster

    await cluster.set('{user1}:name', 'alice')
    await cluster.set('{user1}:age', '30')
    assert.strictEqual(await cluster.get('{user1}:name'), 'alice')
    assert.strictEqual(await cluster.get('{user1}:age'), '30')
  })

  test('cluster sendCommand routes by first key', async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster

    assert.strictEqual(
      await cluster.sendCommand(['SET', 'routed', 'yes']),
      'OK',
    )
    assert.strictEqual(await cluster.sendCommand(['GET', 'routed']), 'yes')
  })

  test('a multi-key command across slots is refused with CROSSSLOT', async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster

    // {x} and {y} hash to different slots → DEL spans two slots. Must throw
    // rather than silently running against the first key's node.
    await assert.rejects(
      () => cluster!.del('{x}:a', '{y}:b'),
      (err: unknown) => {
        assert.ok(err instanceof Error)
        assert.match(err.message, /CROSSSLOT/)
        return true
      },
    )
  })
})
