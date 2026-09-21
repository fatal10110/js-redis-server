import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { ErrorReply, MultiErrorReply, WatchError } from 'redis'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
  type NodeRedisMockCluster,
  type NodeRedisReply,
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

  test('on / once / off drive the client event surface', async () => {
    const client = await makeClient()
    const seen: string[] = []
    // The client IS the EventEmitter (CommandRunner extends it), so these are
    // node-redis' own methods rather than hand-written wrappers.
    const removed = () => seen.push('removed')
    assert.strictEqual(
      client.on('end', () => seen.push('on')),
      client,
      'on() returns the client for chaining',
    )
    client.once('end', () => seen.push('once'))
    client.on('end', removed).off('end', removed)

    await client.quit()
    // A second 'end' — emitted directly rather than by quitting twice, which
    // real node-redis rejects with ClientClosedError. 'once' must not re-fire.
    client.emit('end')

    assert.deepStrictEqual(seen, ['on', 'once', 'on'])
  })

  test('rejects arguments node-redis would not encode', async () => {
    const client = await makeClient()
    // @redis/client's encoder takes string | Buffer and nothing else, so the
    // facade must reject the same values — including the arrays and TypedArrays
    // Buffer.from would happily convert.
    for (const [label, value] of [
      ['number', 5],
      ['array', [104, 105]],
      ['TypedArray', new Uint8Array([104, 105])],
      ['object', {}],
    ] as const) {
      await assert.rejects(
        () => client.sendCommand(['SET', 'n', value as unknown as string]),
        (err: unknown) => {
          assert.ok(err instanceof TypeError, `${label} should be a TypeError`)
          assert.strictEqual(
            err.message,
            `"arguments[2]" must be of type "string | Buffer", got ${typeof value} instead.`,
          )
          return true
        },
      )
    }
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

  test('cluster sendCommand routes by the command keys', async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster

    assert.strictEqual(
      await cluster.sendCommand(['SET', 'routed', 'yes']),
      'OK',
    )
    assert.strictEqual(await cluster.sendCommand(['GET', 'routed']), 'yes')
  })

  // ---------------------------------------------------------------- routing
  //
  // Only a *wrong-node* assertion can catch a bad router here. A cross-slot key
  // set cannot: whichever node the facade picks recomputes the real keys and
  // raises the same CROSSSLOT, so a `/CROSSSLOT/` assertion passes against a
  // first-argument heuristic too. The tests below therefore assert that a
  // command whose first argument is NOT a key still reaches the key's owner —
  // a mis-route surfaces as -MOVED from the node that does not own it.
  //
  // With 3 masters the slot space splits into [0,5460] / [5461,10921] /
  // [10922,16383]; `alpha` (865), `abc` (7638) and `k1` (12706) sit one per
  // range. Whatever slot a command's first argument hashes to, it can cover at
  // most one of the three — so a first-argument router mis-routes at least two
  // runs of every case below, whatever the literal happens to be.
  const KEYS_ONE_PER_MASTER = ['alpha', 'abc', 'k1']

  const NON_KEY_FIRST_ARGUMENT: {
    label: string
    args: (key: string) => string[]
    reply: NodeRedisReply
  }[] = [
    // numkeys-prefixed: the count, not a key.
    { label: 'ZDIFF', args: k => ['ZDIFF', '1', k], reply: [] },
    { label: 'LMPOP', args: k => ['LMPOP', '1', k, 'LEFT'], reply: null },
    { label: 'SINTERCARD', args: k => ['SINTERCARD', '1', k], reply: 0 },
    // subcommand-prefixed: the operation, not a key.
    { label: 'BITOP', args: k => ['BITOP', 'AND', k, k], reply: 0 },
  ]

  for (const { label, args, reply } of NON_KEY_FIRST_ARGUMENT) {
    test(`${label} routes by its keys, not its first argument`, async () => {
      cluster = (await createNodeRedisMock({
        cluster: { masters: 3 },
      })) as NodeRedisMockCluster

      for (const key of KEYS_ONE_PER_MASTER) {
        assert.deepStrictEqual(
          await cluster.sendCommand(args(key)),
          reply,
          `${label} on ${key} did not reach the node owning it`,
        )
      }
    })
  }

  test('EVAL routes by its declared KEYS, not by the script text', async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster

    const script = "return redis.call('SET', KEYS[1], ARGV[1])"
    for (const key of KEYS_ONE_PER_MASTER) {
      assert.strictEqual(
        await cluster.eval(script, {
          keys: [key],
          arguments: [`${key}-value`],
        }),
        'OK',
      )
      assert.strictEqual(await cluster.get(key), `${key}-value`)
    }
  })

  test('SORT BY/GET patterns keep the cluster-mode SORT error', async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster

    // SORT's routing keys deliberately include its BY/GET *patterns* (see
    // sortRoutingKeys in src/commands/keys.ts) so ClusterPolicy can run its own
    // check on them. They are not keys — `COMMAND GETKEYS SORT k BY w_* GET p_*`
    // on real Redis returns just `k` — so they usually look cross-slot. A
    // client-side CROSSSLOT refusal in the facade would fire first and swallow
    // the real error; routing must hand the command to a node instead.
    for (const [option, pattern] of [
      ['BY', 'w_*'],
      ['GET', 'p_*'],
    ]) {
      await assert.rejects(
        () => cluster!.sendCommand(['SORT', 'mylist', option, pattern]),
        (err: unknown) => {
          assert.ok(err instanceof ErrorReply)
          assert.strictEqual(
            err.message,
            `ERR ${option} option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.`,
          )
          return true
        },
      )
    }
  })

  // ------------------------------------------------------------- error shape
  //
  // These pin the client-visible CROSSSLOT wording. They are NOT guards for the
  // routing fix above — they pass against a first-argument router too, because
  // the node it picks rejects the span anyway.

  for (const [label, invoke] of [
    ['del', (c: NodeRedisMockCluster) => c.del('{x}:a', '{y}:b')],
    [
      'mSet',
      (c: NodeRedisMockCluster) =>
        c.mSet([
          ['{x}:a', '1'],
          ['{y}:b', '2'],
        ]),
    ],
    [
      'zUnionStore',
      (c: NodeRedisMockCluster) =>
        c.zUnionStore('{x}:dest', ['{x}:a', '{y}:b']),
    ],
  ] as const) {
    test(`${label} across slots is refused with CROSSSLOT`, async () => {
      cluster = (await createNodeRedisMock({
        cluster: { masters: 3 },
      })) as NodeRedisMockCluster

      await assert.rejects(
        () => invoke(cluster!),
        (err: unknown) => {
          assert.ok(err instanceof ErrorReply)
          assert.strictEqual(
            err.message,
            "CROSSSLOT Keys in request don't hash to the same slot",
          )
          return true
        },
      )
    })
  }

  test('an unplannable command still yields the pipeline error reply', async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster

    // Routing asks the executor to plan the command; an unknown name and a bad
    // arity both fail to plan. Neither may crash the facade — each must come
    // back as the normal server error reply.
    await assert.rejects(
      () => cluster!.sendCommand(['NOSUCHCOMMAND', 'k']),
      (err: unknown) => {
        assert.ok(err instanceof ErrorReply)
        assert.match(err.message, /^ERR unknown command/)
        return true
      },
    )
    await assert.rejects(
      () => cluster!.sendCommand(['GET']),
      (err: unknown) => {
        assert.ok(err instanceof ErrorReply)
        assert.match(err.message, /wrong number of arguments/)
        return true
      },
    )
  })
})
