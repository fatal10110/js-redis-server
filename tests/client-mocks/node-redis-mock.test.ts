import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import {
  ClientClosedError,
  DisconnectsClientError,
  ErrorReply,
  MultiErrorReply,
  WatchError,
} from 'redis'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
  type NodeRedisMockCluster,
  type NodeRedisReply,
} from '../../src/client-mocks/node-redis-mock'

// These are unit tests because the facade IS the client surface — there is no
// separate client library transformation to exercise (unlike the ioredis path).

/**
 * Close a client the way teardown code has to against the *real* node-redis:
 * closing an already-closed single client throws `ClientClosedError`, so a
 * defensive `afterEach` quit must tolerate it. (A cluster closes idempotently
 * and never throws, so this is a no-op safety net there.)
 */
async function quitIfOpen(
  client: NodeRedisMockClient | NodeRedisMockCluster | undefined,
): Promise<void> {
  try {
    await client?.quit()
  } catch (err) {
    if (!(err instanceof ClientClosedError)) {
      throw err
    }
  }
}

describe('createNodeRedisMock (standalone)', () => {
  const openClients: NodeRedisMockClient[] = []

  afterEach(async () => {
    while (openClients.length > 0) {
      await quitIfOpen(openClients.pop())
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
    // Real node-redis emits 'end' exactly once per close, so a second 'end' can
    // only be provoked by emitting it directly — quitting twice throws instead
    // (see the close-path tests below). 'once' must not re-fire.
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
    await assert.rejects(() => client.get('x'), ClientClosedError)
  })

  // Close-path behaviour below is pinned to what real node-redis v6 does when
  // driven against a real redis-server: every close method on an already-closed
  // client throws `ClientClosedError('The client is closed')` — quit() and
  // disconnect() as a rejection, destroy() synchronously — 'end' is emitted
  // exactly once per close, and a clean close emits no 'error'.

  test("double quit() throws ClientClosedError and 'end' fires once", async () => {
    const client = await makeClient()
    let ends = 0
    client.on('end', () => ends++)

    assert.strictEqual(await client.quit(), 'OK')
    await assert.rejects(() => client.quit(), ClientClosedError)
    await assert.rejects(() => client.quit(), ClientClosedError)

    assert.strictEqual(ends, 1, "'end' must fire exactly once")
  })

  test('every close method throws ClientClosedError once closed', async () => {
    // quit() -> disconnect()
    const afterQuit = await makeClient()
    await afterQuit.quit()
    await assert.rejects(() => afterQuit.disconnect(), ClientClosedError)
    assert.throws(() => afterQuit.destroy(), ClientClosedError)

    // disconnect() -> quit(); disconnect() itself resolves undefined
    const afterDisconnect = await makeClient()
    assert.strictEqual(await afterDisconnect.disconnect(), undefined)
    await assert.rejects(() => afterDisconnect.quit(), ClientClosedError)
    await assert.rejects(() => afterDisconnect.disconnect(), ClientClosedError)

    // destroy() -> quit(); destroy() is synchronous and returns undefined
    const afterDestroy = await makeClient()
    assert.strictEqual(afterDestroy.destroy(), undefined)
    await assert.rejects(() => afterDestroy.quit(), ClientClosedError)
    assert.throws(() => afterDestroy.destroy(), ClientClosedError)
  })

  test("disconnect()/destroy() emit 'end' exactly once and no 'error'", async () => {
    for (const close of [
      (c: NodeRedisMockClient) => c.disconnect(),
      (c: NodeRedisMockClient) => c.destroy(),
    ]) {
      const client = await makeClient()
      let ends = 0
      const errors: unknown[] = []
      client.on('end', () => ends++)
      client.on('error', err => errors.push(err))

      await close(client)
      // A second close throws; it must not emit another 'end'.
      await assert.rejects(async () => close(client), ClientClosedError)
      await new Promise(resolve => setImmediate(resolve))

      assert.strictEqual(ends, 1, "'end' must fire exactly once")
      assert.deepStrictEqual(errors, [], 'a clean close emits no error')
    }
  })

  test('closing a client with a live subscription still emits one end', async () => {
    const client = await makeClient()
    let ends = 0
    const errors: unknown[] = []
    client.on('end', () => ends++)
    client.on('error', err => errors.push(err))
    await client.subscribe('news', () => {})

    await client.quit()
    await assert.rejects(() => client.quit(), ClientClosedError)
    await new Promise(resolve => setImmediate(resolve))

    assert.strictEqual(ends, 1)
    assert.deepStrictEqual(errors, [])
  })

  test('quit() drains commands already issued', async () => {
    const client = await makeClient()
    // Real node-redis' quit() appends QUIT to the command queue, so every
    // command issued before it still runs — 2000 pending commands all resolve.
    const pending = Array.from({ length: 500 }, (_, i) =>
      client.set(`drain:${i}`, String(i)),
    )
    const quit = client.quit()

    const settled = await Promise.allSettled(pending)
    assert.strictEqual(await quit, 'OK')
    assert.deepStrictEqual(
      settled.filter(entry => entry.status === 'rejected'),
      [],
      'commands issued before quit() must not be retroactively killed',
    )
  })

  test('disconnect()/destroy() flush in-flight with DisconnectsClientError', async () => {
    // Unlike quit(), these do not drain: real node-redis' destroy() does
    // `#queue.flushAll(new DisconnectsClientError())`, and disconnect() is an
    // alias for destroy().
    for (const close of [
      (c: NodeRedisMockClient) => c.disconnect(),
      (c: NodeRedisMockClient) => c.destroy(),
    ]) {
      const client = await makeClient()
      const pending = Array.from({ length: 50 }, (_, i) =>
        client.set(`flush:${i}`, String(i)),
      )
      await close(client)

      const settled = await Promise.allSettled(pending)
      assert.strictEqual(
        settled.filter(entry => entry.status === 'fulfilled').length,
        0,
        'a hard close must not let queued commands through',
      )
      for (const entry of settled) {
        assert.ok(
          entry.status === 'rejected' &&
            entry.reason instanceof DisconnectsClientError,
          'in-flight commands reject with DisconnectsClientError',
        )
      }
    }
  })

  test('connect() on a closed client fails loudly (known gap)', async () => {
    // KNOWN GAP (#440), pinned so it cannot regress silently: real node-redis re-opens
    // a closed client — it reconnects, serves commands and emits a second
    // 'end'. This facade cannot, because the owning client closes its
    // RedisServerState and that is terminal. Until it can, connect() refuses
    // rather than handing back a dead client that looks alive.
    const client = await makeClient()
    await client.quit()
    await assert.rejects(() => client.connect(), ClientClosedError)
  })
})

describe('createNodeRedisMock (cluster)', () => {
  let cluster: NodeRedisMockCluster | undefined

  afterEach(async () => {
    await quitIfOpen(cluster)
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

  // A real RedisCluster does NOT close like a single client, so these pin the
  // cluster's own contract — ground-truthed against node-redis v6 driving a
  // live 3-node cluster, and matching `cluster/cluster-slots.js`.
  test("the close path emits 'disconnect', never 'end', and never throws", async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster
    const events: string[] = []
    cluster.on('end', () => events.push('end'))
    cluster.on('disconnect', () => events.push('disconnect'))

    // Every close resolves `undefined` — the cluster's quit() returns its
    // internal #destroy() promise, not the standalone client's 'OK' …
    assert.strictEqual(await cluster.quit(), undefined)
    // … and a redundant close is a no-op that resolves, never a throw:
    // #destroy() reset the slot/node maps, so the second call finds nothing to
    // close and awaits Promise.allSettled([]).
    assert.strictEqual(await cluster.quit(), undefined)
    assert.strictEqual(await cluster.disconnect(), undefined)
    assert.strictEqual(cluster.destroy(), undefined)

    // 'disconnect' fires once per close CALL (not once per open→closed
    // transition), and 'end' never fires at all.
    assert.deepStrictEqual(events, [
      'disconnect',
      'disconnect',
      'disconnect',
      'disconnect',
    ])
  })

  test('a command on a closed cluster throws ClientClosedError', async () => {
    cluster = (await createNodeRedisMock({
      cluster: { masters: 3 },
    })) as NodeRedisMockCluster
    await cluster.quit()
    // DELIBERATE DEVIATION: real node-redis v6 throws an internal
    // `TypeError: Cannot read properties of undefined (reading 'replicas')`
    // here — it dereferences the slot map its own close path just reset. That
    // is an upstream crash rather than a contract, so the facade throws the
    // error that actually describes the situation.
    await assert.rejects(() => cluster!.get('alpha'), ClientClosedError)
  })
})
