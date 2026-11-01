import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { ClientClosedError, MultiErrorReply, SimpleError } from 'redis'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
  type NodeRedisMockCluster,
} from '../../src/client-mocks/node-redis-mock'

// #490: arguments node-redis' encoder refuses (anything but a string or a Buffer).
// Every expectation below was observed through real node-redis 6.0.0 against
// Redis 8.0.0, identically at RESP2 and RESP3.

/** The encoder's TypeError for argument `index` of the command (its name is 0). */
function encodeError(index: number, type = 'number'): TypeError {
  return new TypeError(
    `"arguments[${index}]" must be of type "string | Buffer", got ${type} instead.`,
  )
}

/** Numbers, which node-redis' `mSet()` types do not accept, past the compiler. */
function unchecked<T>(value: unknown): T {
  return value as T
}

for (const RESP of [2, 3] as const) {
  describe(`node-redis mock: un-encodable arguments (RESP${RESP})`, () => {
    const openClients: NodeRedisMockClient[] = []

    afterEach(async () => {
      while (openClients.length > 0) {
        try {
          await openClients.pop()?.quit()
        } catch (err) {
          if (!(err instanceof ClientClosedError)) {
            throw err
          }
        }
      }
    })

    async function makeClient(): Promise<NodeRedisMockClient> {
      const client = (await createNodeRedisMock({
        RESP,
      })) as NodeRedisMockClient
      openClients.push(client)
      return client
    }

    test('mSet rejects a number value or key in every form, writing nothing', async () => {
      const c = await makeClient()
      const cases: [unknown, number][] = [
        [[['n1', 1]], 2],
        [
          [
            ['n1', 'x'],
            ['n2', 2],
          ],
          4,
        ],
        [['n1', 1], 2],
        [['n1', 'x', 'n2', 2], 4],
        [{ n1: 1 }, 2],
        [{ n1: 'x', n2: 2 }, 4],
        [[[5, 'v']], 1],
      ]
      for (const [toSet, index] of cases) {
        await assert.rejects(
          () => c.mSet(unchecked(toSet)),
          encodeError(index),
          JSON.stringify(toSet),
        )
      }
      await assert.rejects(
        () => c.mSet(unchecked(['n1'])),
        encodeError(2, 'undefined'),
      )
      assert.deepStrictEqual(
        await Promise.all([c.get('n1'), c.get('n2'), c.get('5')]),
        [null, null, null],
      )
      assert.strictEqual(await c.ping(), 'PONG')
    })

    test('exec() skips the un-encodable command, runs the rest and rejects with its TypeError', async () => {
      const c = await makeClient()
      await assert.rejects(
        () =>
          c
            .multi()
            .set('e:first', 'v')
            .mSet(unchecked([['e:m', 1]]))
            .set('e:last', 'v')
            .exec(),
        encodeError(2),
      )
      // Not left inside MULTI: the other commands and EXEC ran.
      assert.strictEqual(await c.ping(), 'PONG')
      assert.deepStrictEqual(
        await Promise.all([c.get('e:first'), c.get('e:m'), c.get('e:last')]),
        ['v', null, 'v'],
      )
      assert.deepStrictEqual(await c.multi().set('e:next', 'v').exec(), ['OK'])
    })

    test('the TypeError wins over runtime and queue-time errors, in either order', async () => {
      const c = await makeClient()
      await c.rPush('e:list', 'x')
      const bad = unchecked<string>(5)
      await assert.rejects(
        () => c.multi().get(bad).ping().exec(),
        encodeError(1),
      )
      await assert.rejects(
        () => c.multi().incr('e:list').get(bad).exec(),
        encodeError(1),
      )
      await assert.rejects(
        () => c.multi().get(bad).incr('e:list').exec(),
        encodeError(1),
      )
      await assert.rejects(
        () => c.multi().addCommand(['GET']).get(bad).exec(),
        encodeError(1),
      )
      await assert.rejects(
        () => c.multi().get(bad).addCommand(['GET']).exec(),
        encodeError(1),
      )
      assert.strictEqual(await c.ping(), 'PONG')
      assert.deepStrictEqual(await c.multi().ping().exec(), ['PONG'])
    })

    test('a WATCH is consumed by an exec() that rejects with a TypeError', async () => {
      const c = await makeClient()
      await c.watch('e:w')
      await c.set('e:w', 'changed')
      await assert.rejects(
        () => c.multi().get(unchecked<string>(5)).exec(),
        encodeError(1),
      )
      assert.deepStrictEqual(await c.multi().set('e:w', 'v').exec(), ['OK'])
      assert.strictEqual(await c.get('e:w'), 'v')
    })

    test('a cluster exec() does the same: the slot owner runs the rest', async () => {
      const cluster = (await createNodeRedisMock({
        RESP,
        cluster: { masters: 3 },
      })) as NodeRedisMockCluster
      try {
        // node-redis' cluster multi runs on the slot owner's own client, so it
        // goes through the same `_executeMulti` as a standalone client.
        await assert.rejects(
          () =>
            cluster
              .multi()
              .mSet(unchecked([['{e}m', 1]]))
              .set('{e}first', 'v')
              .exec(),
          encodeError(2),
        )
        assert.deepStrictEqual(
          await Promise.all([cluster.get('{e}first'), cluster.get('{e}m')]),
          ['v', null],
        )
        assert.deepStrictEqual(
          await cluster.multi().set('{e}next', 'v').exec(),
          ['OK'],
        )
      } finally {
        await cluster.quit()
      }
    })

    test('runtime errors still reject with MultiErrorReply', async () => {
      const c = await makeClient()
      await c.rPush('e:list', 'x')
      await assert.rejects(
        () => c.multi().incr('e:list').get('e:list').sAdd('e:list', 'z').exec(),
        (err: unknown) => {
          assert.ok(err instanceof MultiErrorReply)
          assert.deepStrictEqual(err.errorIndexes, [0, 1, 2])
          for (const reply of err.replies) {
            assert.ok(reply instanceof SimpleError)
          }
          return true
        },
      )
      await assert.rejects(
        () => c.multi().evalSha('0'.repeat(40)).exec(),
        (err: unknown) => {
          assert.ok(err instanceof MultiErrorReply)
          assert.deepStrictEqual(err.errorIndexes, [0])
          assert.ok(err.replies[0] instanceof SimpleError)
          assert.strictEqual(
            (err.replies[0] as SimpleError).message,
            'NOSCRIPT No matching script. Please use EVAL.',
          )
          return true
        },
      )
    })
  })
}
