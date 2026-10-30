import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import {
  ClientClosedError,
  ErrorReply,
  MultiErrorReply,
  SimpleError,
} from 'redis'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
} from '../../src/client-mocks/node-redis-mock'

// #510. Every expectation below was observed through real node-redis 6.0.0
// against Redis 8.0.0, identically at RESP2 and RESP3.

const NESTED = "return {1, redis.error_reply('x')}"

function assertSimpleError(value: unknown, message: string): void {
  assert.ok(
    value instanceof SimpleError,
    `expected SimpleError, got ${String(value)}`,
  )
  assert.ok(value instanceof ErrorReply)
  assert.strictEqual(value.message, message)
}

for (const RESP of [2, 3] as const) {
  describe(`node-redis mock reply errors (RESP${RESP})`, () => {
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

    test('an error nested in an array reply is returned in place', async () => {
      const client = await makeClient()
      const reply = await client.sendCommand(['EVAL', NESTED, '0'])
      assert.ok(Array.isArray(reply))
      assert.strictEqual(reply[0], 1)
      assertSimpleError(reply[1], 'ERR x')
    })

    test('errors nested at any depth are returned in place', async () => {
      const client = await makeClient()
      const reply = await client.sendCommand([
        'EVAL',
        "return {1, redis.error_reply('ERR boom'), {2, redis.error_reply('MYERR deep')}}",
        '0',
      ])
      assert.ok(Array.isArray(reply))
      assert.strictEqual(reply[0], 1)
      assertSimpleError(reply[1], 'ERR boom')
      const inner = reply[2]
      assert.ok(Array.isArray(inner))
      assert.strictEqual(inner[0], 2)
      assertSimpleError(inner[1], 'MYERR deep')
    })

    test('a redis.pcall error inside a table is returned in place', async () => {
      const client = await makeClient()
      const reply = await client.sendCommand([
        'EVAL',
        "return {redis.pcall('INCR', 'k', 'y'), 5}",
        '0',
      ])
      assert.ok(Array.isArray(reply))
      assertSimpleError(
        reply[0],
        'ERR Wrong number of args calling Redis command from script',
      )
      assert.strictEqual(reply[1], 5)
    })

    test('an error value inside a map reply is returned in place', async () => {
      const client = await makeClient()
      const reply = await client.sendCommand([
        'EVAL',
        "redis.setresp(3); return {map={a=redis.error_reply('x'), b=1}}",
        '0',
      ])
      if (RESP === 3) {
        assert.ok(reply && typeof reply === 'object' && !Array.isArray(reply))
        const map = reply as { [key: string]: unknown }
        assertSimpleError(map.a, 'ERR x')
        assert.strictEqual(map.b, 1)
      } else {
        // RESP2 flattens the map; the pair order is the Lua table's.
        assert.ok(Array.isArray(reply))
        assert.strictEqual(reply.length, 4)
        const a = reply.indexOf('a')
        const b = reply.indexOf('b')
        assertSimpleError(reply[a + 1], 'ERR x')
        assert.strictEqual(reply[b + 1], 1)
      }
    })

    test('a top-level error reply still rejects', async () => {
      const client = await makeClient()
      await assert.rejects(
        client.sendCommand(['EVAL', "return redis.error_reply('x')", '0']),
        (err: unknown) => {
          assertSimpleError(err, 'ERR x')
          return true
        },
      )
    })

    test('exec() returns a nested error inside its reply, not MultiErrorReply', async () => {
      const client = await makeClient()
      const replies = await client
        .multi()
        .addCommand(['EVAL', NESTED, '0'])
        .exec()
      assert.strictEqual(replies.length, 1)
      const [first] = replies
      assert.ok(Array.isArray(first))
      assert.strictEqual(first[0], 1)
      assertSimpleError(first[1], 'ERR x')
    })

    test('exec() rejects with the first command that failed to queue, not EXECABORT', async () => {
      const client = await makeClient()
      await assert.rejects(
        client.multi().set('qe:a', '1').addCommand(['EVALSHA', 'abc']).exec(),
        (err: unknown) => {
          assertSimpleError(
            err,
            "ERR wrong number of arguments for 'evalsha' command",
          )
          assert.ok(!(err instanceof MultiErrorReply))
          return true
        },
      )
      // The transaction was discarded, and the client left MULTI.
      assert.strictEqual(await client.get('qe:a'), null)
      assert.deepStrictEqual(
        await client.multi().set('qe:b', '1').get('qe:b').exec(),
        ['OK', '1'],
      )
    })

    test('only the first queue-time error is reported', async () => {
      const client = await makeClient()
      await assert.rejects(
        client
          .multi()
          .addCommand(['NOSUCHCMD'])
          .addCommand(['GET'])
          .addCommand(['GET', 'qe:x'])
          .exec(),
        (err: unknown) => {
          assertSimpleError(
            err,
            "ERR unknown command 'NOSUCHCMD', with args beginning with: ",
          )
          return true
        },
      )
    })

    test('a queue-time error wins over a runtime error in the same transaction', async () => {
      const client = await makeClient()
      await client.lPush('qe:list', 'v')
      await assert.rejects(
        client.multi().get('qe:list').addCommand(['GET']).exec(),
        (err: unknown) => {
          assertSimpleError(
            err,
            "ERR wrong number of arguments for 'get' command",
          )
          return true
        },
      )
    })
  })
}
