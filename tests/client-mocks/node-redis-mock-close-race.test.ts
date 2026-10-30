import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { ClientClosedError, DisconnectsClientError } from 'redis'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
} from '../../src/client-mocks/node-redis-mock'

// #457: commands issued in the same tick as quit(), or after it. Every row was
// observed through real node-redis 6.0.0 against Redis 8.0.0, identically at
// RESP2 and RESP3: what was issued before quit() resolves, quit() resolves
// 'OK', and anything issued after it rejects ClientClosedError.

type Settled = { ok: unknown } | { err: unknown }

function settle(promise: Promise<unknown>): Promise<Settled> {
  return promise.then(
    ok => ({ ok }),
    (err: unknown) => ({ err }),
  )
}

function assertClosed(result: Settled): void {
  assert.ok(
    'err' in result,
    `expected a rejection, got ${JSON.stringify(result)}`,
  )
  assert.ok(result.err instanceof ClientClosedError, String(result.err))
}

for (const RESP of [2, 3] as const) {
  describe(`node-redis mock: commands racing quit() (RESP${RESP})`, () => {
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
      await client.set('race:k', 'v')
      return client
    }

    test('get; quit; get in one tick: the later get rejects', async () => {
      const client = await makeClient()
      const [before, quit, after] = await Promise.all([
        settle(client.get('race:k')),
        settle(client.quit()),
        settle(client.get('race:k')),
      ])
      assert.deepStrictEqual(before, { ok: 'v' })
      assert.deepStrictEqual(quit, { ok: 'OK' })
      assertClosed(after)
    })

    test('everything issued before quit() drains, everything after rejects', async () => {
      const client = await makeClient()
      const results = await Promise.all([
        settle(client.get('race:k')),
        settle(client.get('race:k')),
        settle(client.get('race:k')),
        settle(client.quit()),
        settle(client.get('race:k')),
        settle(client.get('race:k')),
        settle(client.set('race:k', 'w')),
      ])
      for (const result of results.slice(0, 3)) {
        assert.deepStrictEqual(result, { ok: 'v' })
      }
      assert.deepStrictEqual(results[3], { ok: 'OK' })
      results.slice(4).forEach(assertClosed)
    })

    test('a command issued a microtask or a macrotask after quit() rejects', async () => {
      for (const later of [
        () => Promise.resolve(),
        () => new Promise(resolve => setTimeout(resolve, 0)),
      ]) {
        const client = await makeClient()
        const quit = settle(client.quit())
        await later()
        assertClosed(await settle(client.get('race:k')))
        assert.deepStrictEqual(await quit, { ok: 'OK' })
      }
    })

    test('a transaction issued after quit() rejects; one issued before drains', async () => {
      const client = await makeClient()
      const built = client.multi().set('race:m', '1')
      const [before, quit, after, builtExec] = await Promise.all([
        settle(client.multi().set('race:t', '1').get('race:t').exec()),
        settle(client.quit()),
        settle(client.multi().set('race:u', '1').exec()),
        settle(built.exec()),
      ])
      assert.deepStrictEqual(before, { ok: ['OK', '1'] })
      assert.deepStrictEqual(quit, { ok: 'OK' })
      assertClosed(after)
      assertClosed(builtExec)
    })

    test('quit(); quit() in one tick: the second rejects', async () => {
      const client = await makeClient()
      const [first, second] = await Promise.all([
        settle(client.quit()),
        settle(client.quit()),
      ])
      assert.deepStrictEqual(first, { ok: 'OK' })
      assertClosed(second)
    })

    test('publish, watch and sendCommand issued after quit() reject', async () => {
      const client = await makeClient()
      const [quit, ...rest] = await Promise.all([
        settle(client.quit()),
        settle(client.publish('race:ch', 'x')),
        settle(client.watch('race:k')),
        settle(client.sendCommand(['GET', 'race:k'])),
      ])
      assert.deepStrictEqual(quit, { ok: 'OK' })
      rest.forEach(assertClosed)
    })

    test('get; destroy(); get: flushed, then closed', async () => {
      const client = await makeClient()
      const before = settle(client.get('race:k'))
      client.destroy()
      const after = settle(client.get('race:k'))
      const flushed = await before
      assert.ok(
        'err' in flushed && flushed.err instanceof DisconnectsClientError,
      )
      assertClosed(await after)
      assertClosed(await settle(client.quit()))
      assert.throws(() => client.destroy(), ClientClosedError)
    })
  })
}
