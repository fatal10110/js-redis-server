import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { createClient } from 'redis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

/**
 * node-redis client-side caching (#252): `clientSideCache` turns on
 * `CLIENT TRACKING ON` over RESP3 and drops an entry when the server sends
 * its `invalidate` push, or everything on a flush's null.
 */
const testRunner = new TestRunner()
const RUN = randomKey()
// A database no other suite selects: the FLUSHDB test empties it.
const DATABASE = 13

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

async function connect(port: number, cached: boolean) {
  const client = createClient({
    url: `redis://127.0.0.1:${port}`,
    database: DATABASE,
    ...(cached ? { RESP: 3 as const, clientSideCache: {} } : {}),
  })
  client.on('error', () => {})
  await client.connect()
  return client
}

type Client = Awaited<ReturnType<typeof connect>>

describe(`client-side caching (node-redis, ${testRunner.getBackendName()})`, () => {
  let cached: Client
  let writer: Client

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    ;[cached, writer] = await Promise.all([
      connect(port, true),
      connect(port, false),
    ])
  })

  after(async () => {
    await Promise.allSettled([cached?.close(), writer?.close()])
    await testRunner.cleanup()
  })

  /** Wait until the cache holds `size` entries. */
  async function cacheSize(size: number): Promise<number> {
    const cache = cached.clientSideCache!
    for (let attempt = 0; attempt < 100 && cache.size() !== size; attempt++) {
      await delay(10)
    }
    return cache.size()
  }

  test('a cached GET is served locally until another client writes the key', async () => {
    const key = `${RUN}:k`
    await writer.set(key, 'a')
    assert.strictEqual(await cached.get(key), 'a')
    const hits = cached.clientSideCache!.stats().hitCount
    assert.strictEqual(await cached.get(key), 'a')
    assert.strictEqual(cached.clientSideCache!.stats().hitCount, hits + 1)
    assert.strictEqual(await cacheSize(1), 1)

    await writer.set(key, 'b')
    assert.strictEqual(await cacheSize(0), 0)
    assert.strictEqual(await cached.get(key), 'b')
  })

  test('FLUSHDB empties the cache', async () => {
    const [a, b] = [`${RUN}:a`, `${RUN}:b`]
    await writer.mSet([
      [a, '1'],
      [b, '2'],
    ])
    assert.strictEqual(await cached.get(a), '1')
    assert.strictEqual(await cached.get(b), '2')
    assert.ok((await cacheSize(3)) >= 2)

    await writer.flushDb()
    assert.strictEqual(await cacheSize(0), 0)
    assert.strictEqual(await cached.get(a), null)
  })
})
