import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createClient, createCluster } from 'redis'
import { createRedisMock, type RedisMock } from '../../src/mock'
import type { SeedEntry } from '../../src/seed'

const SEED: SeedEntry[] = [
  { key: 'user:1', type: 'string', value: 'alice' },
  { key: 'counter', type: 'string', value: 42 },
  { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
  { key: 'l:1', type: 'list', value: ['a', 'b', 'c'] },
  { key: 's:1', type: 'set', value: ['x', 'y'] },
  { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
  { key: 'ttl:1', type: 'string', value: 'soon', ttlMs: 50_000 },
]

type NodeReader = {
  get(key: string): Promise<string | null>
  hGetAll(key: string): Promise<Record<string, string>>
  lRange(key: string, start: number, stop: number): Promise<string[]>
  sMembers(key: string): Promise<string[]>
  zRange(key: string, start: number, stop: number): Promise<string[]>
  zScore(key: string, member: string): Promise<number | string | null>
  pTTL(key: string): Promise<number>
}

async function assertSeedReadable(client: NodeReader): Promise<void> {
  assert.strictEqual(await client.get('user:1'), 'alice')
  assert.strictEqual(await client.get('counter'), '42')
  assert.deepStrictEqual(await client.hGetAll('h:1'), {
    name: 'bob',
    age: '30',
  })
  assert.deepStrictEqual(await client.lRange('l:1', 0, -1), ['a', 'b', 'c'])
  assert.deepStrictEqual((await client.sMembers('s:1')).sort(), ['x', 'y'])
  assert.deepStrictEqual(await client.zRange('z:1', 0, -1), ['a', 'b'])
  assert.strictEqual(Number(await client.zScore('z:1', 'a')), 1)
  assert.strictEqual(Number(await client.zScore('z:1', 'b')), 2)
  const pttl = await client.pTTL('ttl:1')
  assert.ok(pttl > 0 && pttl <= 50_000, `expected live TTL, got ${pttl}`)
}

describe('createRedisMock standalone seed → node-redis read-back', () => {
  let mock: RedisMock
  let client: ReturnType<typeof createClient>

  before(async () => {
    mock = await createRedisMock()
    await mock.seed(SEED)
    client = createClient({ url: mock.url })
    await client.connect()
  })

  after(async () => {
    await client?.close()
    await mock?.close()
  })

  test('reads every seeded type back through node-redis', async () => {
    await assertSeedReadable(client as unknown as NodeReader)
  })
})

describe('createRedisMock cluster seed → node-redis read-back', () => {
  let mock: RedisMock
  let client: ReturnType<typeof createCluster>

  before(async () => {
    mock = await createRedisMock({ cluster: { masters: 3 } })
    await mock.seed(SEED)
    client = createCluster({
      rootNodes: mock.clusterNodes().map(node => ({
        url: `redis://${node.host}:${node.port}`,
      })),
    })
    await client.connect()
  })

  after(async () => {
    await client?.close()
    await mock?.close()
  })

  test('reads every seeded type back through a node-redis cluster client', async () => {
    await assertSeedReadable(client as unknown as NodeReader)
  })
})
