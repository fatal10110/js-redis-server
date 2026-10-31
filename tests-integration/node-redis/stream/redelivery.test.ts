import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { connectToNodeRedisSlotOwner, randomKey } from '../../utils'

const testRunner = new TestRunner()

const SLEEP_MS = 300
/** Generous bound for "reset to about 0" on a loaded CI box. */
const FRESH_MS = 200

// XINFO replies are RESP3 maps (objects) on node-redis; read them raw.
type StreamInfo = Record<string, unknown>

// XREADGROUP history reads bump each re-delivered entry's delivery count and
// reset its idle time; a deleted entry's NACK is left alone (#624).
describe(`XREADGROUP history re-delivery (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('XPENDING and XINFO STREAM FULL see the bumped NACKs', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: '1' })
      await node.xAdd(key, '2-1', { f: '2' })
      await node.xGroupCreate(key, 'workers', '0')
      await node.xReadGroup('workers', 'alice', { key, id: '>' })

      // Two history reads of live entries.
      for (let i = 0; i < 2; i++) {
        const history = await node.xReadGroup('workers', 'alice', {
          key,
          id: '0',
        })
        assert.deepStrictEqual(
          history?.[0].messages.map(m => m.id),
          ['1-1', '2-1'],
        )
      }
      assert.strictEqual(await node.xDel(key, '2-1'), 1)
      await sleep(SLEEP_MS)

      // A deleted PEL entry comes back nil, which the typed xReadGroup
      // transform rejects: read raw.
      await node.sendCommand([
        'XREADGROUP',
        'GROUP',
        'workers',
        'alice',
        'STREAMS',
        key,
        '0',
      ])

      const pending = await node.xPendingRange(key, 'workers', '-', '+', 10)
      assert.deepStrictEqual(
        pending.map(p => [p.id, p.consumer, p.deliveriesCounter]),
        [
          ['1-1', 'alice', 4],
          ['2-1', 'alice', 3],
        ],
      )
      const [live, deleted] = pending
      assert.ok(
        live.millisecondsSinceLastDelivery < FRESH_MS,
        `1-1 idle ${live.millisecondsSinceLastDelivery}`,
      )
      assert.ok(
        deleted.millisecondsSinceLastDelivery >= SLEEP_MS,
        `2-1 idle ${deleted.millisecondsSinceLastDelivery}`,
      )

      const info = (await node.sendCommand([
        'XINFO',
        'STREAM',
        key,
        'FULL',
      ])) as StreamInfo
      const groups = info['groups'] as StreamInfo[]
      const groupPel = groups[0]['pending'] as unknown[][]
      assert.deepStrictEqual(
        groupPel.map(row => [row[0], row[1], row[3]]),
        [
          ['1-1', 'alice', 4],
          ['2-1', 'alice', 3],
        ],
      )
      const consumers = groups[0]['consumers'] as StreamInfo[]
      const consumerPel = consumers[0]['pending'] as unknown[][]
      assert.deepStrictEqual(
        consumerPel.map(row => [row[0], row[2]]),
        [
          ['1-1', 4],
          ['2-1', 3],
        ],
      )
    } finally {
      node.destroy()
    }
  })
})
