import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, randomKey } from '../../utils'

const testRunner = new TestRunner()

const SLEEP_MS = 300
/** Generous bound for "reset to about 0" on a loaded CI box. */
const FRESH_MS = 200

function kvArrayGet(items: unknown[], key: string): unknown {
  const index = items.indexOf(key)
  assert.notStrictEqual(index, -1, `expected ${key} field`)
  return items[index + 1]
}

// XREADGROUP history reads bump each re-delivered entry's delivery count and
// reset its idle time; a deleted entry's NACK is left alone (#624).
describe(`XREADGROUP history re-delivery (ioredis, ${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('stream-redelivery')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('XPENDING and XINFO STREAM FULL see the bumped NACKs', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'f', '1')
      await node.xadd(key, '2-1', 'f', '2')
      await node.xgroup('CREATE', key, 'workers', '0')
      await node.xreadgroup('GROUP', 'workers', 'alice', 'STREAMS', key, '>')
      assert.strictEqual(await node.xdel(key, '2-1'), 1)
      await sleep(SLEEP_MS)

      assert.deepStrictEqual(
        await node.xreadgroup('GROUP', 'workers', 'alice', 'STREAMS', key, '0'),
        [
          [
            key,
            [
              ['1-1', ['f', '1']],
              ['2-1', null],
            ],
          ],
        ],
      )

      const pending = (await node.xpending(key, 'workers', '-', '+', 10)) as [
        string,
        string,
        number,
        number,
      ][]
      assert.deepStrictEqual(
        pending.map(([id, consumer, , count]) => [id, consumer, count]),
        [
          ['1-1', 'alice', 2],
          ['2-1', 'alice', 1],
        ],
      )
      assert.ok(pending[0][2] < FRESH_MS, `1-1 idle ${pending[0][2]}`)
      assert.ok(pending[1][2] >= SLEEP_MS, `2-1 idle ${pending[1][2]}`)

      const info = (await node.xinfo('STREAM', key, 'FULL')) as unknown[]
      const groups = kvArrayGet(info, 'groups') as unknown[][]
      const groupPel = kvArrayGet(groups[0], 'pending') as unknown[][]
      assert.deepStrictEqual(
        groupPel.map(row => [row[0], row[1], row[3]]),
        [
          ['1-1', 'alice', 2],
          ['2-1', 'alice', 1],
        ],
      )
      const consumers = kvArrayGet(groups[0], 'consumers') as unknown[][]
      const consumerPel = kvArrayGet(consumers[0], 'pending') as unknown[][]
      assert.deepStrictEqual(
        consumerPel.map(row => [row[0], row[2]]),
        [
          ['1-1', 2],
          ['2-1', 1],
        ],
      )
      // Delivery times: 1-1 was just re-delivered, 2-1 kept the first one.
      assert.ok(
        (groupPel[0][2] as number) - (groupPel[1][2] as number) >= SLEEP_MS / 2,
      )
    } finally {
      node.disconnect()
    }
  })
})
