import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType, RESP_TYPES } from 'redis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

const testRunner = new TestRunner()

// DUMP / RESTORE through node-redis (#202). DUMP's payload is binary, so it is
// read with a Buffer type mapping.
describe(`DUMP / RESTORE Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let client: RedisClusterType

  before(async () => {
    client = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('DUMP then RESTORE with REPLACE and a ttl', async () => {
    const tag = `{${randomKey()}}`
    await client.hSet(`${tag}:h`, { f: 'v', n: '7' })
    const binary = client.withTypeMapping({
      [RESP_TYPES.BLOB_STRING]: Buffer,
    })
    const payload = (await binary.dump(`${tag}:h`)) as unknown as Buffer
    assert.ok(Buffer.isBuffer(payload))

    assert.strictEqual(await client.restore(`${tag}:copy`, 0, payload), 'OK')
    assert.deepStrictEqual(
      { ...(await client.hGetAll(`${tag}:copy`)) },
      { f: 'v', n: '7' },
    )
    await assert.rejects(client.restore(`${tag}:copy`, 0, payload), /BUSYKEY/)
    assert.strictEqual(
      await client.restore(`${tag}:copy`, 60000, payload, { REPLACE: true }),
      'OK',
    )
    const pttl = await client.pTTL(`${tag}:copy`)
    assert.ok(pttl > 30000 && pttl <= 60000, String(pttl))
    assert.strictEqual(await client.dump(`${tag}:missing`), null)
  })
})
