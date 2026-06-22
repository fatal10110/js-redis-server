import { RedisClientType } from 'redis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

// SELECT is rejected in cluster mode, so this regression must run against a
// standalone server (in-process Resp2Server on mock, real redis-server on real).
const testRunner = new TestRunner()

describe('SELECT inside MULTI (node-redis, standalone)', () => {
  let client: RedisClientType

  before(async () => {
    client = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('a queued SELECT switches the DB for later commands in the same EXEC', async () => {
    const key = randomKey()

    const res = await client
      .multi()
      .addCommand(['SELECT', '1'])
      .set(key, 'value')
      .exec()

    assert.deepStrictEqual(res, ['OK', 'OK'])

    // The SET must have landed in DB 1 (selected mid-EXEC), not DB 0.
    await client.select(1)
    assert.strictEqual(await client.get(key), 'value')

    await client.select(0)
    assert.strictEqual(await client.get(key), null)
  })
})
