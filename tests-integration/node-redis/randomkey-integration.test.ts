import { after, afterEach, before, beforeEach, describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as delay } from 'node:timers/promises'
import { RedisClientType } from 'redis'
import { TestRunner } from '../test-config'
import { errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`RANDOMKEY integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let client: RedisClientType

  before(async () => {
    client = await testRunner.setupNodeRedisStandalone()
  })

  beforeEach(async () => {
    await client.flushAll()
    await client.select(0)
  })

  afterEach(async () => {
    await client.flushAll()
    await client.select(0)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('returns null when the selected database is empty', async () => {
    assert.strictEqual(await client.sendCommand(['RANDOMKEY']), null)
  })

  test('returns a key from the currently selected database', async () => {
    const first = `randomkey:${randomKey()}:first`
    const second = `randomkey:${randomKey()}:second`
    const otherDb = `randomkey:${randomKey()}:other-db`

    await client.set(first, 'one')
    await client.set(second, 'two')

    assert.ok(
      [first, second].includes(
        (await client.sendCommand(['RANDOMKEY'])) as string,
      ),
    )

    await client.select(1)
    assert.strictEqual(await client.sendCommand(['RANDOMKEY']), null)

    await client.set(otherDb, 'other')
    assert.strictEqual(await client.sendCommand(['RANDOMKEY']), otherDb)
  })

  test('does not return lazily expired keys', async () => {
    const key = `randomkey-expired:${randomKey()}`

    await client.set(key, 'expired', { expiration: { type: 'PX', value: 1 } })
    await delay(50)

    assert.strictEqual(await client.sendCommand(['RANDOMKEY']), null)
    assert.strictEqual(await client.exists(key), 0)
  })

  test('rejects extra arguments', async () => {
    await assert.rejects(
      () => client.sendCommand(['RANDOMKEY', 'extra']),
      errorWithMessage("ERR wrong number of arguments for 'randomkey' command"),
    )
  })
})
