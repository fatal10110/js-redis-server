import { after, afterEach, before, beforeEach, describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as delay } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`RANDOMKEY integration (${testRunner.getBackendName()})`, () => {
  let client: Redis | undefined

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  beforeEach(async () => {
    await client?.flushall()
    await client?.select(0)
  })

  afterEach(async () => {
    await client?.flushall()
    await client?.select(0)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('returns null when the selected database is empty', async () => {
    assert.strictEqual(await client?.randomkey(), null)
  })

  test('returns a key from the currently selected database', async () => {
    const first = `randomkey:${randomKey()}:first`
    const second = `randomkey:${randomKey()}:second`
    const otherDb = `randomkey:${randomKey()}:other-db`

    await client?.set(first, 'one')
    await client?.set(second, 'two')

    assert.ok([first, second].includes((await client?.randomkey()) as string))

    await client?.select(1)
    assert.strictEqual(await client?.randomkey(), null)

    await client?.set(otherDb, 'other')
    assert.strictEqual(await client?.randomkey(), otherDb)
  })

  test('does not return lazily expired keys', async () => {
    const key = `randomkey-expired:${randomKey()}`

    await client?.set(key, 'expired', 'PX', 1)
    await delay(50)

    assert.strictEqual(await client?.randomkey(), null)
    assert.strictEqual(await client?.exists(key), 0)
  })
})
