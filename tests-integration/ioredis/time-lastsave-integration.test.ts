import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../test-config'

const testRunner = new TestRunner()

describe(`TIME / LASTSAVE integration (${testRunner.getBackendName()})`, () => {
  let client: Redis | undefined

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('TIME returns [seconds, microseconds] close to current time', async () => {
    const before = Math.floor(Date.now() / 1000)
    const reply = (await client?.time()) as [string, string]
    const after = Math.floor(Date.now() / 1000)

    assert.ok(Array.isArray(reply), 'TIME must return an array')
    assert.strictEqual(reply.length, 2, 'TIME must return two elements')

    const seconds = Number(reply[0])
    const micros = Number(reply[1])

    assert.ok(Number.isInteger(seconds), 'seconds must be an integer string')
    assert.ok(
      Number.isInteger(micros),
      'microseconds must be an integer string',
    )
    assert.ok(
      seconds >= before - 1 && seconds <= after + 1,
      `seconds ${seconds} should be near ${before}..${after}`,
    )
    assert.ok(
      micros >= 0 && micros <= 999999,
      `microseconds ${micros} must be in [0, 999999]`,
    )
  })

  test('LASTSAVE returns a Unix timestamp integer not in the future', async () => {
    const reply = await client?.lastsave()
    const now = Math.floor(Date.now() / 1000)

    const timestamp = Number(reply)
    assert.ok(Number.isInteger(timestamp), 'LASTSAVE must return an integer')
    assert.ok(timestamp > 0, 'LASTSAVE timestamp must be positive')
    assert.ok(
      timestamp <= now + 1,
      `LASTSAVE timestamp ${timestamp} must not be in the future (now ${now})`,
    )
  })
})
