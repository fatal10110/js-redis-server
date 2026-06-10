import { Redis } from 'ioredis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

// SELECT is rejected in cluster mode, so this regression must run against a
// standalone server (in-process Resp2Server on mock, real redis-server on real).
const testRunner = new TestRunner()

describe('SELECT inside MULTI (standalone)', () => {
  let client: Redis | undefined

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('a queued SELECT switches the DB for later commands in the same EXEC', async () => {
    const key = randomKey()

    const res = await client!.multi().select(1).set(key, 'value').exec()

    assert.deepStrictEqual(res, [
      [null, 'OK'],
      [null, 'OK'],
    ])

    // The SET must have landed in DB 1 (selected mid-EXEC), not DB 0.
    await client!.select(1)
    assert.strictEqual(await client!.get(key), 'value')

    await client!.select(0)
    assert.strictEqual(await client!.get(key), null)
  })
})
