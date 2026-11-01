import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile } from '../utils'
import { TrackingClient, invalidateAll } from '../raw-tcp/tracking-client'

/**
 * A tracking client's own FLUSHDB (#252): the null invalidation follows the
 * `+OK` on every version, except a sync flush outside MULTI on redis 7.4+,
 * which runs as a blocking async flush and replies only once it is done, so
 * the null comes first (redis@7.4.0:src/db.c:730-763). Checked against
 * redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0,
 * 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const pushFirst = ['redis-7.4', 'redis-8.0'].includes(profile)

// A database no other suite selects.
const DB = '15'

describe(`CLIENT TRACKING flush order (${testRunner.getBackendName()}, ${profile})`, () => {
  let client: TrackingClient

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    client = await TrackingClient.open(port, true, DB)
    assert.strictEqual(await client.call('CLIENT', 'TRACKING', 'ON'), '+OK\r\n')
  })

  after(async () => {
    client.close()
    await testRunner.cleanup()
  })

  /** Send `args`, read the reply and the null in wire order. */
  async function flushFrames(...args: string[]): Promise<string[]> {
    client.write(...args)
    return [await client.next(), await client.next()]
  }

  for (const mode of [[], ['SYNC']]) {
    test(`FLUSHDB ${mode.join(' ')}: ${pushFirst ? 'null, then +OK' : '+OK, then null'}`, async () => {
      const frames = await flushFrames('FLUSHDB', ...mode)
      assert.deepStrictEqual(
        frames,
        pushFirst ? [invalidateAll, '+OK\r\n'] : ['+OK\r\n', invalidateAll],
      )
    })
  }

  test('FLUSHDB ASYNC: +OK, then null', async () => {
    assert.deepStrictEqual(await flushFrames('FLUSHDB', 'ASYNC'), [
      '+OK\r\n',
      invalidateAll,
    ])
  })

  test('FLUSHDB inside MULTI: the EXEC reply, then null', async () => {
    assert.strictEqual(await client.call('MULTI'), '+OK\r\n')
    assert.strictEqual(await client.call('FLUSHDB'), '+QUEUED\r\n')
    assert.deepStrictEqual(await flushFrames('EXEC'), [
      '*1\r\n+OK\r\n',
      invalidateAll,
    ])
  })

  test('FLUSHDB from a script: the script reply, then null', async () => {
    assert.deepStrictEqual(
      await flushFrames('EVAL', "return redis.call('flushdb')", '0'),
      ['+OK\r\n', invalidateAll],
    )
  })
})
