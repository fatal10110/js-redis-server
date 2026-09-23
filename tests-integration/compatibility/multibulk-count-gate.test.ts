import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'

/**
 * The multibulk element-count bound is version-specific (#441): Redis 6.2
 * refuses a count above 1024*1024, 7.0 relaxed it to INT_MAX. Verified on
 * redis 6.2.24 / 7.0.15 / 8.0 and valkey 7.2.14 (valkey forked after the
 * change, so every Valkey profile has the INT_MAX bound).
 *
 * A count that passes the check is otherwise invisible — the server just waits
 * for elements — so each probe follows the header with a `+` element prefix:
 * past the count check, the parser names that byte instead.
 */
const testRunner = new TestRunner()
const profile = activeProfile

const INVALID_MULTIBULK = '-ERR Protocol error: invalid multibulk length\r\n'
const EXPECTED_DOLLAR = "-ERR Protocol error: expected '$', got '+'\r\n"

describe(
  `multibulk count bound (${testRunner.getBackendName()}, ${profile})`,
  { skip: testRunner.backend === 'real' && 'profiles are mock-only' },
  () => {
    let port: number
    const connections: RawRedisConnection[] = []

    before(async () => {
      port = await testRunner.setupRawStandalone()
    })

    after(async () => {
      for (const connection of connections) {
        connection.close()
      }
      connections.length = 0
      await testRunner.cleanup()
    })

    async function probe(count: string): Promise<string> {
      const conn = await RawRedisConnection.connect('127.0.0.1', port)
      connections.push(conn)
      conn.write(`*${count}\r\n+x\r\n`)
      return (await conn.readUntilClose()).toString()
    }

    const intMaxBound = profile !== 'redis-6.2'

    test('1024*1024 elements are accepted on every profile', async () => {
      assert.strictEqual(await probe('1048576'), EXPECTED_DOLLAR)
    })

    test('1024*1024 + 1 elements follow the profile bound', async () => {
      assert.strictEqual(
        await probe('1048577'),
        intMaxBound ? EXPECTED_DOLLAR : INVALID_MULTIBULK,
      )
    })

    test('INT_MAX elements follow the profile bound', async () => {
      assert.strictEqual(
        await probe('2147483647'),
        intMaxBound ? EXPECTED_DOLLAR : INVALID_MULTIBULK,
      )
    })

    test('INT_MAX + 1 elements are refused on every profile', async () => {
      assert.strictEqual(await probe('2147483648'), INVALID_MULTIBULK)
    })
  },
)
