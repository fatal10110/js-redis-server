import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
  encodeRedisValue,
  type CompatibilitySpec,
} from '../../src/internal'

/**
 * HELLO ... SETNAME refused with NOAUTH (#522): 6.2 and 7.0 have set the name
 * by then (redis@7.0.15:src/networking.c:3491-3507), 7.2+ has not. Probed on
 * redis-server 6.2.14, 7.0.15 and 7.2.4 with --requirepass.
 */
describe('HELLO SETNAME before the NOAUTH check', () => {
  for (const [spec, expected] of [
    ['redis-6.2', '$6\r\nleaked\r\n'],
    ['redis-7.0', '$6\r\nleaked\r\n'],
    ['redis-7.2', '$-1\r\n'],
    ['valkey-8.0', '$-1\r\n'],
  ] as const) {
    test(`${spec}: CLIENT GETNAME after AUTH answers ${JSON.stringify(expected)}`, async () => {
      const server = new RedisServerState({
        compatibility: spec as CompatibilitySpec,
        databaseCount: 16,
        requirepass: 'pw',
      })
      const executor = createRedisCommandExecutor({
        compatibility: server.profile,
      })
      const s = new ClientSession({ server, executor })
      const run = async (name: string, ...args: string[]) =>
        encodeRedisValue(
          (
            await s.execute(
              name,
              args.map(arg => Buffer.from(arg)),
            )
          ).value,
        ).toString()

      assert.match(await run('HELLO', '2', 'SETNAME', 'leaked'), /^-NOAUTH /)
      assert.strictEqual(await run('AUTH', 'pw'), '+OK\r\n')
      assert.strictEqual(await run('CLIENT', 'GETNAME'), expected)
    })
  }
})
