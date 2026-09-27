import { afterEach, describe, test } from 'node:test'
import assert from 'node:assert'
import { createInMemoryClient } from '../src'
import type { CompatibilitySpec, InMemoryRedisClient } from '../src'

/**
 * The Lua sandbox a profile gets. Valkey 7.2 is not one of the presets the
 * compatibility suite runs, so its mapping (Redis 7.2's sandbox plus the
 * `server` alias) is pinned here, next to Valkey 8.0 and Redis 7.2 for
 * contrast. Replies checked against valkey-server 7.2.14 / 8.0.11 and
 * redis-server 7.2.16.
 */
describe('Lua engine profile per compatibility profile', () => {
  let client: InMemoryRedisClient

  afterEach(() => {
    client?.close()
  })

  async function sandbox(compatibility: CompatibilitySpec) {
    client = await createInMemoryClient({ compatibility })
    const has = (global: string) =>
      client.command(
        'EVAL',
        `local ok = pcall(function() return ${global} end) return ok and 1 or 0`,
        '0',
      )
    return {
      server: await has('server'),
      os: await has('os'),
      logLevel: await client.command(
        'EVAL',
        "local ok, e = pcall(redis.log, 9, 'x') return e",
        '0',
      ),
      argumentType: await client
        .command('EVAL', "return redis.pcall('set', 'k', {})", '0')
        .then(
          () => assert.fail('expected an error reply'),
          (err: Error) => err.message,
        ),
      versions: await client.command(
        'EVAL',
        'return {redis.REDIS_VERSION, redis.SERVER_NAME, redis.VALKEY_VERSION}',
        '0',
      ),
    }
  }

  test('valkey 7.2 gets the 7.2 sandbox with the server alias', async () => {
    assert.deepStrictEqual(
      await sandbox({ flavor: 'valkey', version: '7.2.4' }),
      {
        server: 1,
        os: 0,
        logLevel: 'ERR Invalid debug level.',
        argumentType:
          'ERR Lua redis lib command arguments must be strings or integers',
        versions: ['7.2.4', 'valkey', '7.2.4'],
      },
    )
  })

  test('valkey 8.0 gets its own sandbox', async () => {
    assert.deepStrictEqual(await sandbox('valkey-8.0'), {
      server: 1,
      os: 1,
      logLevel: 'ERR Invalid log level.',
      argumentType: 'ERR Command arguments must be strings or integers',
      versions: ['7.2.4', 'valkey', '8.0.0'],
    })
  })

  test('redis 7.2 has no server alias', async () => {
    assert.deepStrictEqual(await sandbox('redis-7.2'), {
      server: 0,
      os: 0,
      logLevel: 'ERR Invalid debug level.',
      argumentType:
        'ERR Lua redis lib command arguments must be strings or integers',
      versions: ['7.2.4'],
    })
  })
})
