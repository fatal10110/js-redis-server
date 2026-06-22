import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

// CONFIG GET is a flat array on RESP2 and a map (object) on RESP3 — node-redis
// negotiates RESP3, so normalise both into a Map keyed by lower-cased name.
function configToMap(reply: unknown): Map<string, string> {
  const map = new Map<string, string>()
  if (Array.isArray(reply)) {
    for (let i = 0; i < reply.length; i += 2) {
      map.set(String(reply[i]).toLowerCase(), String(reply[i + 1]))
    }
    return map
  }
  for (const [key, value] of Object.entries(reply as Record<string, unknown>)) {
    map.set(key.toLowerCase(), String(value))
  }
  return map
}

describe(`CONFIG GET/SET integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType
  let directClient: RedisClientType
  let standaloneClient: RedisClientType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      `{config:${randomKey()}}:probe`,
    )
    standaloneClient = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    directClient.destroy()
    await testRunner.cleanup()
  })

  test('CONFIG GET returns a value for a known parameter', async () => {
    const config = configToMap(
      await directClient.sendCommand(['CONFIG', 'GET', 'appendonly']),
    )
    assert.ok(config.has('appendonly'))
    assert.strictEqual(typeof config.get('appendonly'), 'string')
  })

  test('CONFIG GET parameter names are case-insensitive', async () => {
    const config = configToMap(
      await directClient.sendCommand(['CONFIG', 'GET', 'APPENDONLY']),
    )
    assert.ok(config.has('appendonly'))
  })

  test('CONFIG SET updates a value that CONFIG GET reads back', async () => {
    assert.strictEqual(
      await directClient.sendCommand([
        'CONFIG',
        'SET',
        'maxmemory-policy',
        'allkeys-lru',
      ]),
      'OK',
    )

    const config = configToMap(
      await directClient.sendCommand(['CONFIG', 'GET', 'maxmemory-policy']),
    )
    assert.strictEqual(config.get('maxmemory-policy'), 'allkeys-lru')
  })

  test('CONFIG GET supports glob patterns matching multiple parameters', async () => {
    const config = configToMap(
      await directClient.sendCommand(['CONFIG', 'GET', 'maxmemory*']),
    )
    assert.ok(config.has('maxmemory'))
    assert.ok(config.has('maxmemory-policy'))
  })

  test('CONFIG SET rejects an unknown parameter', async () => {
    await assert.rejects(() =>
      directClient.sendCommand([
        'CONFIG',
        'SET',
        'definitely-not-a-real-param',
        '1',
      ]),
    )
  })

  test('CONFIG RESETSTAT returns OK', async () => {
    assert.strictEqual(
      await standaloneClient.sendCommand(['CONFIG', 'RESETSTAT']),
      'OK',
    )
  })

  test('CONFIG RESETSTAT rejects extra arguments', async () => {
    await assert.rejects(
      () => standaloneClient.sendCommand(['CONFIG', 'RESETSTAT', 'extra']),
      errorWithMessage(
        "ERR wrong number of arguments for 'config|resetstat' command",
      ),
    )
  })

  test('CONFIG REWRITE rejects when no config file is loaded', async () => {
    await assert.rejects(
      () => standaloneClient.sendCommand(['CONFIG', 'REWRITE']),
      errorWithMessage('ERR The server is running without a config file'),
    )
  })

  test('CONFIG REWRITE rejects extra arguments', async () => {
    await assert.rejects(
      () => standaloneClient.sendCommand(['CONFIG', 'REWRITE', 'extra']),
      errorWithMessage(
        "ERR wrong number of arguments for 'config|rewrite' command",
      ),
    )
  })
})
