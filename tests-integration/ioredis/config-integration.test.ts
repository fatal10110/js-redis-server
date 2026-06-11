import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, randomKey } from '../utils'

const testRunner = new TestRunner()

function configArrayToMap(reply: unknown): Map<string, string> {
  assert.ok(Array.isArray(reply), 'CONFIG GET should reply with an array')
  const map = new Map<string, string>()
  for (let i = 0; i < reply.length; i += 2) {
    // Redis config parameter names are case-insensitive; normalise to lower.
    map.set(String(reply[i]).toLowerCase(), String(reply[i + 1]))
  }
  return map
}

describe(`CONFIG GET/SET integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined
  let directClient: Redis | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('config-integration')
    directClient = await connectToSlotOwner(
      redisClient,
      `{config:${randomKey()}}:probe`,
    )
  })

  after(async () => {
    directClient?.disconnect()
    await testRunner.cleanup()
  })

  test('CONFIG GET returns a value for a known parameter', async () => {
    const reply = await directClient?.call('CONFIG', 'GET', 'appendonly')
    const config = configArrayToMap(reply)
    assert.ok(config.has('appendonly'), 'CONFIG GET should return appendonly')
    assert.strictEqual(typeof config.get('appendonly'), 'string')
  })

  test('CONFIG GET parameter names are case-insensitive', async () => {
    const reply = await directClient?.call('CONFIG', 'GET', 'APPENDONLY')
    const config = configArrayToMap(reply)
    assert.ok(
      config.has('appendonly'),
      'uppercase query should still match appendonly',
    )
  })

  test('CONFIG SET updates a value that CONFIG GET reads back', async () => {
    const setReply = await directClient?.call(
      'CONFIG',
      'SET',
      'maxmemory-policy',
      'allkeys-lru',
    )
    assert.strictEqual(setReply, 'OK')

    const reply = await directClient?.call('CONFIG', 'GET', 'maxmemory-policy')
    const config = configArrayToMap(reply)
    assert.strictEqual(config.get('maxmemory-policy'), 'allkeys-lru')
  })

  test('CONFIG GET supports glob patterns matching multiple parameters', async () => {
    const reply = await directClient?.call('CONFIG', 'GET', 'maxmemory*')
    const config = configArrayToMap(reply)
    assert.ok(config.has('maxmemory'), 'glob should match maxmemory')
    assert.ok(
      config.has('maxmemory-policy'),
      'glob should match maxmemory-policy',
    )
  })

  test('CONFIG SET rejects an unknown parameter', async () => {
    await assert.rejects(async () =>
      directClient?.call('CONFIG', 'SET', 'definitely-not-a-real-param', '1'),
    )
  })
})
