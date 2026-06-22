import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType } from 'redis'
import { TestRunner } from '../test-config'
import { errorWithMessage } from '../utils'

// FLUSHDB / FLUSHALL gained an optional ASYNC | SYNC modifier in Redis 4.0.
// The mock must accept (and ignore) the keyword, reject any other token with a
// syntax error, and still work with no argument. Run on the standalone harness
// because these are keyless admin commands the cluster client can't route
// cleanly to a single node.
const testRunner = new TestRunner()

describe(`FLUSHDB/FLUSHALL ASYNC|SYNC (node-redis, ${testRunner.getBackendName()})`, () => {
  let client: RedisClientType

  before(async () => {
    client = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('FLUSHDB accepts ASYNC and clears the keyspace', async () => {
    await client.set('k1', 'v1')
    assert.strictEqual(await client.sendCommand(['FLUSHDB', 'ASYNC']), 'OK')
    assert.strictEqual(await client.dbSize(), 0)
  })

  test('FLUSHDB accepts SYNC', async () => {
    await client.set('k2', 'v2')
    assert.strictEqual(await client.sendCommand(['FLUSHDB', 'SYNC']), 'OK')
    assert.strictEqual(await client.dbSize(), 0)
  })

  test('FLUSHDB ASYNC|SYNC is case-insensitive', async () => {
    assert.strictEqual(await client.sendCommand(['FLUSHDB', 'async']), 'OK')
    assert.strictEqual(await client.sendCommand(['FLUSHDB', 'Sync']), 'OK')
  })

  test('FLUSHDB with no argument still works', async () => {
    assert.strictEqual(await client.sendCommand(['FLUSHDB']), 'OK')
  })

  test('FLUSHALL accepts ASYNC and clears all databases', async () => {
    await client.set('k3', 'v3')
    assert.strictEqual(await client.sendCommand(['FLUSHALL', 'ASYNC']), 'OK')
    assert.strictEqual(await client.dbSize(), 0)
  })

  test('FLUSHALL accepts SYNC', async () => {
    assert.strictEqual(await client.sendCommand(['FLUSHALL', 'SYNC']), 'OK')
  })

  test('FLUSHALL with no argument still works', async () => {
    assert.strictEqual(await client.sendCommand(['FLUSHALL']), 'OK')
  })

  test('FLUSHDB rejects an unknown modifier with a syntax error', async () => {
    await assert.rejects(
      () => client.sendCommand(['FLUSHDB', 'FOO']),
      errorWithMessage('ERR syntax error'),
    )
  })

  test('FLUSHDB rejects two modifiers with a syntax error', async () => {
    await assert.rejects(
      () => client.sendCommand(['FLUSHDB', 'ASYNC', 'SYNC']),
      errorWithMessage('ERR syntax error'),
    )
  })

  test('FLUSHALL rejects an unknown modifier with a syntax error', async () => {
    await assert.rejects(
      () => client.sendCommand(['FLUSHALL', 'FOO']),
      errorWithMessage('ERR syntax error'),
    )
  })

  test('FLUSHALL rejects two modifiers with a syntax error', async () => {
    await assert.rejects(
      () => client.sendCommand(['FLUSHALL', 'ASYNC', 'SYNC']),
      errorWithMessage('ERR syntax error'),
    )
  })
})
