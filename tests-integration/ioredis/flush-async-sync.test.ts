import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { errorWithMessage } from '../utils'

// FLUSHDB / FLUSHALL gained an optional ASYNC | SYNC modifier in Redis 4.0.
// The mock must accept (and ignore — the keyspace is in-memory, so the flush is
// synchronous either way) the keyword, reject any other token with a syntax
// error, and still work with no argument. Exercised on the standalone harness
// because these are keyless admin commands the cluster client can't route
// cleanly to a single node.
const testRunner = new TestRunner()

describe(`FLUSHDB/FLUSHALL ASYNC|SYNC (${testRunner.getBackendName()})`, () => {
  let client: Redis | undefined

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('FLUSHDB accepts ASYNC and clears the keyspace', async () => {
    await client?.set('k1', 'v1')
    const reply = await client?.call('FLUSHDB', 'ASYNC')
    assert.strictEqual(reply, 'OK')
    assert.strictEqual(await client?.dbsize(), 0)
  })

  test('FLUSHDB accepts SYNC', async () => {
    await client?.set('k2', 'v2')
    const reply = await client?.call('FLUSHDB', 'SYNC')
    assert.strictEqual(reply, 'OK')
    assert.strictEqual(await client?.dbsize(), 0)
  })

  test('FLUSHDB ASYNC|SYNC is case-insensitive', async () => {
    assert.strictEqual(await client?.call('FLUSHDB', 'async'), 'OK')
    assert.strictEqual(await client?.call('FLUSHDB', 'Sync'), 'OK')
  })

  test('FLUSHDB with no argument still works', async () => {
    assert.strictEqual(await client?.call('FLUSHDB'), 'OK')
  })

  test('FLUSHALL accepts ASYNC and clears all databases', async () => {
    await client?.set('k3', 'v3')
    const reply = await client?.call('FLUSHALL', 'ASYNC')
    assert.strictEqual(reply, 'OK')
    assert.strictEqual(await client?.dbsize(), 0)
  })

  test('FLUSHALL accepts SYNC', async () => {
    assert.strictEqual(await client?.call('FLUSHALL', 'SYNC'), 'OK')
  })

  test('FLUSHALL with no argument still works', async () => {
    assert.strictEqual(await client?.call('FLUSHALL'), 'OK')
  })

  test('FLUSHDB rejects an unknown modifier with a syntax error', async () => {
    await assert.rejects(
      async () => client?.call('FLUSHDB', 'FOO'),
      errorWithMessage('ERR syntax error'),
    )
  })

  test('FLUSHDB rejects two modifiers with a syntax error', async () => {
    await assert.rejects(
      async () => client?.call('FLUSHDB', 'ASYNC', 'SYNC'),
      errorWithMessage('ERR syntax error'),
    )
  })

  test('FLUSHALL rejects an unknown modifier with a syntax error', async () => {
    await assert.rejects(
      async () => client?.call('FLUSHALL', 'FOO'),
      errorWithMessage('ERR syntax error'),
    )
  })

  test('FLUSHALL rejects two modifiers with a syntax error', async () => {
    await assert.rejects(
      async () => client?.call('FLUSHALL', 'ASYNC', 'SYNC'),
      errorWithMessage('ERR syntax error'),
    )
  })
})
