import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisClientType } from 'redis'
import { TestRunner } from '../test-config'
import { errorWithMessage } from '../utils'

const testRunner = new TestRunner()

// All cases below run against a server with NO requirepass configured (the
// default `nopass` user), matching real Redis 7.2 behavior.
describe(`AUTH / HELLO AUTH with no password (node-redis, ${testRunner.getBackendName()})`, () => {
  let client: RedisClientType

  before(async () => {
    client = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('single-arg AUTH errors when no password is configured', async () => {
    await assert.rejects(
      () => client.sendCommand(['AUTH', 'somepassword']),
      errorWithMessage(
        'ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?',
      ),
    )
  })

  test('two-arg AUTH for the default user succeeds (nopass)', async () => {
    assert.strictEqual(
      await client.sendCommand(['AUTH', 'default', 'anything']),
      'OK',
    )
  })

  test('two-arg AUTH for an unknown user is WRONGPASS', async () => {
    await assert.rejects(
      () => client.sendCommand(['AUTH', 'nobody', 'anything']),
      errorWithMessage(
        'WRONGPASS invalid username-password pair or user is disabled.',
      ),
    )
  })

  test('HELLO ... AUTH is a no-op handshake when no password is configured', async () => {
    const reply = (await client.sendCommand([
      'HELLO',
      '2',
      'AUTH',
      'default',
      '',
    ])) as unknown[]

    assert.ok(Array.isArray(reply), 'HELLO should return a reply array')
    const flat = reply.map(item => String(item))
    assert.ok(flat.includes('server'))
    assert.ok(flat.includes('redis'))
  })
})
