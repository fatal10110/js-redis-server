import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisClientType } from 'redis'
import { STANDALONE_AUTH_PASSWORD, TestRunner } from '../test-config'
import { errorWithMessage } from '../utils'

const testRunner = new TestRunner()

// These run against a password-protected standalone server (requirepass). The
// client connects WITHOUT a password so the AUTH/NOAUTH/WRONGPASS sequence can
// be driven explicitly. Subtests run in order on the shared connection.
describe(`AUTH enforcement with requirepass (node-redis, ${testRunner.getBackendName()})`, () => {
  let client: RedisClientType

  before(async () => {
    client = await testRunner.setupNodeRedisStandaloneAuth()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('commands before authentication are rejected with NOAUTH', async () => {
    await assert.rejects(
      () => client.sendCommand(['GET', 'k']),
      errorWithMessage('NOAUTH Authentication required.'),
    )
  })

  test('PING is gated too (not on the no-auth allowlist)', async () => {
    await assert.rejects(
      () => client.sendCommand(['PING']),
      errorWithMessage('NOAUTH Authentication required.'),
    )
  })

  test('AUTH with the wrong password is WRONGPASS', async () => {
    await assert.rejects(
      () => client.sendCommand(['AUTH', 'wrong']),
      errorWithMessage(
        'WRONGPASS invalid username-password pair or user is disabled.',
      ),
    )
  })

  test('bare HELLO before authentication returns the HELLO-specific NOAUTH', async () => {
    await assert.rejects(
      () => client.sendCommand(['HELLO', '2']),
      errorWithMessage(
        'NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time',
      ),
    )
  })

  test('a rejected HELLO does not apply SETNAME before authentication', async () => {
    await assert.rejects(
      () => client.sendCommand(['HELLO', '2', 'SETNAME', 'leaked']),
      errorWithMessage(
        'NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time',
      ),
    )

    assert.strictEqual(
      await client.sendCommand(['AUTH', 'default', STANDALONE_AUTH_PASSWORD]),
      'OK',
    )
    const name = await client.sendCommand(['CLIENT', 'GETNAME'])
    assert.notStrictEqual(name, 'leaked')
    assert.ok(name === null || name === '')
  })

  test('two-arg AUTH default <password> unlocks the connection', async () => {
    assert.strictEqual(
      await client.sendCommand(['AUTH', 'default', STANDALONE_AUTH_PASSWORD]),
      'OK',
    )
    assert.strictEqual(await client.sendCommand(['GET', 'k']), null)
  })
})
