import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { STANDALONE_AUTH_PASSWORD, TestRunner } from '../test-config'
import { errorWithMessage } from '../utils'

const testRunner = new TestRunner()

// These run against a password-protected standalone server (requirepass). The
// client connects WITHOUT a password so the AUTH/NOAUTH/WRONGPASS sequence can
// be driven explicitly. Subtests run in order on the shared connection: the
// negative cases must precede the unlock case, since AUTH success persists for
// the rest of the connection's life.
describe(`AUTH enforcement with requirepass (${testRunner.getBackendName()})`, () => {
  let client: Redis | undefined

  before(async () => {
    client = await testRunner.setupIoredisStandaloneAuth()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('commands before authentication are rejected with NOAUTH', async () => {
    await assert.rejects(
      () => client!.get('k') as Promise<unknown>,
      errorWithMessage('NOAUTH Authentication required.'),
    )
  })

  test('PING is gated too (not on the no-auth allowlist)', async () => {
    await assert.rejects(
      () => client!.ping() as Promise<unknown>,
      errorWithMessage('NOAUTH Authentication required.'),
    )
  })

  test('AUTH with the wrong password is WRONGPASS', async () => {
    await assert.rejects(
      () => client!.call('AUTH', 'wrong') as Promise<unknown>,
      errorWithMessage(
        'WRONGPASS invalid username-password pair or user is disabled.',
      ),
    )
  })

  test('bare HELLO before authentication returns the HELLO-specific NOAUTH', async () => {
    await assert.rejects(
      () => client!.call('HELLO', '2') as Promise<unknown>,
      errorWithMessage(
        'NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time',
      ),
    )
  })

  test('a rejected HELLO does not apply SETNAME before authentication', async () => {
    // HELLO option side effects (SETNAME) must not mutate connection state when
    // the command fails the NOAUTH gate on a password-protected server.
    await assert.rejects(
      () => client!.call('HELLO', '2', 'SETNAME', 'leaked') as Promise<unknown>,
      errorWithMessage(
        'NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time',
      ),
    )

    // Authenticate, then confirm the failed SETNAME never stuck.
    assert.strictEqual(
      await client!.call('AUTH', 'default', STANDALONE_AUTH_PASSWORD),
      'OK',
    )
    const name = await client!.client('GETNAME')
    assert.notStrictEqual(name, 'leaked')
    assert.ok(name === null || name === '')
  })

  test('two-arg AUTH default <password> unlocks the connection', async () => {
    assert.strictEqual(
      await client!.call('AUTH', 'default', STANDALONE_AUTH_PASSWORD),
      'OK',
    )
    // Once authenticated, previously gated commands succeed.
    assert.strictEqual(await client!.get('k'), null)
  })
})
