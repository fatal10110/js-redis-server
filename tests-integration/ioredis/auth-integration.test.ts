import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { errorWithMessage } from '../utils'

const testRunner = new TestRunner()

// All cases below run against a server with NO requirepass configured (the
// default `nopass` user), matching real Redis 7.2 behavior. The requirepass /
// NOAUTH enforcement path cannot be exercised through this harness (the real
// backend is a fixed instance with no password), so it is covered by unit
// tests in tests/commands-auth.test.ts instead.
describe(`AUTH / HELLO AUTH with no password (${testRunner.getBackendName()})`, () => {
  let client: Redis | undefined

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('single-arg AUTH errors when no password is configured', async () => {
    await assert.rejects(
      () => client!.call('AUTH', 'somepassword') as Promise<unknown>,
      errorWithMessage(
        'ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?',
      ),
    )
  })

  test('two-arg AUTH for the default user succeeds (nopass)', async () => {
    assert.strictEqual(await client!.call('AUTH', 'default', 'anything'), 'OK')
  })

  test('two-arg AUTH for an unknown user is WRONGPASS', async () => {
    await assert.rejects(
      () => client!.call('AUTH', 'nobody', 'anything') as Promise<unknown>,
      errorWithMessage(
        'WRONGPASS invalid username-password pair or user is disabled.',
      ),
    )
  })

  test('HELLO ... AUTH is a no-op handshake when no password is configured', async () => {
    const reply = (await client!.call('HELLO', '2', 'AUTH', 'default', '')) as
      | unknown[]
      | null

    assert.ok(Array.isArray(reply), 'HELLO should return a reply array')
    const flat = reply.map(item => String(item))
    assert.ok(flat.includes('server'))
    assert.ok(flat.includes('redis'))
  })
})
