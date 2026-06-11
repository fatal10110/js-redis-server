import { describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisResult } from '../src'
import { createRedisSessionHarness as createSession } from './core-session-test-helpers'

const NOAUTH = RedisResult.error('Authentication required.', 'NOAUTH')
const WRONGPASS = RedisResult.error(
  'invalid username-password pair or user is disabled.',
  'WRONGPASS',
)
const HELLO_NOAUTH = RedisResult.error(
  'HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time',
  'NOAUTH',
)

describe('AUTH enforcement with requirepass configured', () => {
  test('rejects commands before authentication with NOAUTH', async () => {
    const { session } = createSession({ requirepass: 'secret' })

    assert.deepStrictEqual(
      await session.execute('get', [Buffer.from('k')]),
      NOAUTH,
    )
    // PING is gated too — only AUTH/HELLO/RESET/QUIT are exempt.
    assert.deepStrictEqual(await session.execute('ping', []), NOAUTH)
  })

  test('AUTH with the wrong password is WRONGPASS and stays unauthenticated', async () => {
    const { session } = createSession({ requirepass: 'secret' })

    assert.deepStrictEqual(
      await session.execute('auth', [Buffer.from('nope')]),
      WRONGPASS,
    )
    assert.deepStrictEqual(
      await session.execute('get', [Buffer.from('k')]),
      NOAUTH,
    )
  })

  test('AUTH with the correct password unlocks the connection', async () => {
    const { session } = createSession({ requirepass: 'secret' })

    assert.deepStrictEqual(
      await session.execute('auth', [Buffer.from('secret')]),
      RedisResult.ok(),
    )
    assert.notDeepStrictEqual(
      await session.execute('get', [Buffer.from('k')]),
      NOAUTH,
    )
  })

  test('two-arg AUTH default <password> works under requirepass', async () => {
    const { session } = createSession({ requirepass: 'secret' })

    assert.deepStrictEqual(
      await session.execute('auth', [
        Buffer.from('default'),
        Buffer.from('secret'),
      ]),
      RedisResult.ok(),
    )
  })

  test('bare HELLO before authentication returns the HELLO-specific NOAUTH', async () => {
    const { session } = createSession({ requirepass: 'secret' })

    assert.deepStrictEqual(
      await session.execute('hello', [Buffer.from('2')]),
      HELLO_NOAUTH,
    )
  })

  test('HELLO AUTH authenticates inline and unlocks the connection', async () => {
    const { session } = createSession({ requirepass: 'secret' })

    const reply = await session.execute('hello', [
      Buffer.from('2'),
      Buffer.from('AUTH'),
      Buffer.from('default'),
      Buffer.from('secret'),
    ])
    assert.notDeepStrictEqual(reply, HELLO_NOAUTH)
    assert.notDeepStrictEqual(reply, WRONGPASS)

    assert.notDeepStrictEqual(
      await session.execute('get', [Buffer.from('k')]),
      NOAUTH,
    )
  })

  test('HELLO AUTH with the wrong password is WRONGPASS', async () => {
    const { session } = createSession({ requirepass: 'secret' })

    assert.deepStrictEqual(
      await session.execute('hello', [
        Buffer.from('2'),
        Buffer.from('AUTH'),
        Buffer.from('default'),
        Buffer.from('nope'),
      ]),
      WRONGPASS,
    )
  })
})
