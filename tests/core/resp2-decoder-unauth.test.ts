import { describe, test } from 'node:test'
import assert from 'node:assert'

import { resolveCompatibilityProfile } from '../../src/core/compatibility'
import {
  Resp2CommandDecoder,
  Resp2ParseError,
} from '../../src/core/transports/resp2/decoder'
import type { CompatibilitySpec } from '../../src/core/compatibility'

/**
 * Decoder-level pins for #565: before AUTH, Redis refuses a multibulk count
 * above 10 and a bulk length above 16384 (`processMultibulkBuffer`, since redis
 * 5.0; the same on every profile). "Still accepted" is only observable here as
 * `next()` returning `null`; the wire side is in
 * tests-integration/raw-tcp/unauthenticated-request-limits.test.ts.
 */
const UNAUTH_MULTIBULK = 'Protocol error: unauthenticated multibulk length'
const UNAUTH_BULK = 'Protocol error: unauthenticated bulk length'

function decoder(
  authRequired?: () => boolean,
  spec?: CompatibilitySpec,
): Resp2CommandDecoder {
  return new Resp2CommandDecoder({
    maxBulkLength: () => 512n * 1024n * 1024n,
    profile: spec === undefined ? undefined : resolveCompatibilityProfile(spec),
    authRequired,
  })
}

function assertProtocolError(d: Resp2CommandDecoder, message: string): void {
  assert.throws(
    () => d.next(),
    (err: unknown) => err instanceof Resp2ParseError && err.message === message,
  )
}

function pushed(d: Resp2CommandDecoder, bytes: string): Resp2CommandDecoder {
  d.push(Buffer.from(bytes))
  return d
}

describe('RESP2 decoder unauthenticated request caps (#565)', () => {
  const locked = () => true

  test('a multibulk count of 10 waits, 11 is refused', () => {
    assert.strictEqual(pushed(decoder(locked), '*10\r\n').next(), null)
    assertProtocolError(pushed(decoder(locked), '*11\r\n'), UNAUTH_MULTIBULK)
    assertProtocolError(
      pushed(decoder(locked), '*11\r\n$3\r\nfoo\r\n'),
      UNAUTH_MULTIBULK,
    )
  })

  test('a bulk length of 16384 waits, 16385 is refused', () => {
    assert.strictEqual(pushed(decoder(locked), '*1\r\n$16384\r\n').next(), null)
    assertProtocolError(
      pushed(decoder(locked), '*1\r\n$16385\r\n'),
      UNAUTH_BULK,
    )
    assertProtocolError(
      pushed(decoder(locked), '*2\r\n$3\r\nGET\r\n$16385\r\n'),
      UNAUTH_BULK,
    )
  })

  test('a skipped empty multibulk does not lift the cap', () => {
    assertProtocolError(
      pushed(decoder(locked), '*-1\r\n*11\r\n'),
      UNAUTH_MULTIBULK,
    )
  })

  test('the version bound on the count is checked first', () => {
    // 6.2 bounds the count at 1024*1024, so it reports that; 7.0+ bound it at
    // INT_MAX and report the unauthenticated cap instead.
    assertProtocolError(
      pushed(decoder(locked, 'redis-6.2'), '*2000000\r\n'),
      'Protocol error: invalid multibulk length',
    )
    assertProtocolError(
      pushed(decoder(locked, 'redis-7.0'), '*2000000\r\n'),
      UNAUTH_MULTIBULK,
    )
    assertProtocolError(
      pushed(decoder(locked), '*1\r\n$-1\r\n'),
      'Protocol error: invalid bulk length',
    )
  })

  test('inline requests have no such cap', () => {
    const d = pushed(
      decoder(locked),
      `ECHO ${'x'.repeat(20000)} a b c d e f g h i j\r\n`,
    )
    assert.strictEqual(d.next()?.args.length, 11)
  })

  test('the auth state is read live for every header', () => {
    let required = true
    const d = decoder(() => required)
    d.push(Buffer.from('*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n'))
    assert.strictEqual(d.next()?.command.toString(), 'AUTH')
    required = false
    d.push(Buffer.from(`*11\r\n$3\r\nDEL${'\r\n$1\r\nk'.repeat(10)}\r\n`))
    assert.strictEqual(d.next()?.args.length, 10)
    required = true
    d.push(Buffer.from('*11\r\n'))
    assertProtocolError(d, UNAUTH_MULTIBULK)
  })

  test('without the callback nothing is capped', () => {
    const d = pushed(decoder(), '*11\r\n$16385\r\n')
    assert.strictEqual(d.next(), null)
  })
})
