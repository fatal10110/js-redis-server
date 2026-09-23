import { describe, test } from 'node:test'
import assert from 'node:assert'

import { resolveCompatibilityProfile } from '../../src/core/compatibility'
import {
  Resp2CommandDecoder,
  Resp2ParseError,
} from '../../src/core/transports/resp2/decoder'
import type { CompatibilitySpec } from '../../src/core/compatibility'

/**
 * Decoder-level pins for #441 that the raw-tcp suite cannot assert
 * deterministically: "not refused yet" is only observable as `next()`
 * returning `null`, never as bytes on a socket.
 */
function decoder(spec?: CompatibilitySpec): Resp2CommandDecoder {
  return new Resp2CommandDecoder({
    maxBulkLength: () => 512n * 1024n * 1024n,
    profile: spec === undefined ? undefined : resolveCompatibilityProfile(spec),
  })
}

function assertProtocolError(d: Resp2CommandDecoder, message: string): void {
  assert.throws(
    () => d.next(),
    (err: unknown) => err instanceof Resp2ParseError && err.message === message,
  )
}

const INVALID_MULTIBULK = 'Protocol error: invalid multibulk length'
const TOO_BIG_INLINE = 'Protocol error: too big inline request'

describe('Resp2CommandDecoder multibulk count bound', () => {
  test('6.2 refuses a count above 1024*1024', () => {
    const accepted = decoder('redis-6.2')
    accepted.push(Buffer.from('*1048576\r\n'))
    assert.strictEqual(accepted.next(), null)

    const refused = decoder('redis-6.2')
    refused.push(Buffer.from('*1048577\r\n'))
    assertProtocolError(refused, INVALID_MULTIBULK)
  })

  for (const spec of [
    'redis-7.0',
    'redis-8.0',
    { flavor: 'valkey', version: '7.2.0' },
    undefined,
  ] as const) {
    test(`${JSON.stringify(spec) ?? 'no profile'} accepts up to INT_MAX`, () => {
      const accepted = decoder(spec)
      accepted.push(Buffer.from('*2147483647\r\n'))
      assert.strictEqual(accepted.next(), null)

      const refused = decoder(spec)
      refused.push(Buffer.from('*2147483648\r\n'))
      assertProtocolError(refused, INVALID_MULTIBULK)
    })
  }

  test('a negative count is skipped, not refused', () => {
    const d = decoder()
    d.push(Buffer.from('*-5\r\n*1\r\n$4\r\nPING\r\n'))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('PING'),
      args: [],
    })
  })
})

describe('Resp2CommandDecoder bulk terminator', () => {
  test('the two bytes after a payload are skipped unchecked', () => {
    const d = decoder()
    d.push(Buffer.from('*2\r\n$4\r\nECHOXX$2\r\nhi\n\n'))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('ECHO'),
      args: [Buffer.from('hi')],
    })
  })

  test('still waits for both skipped bytes before dispatching', () => {
    const d = decoder()
    d.push(Buffer.from('*1\r\n$4\r\nPINGX'))
    assert.strictEqual(d.next(), null)
    d.push(Buffer.from('X'))
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
  })
})

describe('Resp2CommandDecoder inline cap', () => {
  test('exactly 64KB with no newline waits; one byte more is refused', () => {
    const d = decoder()
    d.push(Buffer.alloc(64 * 1024, 0x61))
    assert.strictEqual(d.next(), null)

    d.push(Buffer.from('a'))
    assertProtocolError(d, TOO_BIG_INLINE)
  })

  test('the cap counts from the start of the inline request', () => {
    const d = decoder()
    d.push(Buffer.from('PING\r\n'))
    d.push(Buffer.alloc(64 * 1024, 0x61))
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
    assert.strictEqual(d.next(), null)
  })

  test('a line over 64KB is served when its newline is already buffered', () => {
    const d = decoder()
    const value = Buffer.alloc(70000, 0x61)
    d.push(Buffer.concat([Buffer.from('ECHO '), value, Buffer.from('\r\n')]))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('ECHO'),
      args: [value],
    })
  })

  test('a bare LF ends an inline request', () => {
    const d = decoder()
    d.push(Buffer.from('ECHO hi\nPING\r\n'))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('ECHO'),
      args: [Buffer.from('hi')],
    })
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
  })
})
