import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  formatHostPort,
  formatSocketAddressParts,
} from '../src/core/network-address'

describe('network address formatting', () => {
  test('formats IPv4 host and port pairs', () => {
    assert.strictEqual(formatHostPort('127.0.0.1', 6379), '127.0.0.1:6379')
    assert.strictEqual(
      formatSocketAddressParts('::ffff:172.18.0.1', 52967),
      '172.18.0.1:52967',
    )
  })

  test('preserves IPv6 client addresses with brackets', () => {
    assert.strictEqual(formatHostPort('::1', 6379), '[::1]:6379')
    assert.strictEqual(
      formatSocketAddressParts('2001:db8::1', 52967),
      '[2001:db8::1]:52967',
    )
  })

  test('returns an address without a port when the socket has no remote port', () => {
    assert.strictEqual(formatSocketAddressParts('::1', undefined), '::1')
    assert.strictEqual(formatSocketAddressParts(undefined, 6379), undefined)
  })
})
