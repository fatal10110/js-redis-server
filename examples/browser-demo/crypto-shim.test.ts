import { test, describe } from 'node:test'
import assert from 'node:assert'
import nodeCrypto from 'node:crypto'
import shim, { createHash } from './crypto-shim'

const nodeSha1 = (data: string | Buffer) =>
  nodeCrypto.createHash('sha1').update(data).digest('hex')

describe('crypto shim sha1', () => {
  test('matches the FIPS 180 "abc" vector', () => {
    assert.strictEqual(
      createHash('sha1').update('abc').digest('hex'),
      'a9993e364706816aba3e25717850c26c9cd0d89d',
    )
  })

  test('matches node:crypto across every padding boundary', () => {
    // 0..200 bytes crosses the 55/56/64-byte padding edges several times.
    for (let len = 0; len <= 200; len++) {
      const bytes = Buffer.alloc(len, 0)
      for (let i = 0; i < len; i++) {
        bytes[i] = (i * 31 + len) & 0xff
      }
      assert.strictEqual(
        createHash('sha1').update(bytes).digest('hex'),
        nodeSha1(bytes),
        `length ${len}`,
      )
    }
  })

  test('hashes strings as UTF-8 and concatenates updates, like Node', () => {
    const script = "return redis.call('GET', KEYS[1]) -- é ✓"
    assert.strictEqual(
      shim.createHash('sha1').update(script).digest('hex'),
      nodeSha1(script),
    )
    assert.strictEqual(
      createHash('sha1').update('ab').update(Buffer.from('c')).digest('hex'),
      nodeSha1('abc'),
    )
  })

  test('raw digest is a Buffer (hyperloglog reads a BigUInt64 from it)', () => {
    const digest = createHash('sha1').update(Buffer.from('elem')).digest()
    assert.ok(Buffer.isBuffer(digest))
    assert.strictEqual(
      digest.readBigUInt64BE(0),
      nodeCrypto
        .createHash('sha1')
        .update(Buffer.from('elem'))
        .digest()
        .readBigUInt64BE(0),
    )
  })

  test('rejects any other algorithm loudly', () => {
    assert.throws(() => createHash('md5'), /unsupported hash md5/)
  })
})
