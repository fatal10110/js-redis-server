import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * QUIT has arity -1 in real Redis (#370): trailing arguments are ignored, not
 * refused. No client sends them, so this is a wire test. Inside MULTI it is
 * not queued (#567): every version answers +OK and closes, dropping the
 * queued commands.
 */
const testRunner = new TestRunner()

describe(`Raw TCP QUIT (${testRunner.getBackendName()})`, () => {
  let port: number

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('replies +OK and closes, ignoring trailing arguments', async () => {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    try {
      conn.write(commandFrame('QUIT', 'extra', 'args'))
      assert.deepStrictEqual(
        await conn.readUntilClose(),
        Buffer.from('+OK\r\n'),
      )
    } finally {
      conn.close()
    }
  })

  test('MULTI then QUIT replies +OK twice and closes', async () => {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    try {
      conn.write(Buffer.concat([commandFrame('MULTI'), commandFrame('QUIT')]))
      assert.deepStrictEqual(
        await conn.readUntilClose(),
        Buffer.from('+OK\r\n+OK\r\n'),
      )
    } finally {
      conn.close()
    }
  })

  test('QUIT inside MULTI closes without running the queued commands', async () => {
    const key = `quit-multi:${randomKey()}`
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    const other = await RawRedisConnection.connect('127.0.0.1', port)
    try {
      await expectReply(conn, ['WATCH', key], '+OK\r\n')
      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['SET', key, '1'], '+QUEUED\r\n')
      conn.write(commandFrame('QUIT', 'extra'))
      assert.deepStrictEqual(
        await conn.readUntilClose(),
        Buffer.from('+OK\r\n'),
      )

      await expectReply(other, ['GET', key], '$-1\r\n')
      await expectReply(other, ['SET', key, '2'], '+OK\r\n')
      await expectReply(other, ['DEL', key], ':1\r\n')
    } finally {
      conn.close()
      other.close()
    }
  })
})
