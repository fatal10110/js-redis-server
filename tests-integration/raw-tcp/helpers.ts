import assert from 'node:assert'
import { commandFrame } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * Write a command as a RESP array of bulk strings and assert the raw reply
 * frame byte-for-byte. The whole point of the raw-tcp error suites is wire
 * fidelity, so we compare the exact bytes (e.g. `-ERR ...\r\n`) rather than a
 * client-parsed value.
 */
export async function expectReply(
  conn: RawRedisConnection,
  args: string[],
  expected: string,
): Promise<void> {
  conn.write(commandFrame(...args))
  assert.strictEqual((await conn.readRawFrame()).toString(), expected)
}
