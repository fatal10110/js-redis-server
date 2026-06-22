import assert from 'node:assert'
import clusterKeySlot from 'cluster-key-slot'
import { commandFrame } from '../utils'
import { RawRedisConnection, respNumber, respText } from './raw-connection'

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

/**
 * Write a command and assert the reply is a RESP bulk string equal to `value`.
 */
export async function expectBulk(
  conn: RawRedisConnection,
  args: string[],
  value: string,
): Promise<void> {
  conn.write(commandFrame(...args))
  assert.strictEqual(
    (await conn.readRawFrame()).toString(),
    `$${Buffer.byteLength(value)}\r\n${value}\r\n`,
  )
}

/**
 * Write a command and read (discard) exactly one reply frame. For setup /
 * mutation steps whose reply value is not under test — only their effect on a
 * subsequent WATCH/EXEC is.
 */
export async function send(
  conn: RawRedisConnection,
  args: string[],
): Promise<void> {
  conn.write(commandFrame(...args))
  await conn.readRawFrame()
}

/**
 * Like `expectReply`, but only asserts the reply starts with `prefix`. Use for
 * replies whose tail is version-specific (e.g. the unknown-command error, which
 * echoes the offending args).
 */
export async function expectReplyPrefix(
  conn: RawRedisConnection,
  args: string[],
  prefix: string,
): Promise<void> {
  conn.write(commandFrame(...args))
  const reply = (await conn.readRawFrame()).toString()
  assert.ok(
    reply.startsWith(prefix),
    `expected reply to start with ${JSON.stringify(prefix)}, got ${JSON.stringify(reply)}`,
  )
}

/**
 * Open a raw socket to the cluster node that owns `key`'s slot.
 *
 * Discovers ownership over a bare connection (`CLUSTER SLOTS` against the first
 * node), so raw-tcp tests can drive cluster-routed commands — interleaved
 * MULTI/EXEC/WATCH, CROSSSLOT, MOVED — without an ioredis/node-redis client in
 * the loop. `ports` comes from `TestRunner.setupRawCluster()`.
 */
export async function connectToRawSlotOwner(
  ports: number[],
  key: string,
): Promise<RawRedisConnection> {
  assert.ok(ports.length > 0, 'connectToRawSlotOwner: no cluster ports')
  const slot = clusterKeySlot(key)

  const probe = await RawRedisConnection.connect('127.0.0.1', ports[0])
  let host: string | undefined
  let port: number | undefined
  try {
    probe.write(commandFrame('CLUSTER', 'SLOTS'))
    const slots = await probe.readFrame()
    assert.ok(Array.isArray(slots), 'CLUSTER SLOTS: expected an array reply')

    for (const range of slots) {
      if (!Array.isArray(range)) continue
      const [min, max, master] = range
      if (
        typeof min === 'number' &&
        typeof max === 'number' &&
        slot >= min &&
        slot <= max &&
        Array.isArray(master)
      ) {
        host = respText(master[0]) || '127.0.0.1'
        port = respNumber(master[1])
        break
      }
    }
  } finally {
    probe.close()
  }

  if (port === undefined) {
    throw new Error(`No raw cluster slot owner found for slot ${slot}`)
  }
  return RawRedisConnection.connect(host ?? '127.0.0.1', port)
}
