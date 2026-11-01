import { after, before, describe, test } from 'node:test'
import { needsCluster, TestRunner } from '../test-config'
import { activeProfile, keyInAnotherSlot, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import {
  connectToRawSlotOwner,
  expectReply,
  expectReplyPrefix,
  rawSlotOwner,
} from './helpers'

/**
 * A cluster MULTI whose queued commands touch different slots (#519). Each
 * command is queued (`+QUEUED`); EXEC's own cluster check (`getNodeByQuery`
 * over the whole queue) answers a bare `-CROSSSLOT` and discards the
 * transaction (unwatching its keys), so nothing runs. A single command whose
 * keys span slots is still refused while queuing and aborts EXEC.
 *
 * Once a queued command is refused, Redis stops queueing and keeps what it
 * queued, so EXEC still answers CROSSSLOT for the earlier commands; Valkey 8.0
 * empties the queue on that refusal, so EXEC answers EXECABORT.
 *
 * From 7.0 a touched WATCH stops queueing as well (CLIENT_DIRTY_CAS), so
 * EXEC answers nil for later commands that span slots; 6.2 keeps queueing and
 * answers CROSSSLOT.
 *
 * Checked against single-node clusters of redis-server 7.0.15, 7.2.4, 7.4.4,
 * 8.0.0 and valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const valkey = activeProfile.startsWith('valkey')
const legacy = activeProfile === 'redis-6.2'

const QUEUED = '+QUEUED\r\n'
const CROSSSLOT = "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
const EXECABORT =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'
const EXEC_WITHOUT_MULTI = '-ERR EXEC without MULTI\r\n'

const clusterSuite = `Raw TCP cluster MULTI across slots (${testRunner.getBackendName()}, ${activeProfile})`
describe(clusterSuite, needsCluster, () => {
  let ports: number[]
  const connections: RawRedisConnection[] = []

  before(async () => {
    ports = await testRunner.setupRawCluster()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  /**
   * A connection to the owner of `first`, and a key in another slot of the
   * same node (the tag differs, the node does not).
   */
  async function setup() {
    const first = `{mcs:${randomKey()}}:a`
    const conn = await connectToRawSlotOwner(ports, first)
    connections.push(conn)
    const owner = await rawSlotOwner(ports, first)
    const candidate = () => `{mcs:${randomKey()}}:b`
    let second = keyInAnotherSlot(first, candidate)
    while ((await rawSlotOwner(ports, second)).port !== owner.port) {
      second = keyInAnotherSlot(first, candidate)
    }
    return { conn, first, second }
  }

  test('EXEC answers CROSSSLOT and discards the transaction', async () => {
    const { conn, first, second } = await setup()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', first, '1'], QUEUED)
    await expectReply(conn, ['SET', second, '2'], QUEUED)
    await expectReply(conn, ['EXEC'], CROSSSLOT)
    await expectReply(conn, ['EXEC'], EXEC_WITHOUT_MULTI)
    await expectReply(conn, ['GET', first], '$-1\r\n')
    await expectReply(conn, ['GET', second], '$-1\r\n')
  })

  test('reads and keyless commands in between take part too', async () => {
    const { conn, first, second } = await setup()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['GET', first], QUEUED)
    await expectReply(conn, ['PING'], QUEUED)
    await expectReply(conn, ['GET', second], QUEUED)
    await expectReply(conn, ['EXEC'], CROSSSLOT)
  })

  test("a script's declared keys take part", async () => {
    const { conn, first, second } = await setup()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', first, '1'], QUEUED)
    await expectReply(
      conn,
      ['EVAL', "return redis.call('SET',KEYS[1],1)", '1', second],
      QUEUED,
    )
    await expectReply(conn, ['EXEC'], CROSSSLOT)
    await expectReply(conn, ['GET', first], '$-1\r\n')
  })

  test('commands in one slot still run', async () => {
    const { conn, first } = await setup()
    const sibling = `${first}:sibling`
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', first, '1'], QUEUED)
    await expectReply(conn, ['SET', sibling, '2'], QUEUED)
    await expectReply(conn, ['EXEC'], '*2\r\n+OK\r\n+OK\r\n')
  })

  test('the CROSSSLOT discard unwatches the keys', async () => {
    const { conn, first, second } = await setup()
    const other = await connectToRawSlotOwner(ports, first)
    connections.push(other)

    await expectReply(conn, ['WATCH', first], '+OK\r\n')
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', first, '1'], QUEUED)
    await expectReply(conn, ['SET', second, '2'], QUEUED)
    await expectReply(conn, ['EXEC'], CROSSSLOT)

    await expectReply(other, ['SET', first, 'changed'], '+OK\r\n')
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', first, 'mine'], QUEUED)
    await expectReply(conn, ['EXEC'], '*1\r\n+OK\r\n')
  })

  test('a single command spanning slots is refused while queuing', async () => {
    const { conn, first, second } = await setup()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', first, '1'], QUEUED)
    await expectReply(conn, ['MSET', first, '1', second, '2'], CROSSSLOT)
    // Nothing is queued after the refusal, so this SET is no part of EXEC's
    // check: the transaction aborts rather than answering CROSSSLOT.
    await expectReply(conn, ['SET', second, '2'], QUEUED)
    await expectReply(conn, ['EXEC'], EXECABORT)
  })

  test('a refusal after cross-slot commands: CROSSSLOT on Redis, EXECABORT on Valkey', async () => {
    const { conn, first, second } = await setup()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', first, '1'], QUEUED)
    await expectReply(conn, ['SET', second, '2'], QUEUED)
    await expectReplyPrefix(conn, ['NOTACOMMAND'], '-ERR unknown command')
    await expectReply(conn, ['EXEC'], valkey ? EXECABORT : CROSSSLOT)
    await expectReply(conn, ['EXEC'], EXEC_WITHOUT_MULTI)
  })

  test('a touched WATCH stops queueing from 7.0: EXEC answers nil, CROSSSLOT on 6.2', async () => {
    const { conn, first, second } = await setup()
    const sibling = `${first}:sibling`
    await expectReply(conn, ['WATCH', first], '+OK\r\n')
    await expectReply(conn, ['SET', first, 'touched'], '+OK\r\n')
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', sibling, '1'], QUEUED)
    await expectReply(conn, ['SET', second, '2'], QUEUED)
    await expectReply(conn, ['EXEC'], legacy ? CROSSSLOT : '*-1\r\n')
    await expectReply(conn, ['EXEC'], EXEC_WITHOUT_MULTI)
    await expectReply(conn, ['GET', second], '$-1\r\n')
  })
})
