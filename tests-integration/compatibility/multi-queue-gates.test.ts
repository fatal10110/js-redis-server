import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import {
  connectToRawSlotOwner,
  expectReply,
  expectReplyPrefix,
} from '../raw-tcp/helpers'

/**
 * MULTI rows whose reply depends on the profile (#518); the Redis 8.0 rows
 * are `tests-integration/raw-tcp/multi-queue-errors.test.ts`. Redis 6.2 has
 * no subcommand entries, so an unknown or wrong-count container subcommand
 * is queued and answered by the container at EXEC, and it words the odd
 * MSET / XADD tail and the unknown-command error its own way. A Valkey 9
 * cluster has databases (`cluster.multi-db`), so a queued SELECT / MOVE runs
 * there instead of answering `... is not allowed in cluster mode`. Checked
 * against redis-server 6.2.24, 7.0.15, 8.0.6 and valkey 8.0.11 / 9.0.6,
 * standalone and clustered.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const valkey9 = profile === 'valkey-9.0'

const QUEUED = '+QUEUED\r\n'
const EXECABORT =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'
const arity = (command: string) =>
  `-ERR wrong number of arguments for '${command}' command\r\n`

async function queueOne(
  conn: RawRedisConnection,
  command: string[],
  queued: string,
  exec: string,
): Promise<void> {
  await expectReply(conn, ['MULTI'], '+OK\r\n')
  await expectReply(conn, command, queued)
  await expectReply(conn, ['EXEC'], exec)
}

describe(`MULTI profile rows (${testRunner.getBackendName()}, ${profile})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    return connection
  }

  test('deferred odd-tail errors use the profile wording', async () => {
    const conn = await connect()
    const tag = `{multi-g:${randomKey()}}`
    const mset = legacy
      ? '-ERR wrong number of arguments for MSET\r\n'
      : arity('mset')
    const xadd = legacy
      ? '-ERR wrong number of arguments for XADD\r\n'
      : arity('xadd')

    await queueOne(
      conn,
      ['MSET', `${tag}:a`, 'b', `${tag}:c`],
      QUEUED,
      `*1\r\n${mset}`,
    )
    await queueOne(
      conn,
      ['XADD', `${tag}:x`, '*', 'f', 'v', 'x'],
      QUEUED,
      `*1\r\n${xadd}`,
    )
    await queueOne(
      conn,
      ['XADD', `${tag}:x`, 'MAXLEN', '10', '*'],
      QUEUED,
      `*1\r\n${xadd}`,
    )
  })

  test('an unknown command uses the profile wording', async () => {
    const conn = await connect()
    await queueOne(
      conn,
      ['NOSUCHCMD'],
      legacy
        ? '-ERR unknown command `NOSUCHCMD`, with args beginning with: \r\n'
        : "-ERR unknown command 'NOSUCHCMD', with args beginning with: \r\n",
      EXECABORT,
    )
  })

  // 6.2 queues these and the container answers when EXEC runs it.
  test('container subcommands are refused at queue time from 7.0', async () => {
    const conn = await connect()
    const key = `{multi-g:${randomKey()}}:x`

    if (legacy) {
      await queueOne(
        conn,
        ['CONFIG', 'BOGUS'],
        QUEUED,
        "*1\r\n-ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try CONFIG HELP.\r\n",
      )
      // Real 6.2 answers `Unknown subcommand or wrong number of arguments
      // for 'DESTROY'. Try XGROUP HELP.` in the EXEC slot; this server still
      // uses the 7.0 arity wording there (#437), so only the queueing is
      // pinned.
      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['XGROUP', 'DESTROY', key], QUEUED)
      await expectReplyPrefix(conn, ['EXEC'], '*1\r\n-ERR ')
      return
    }

    await queueOne(
      conn,
      ['CONFIG', 'BOGUS'],
      "-ERR unknown subcommand 'BOGUS'. Try CONFIG HELP.\r\n",
      EXECABORT,
    )
    await queueOne(
      conn,
      ['XGROUP', 'DESTROY', key],
      arity('xgroup|destroy'),
      EXECABORT,
    )
  })
})

describe(`MULTI profile rows in a cluster (${testRunner.getBackendName()}, ${profile})`, () => {
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

  // `multiDbOnly` (MOVE) and `singleDb` (SELECT): refused when they run,
  // unless the cluster has databases; a single-database Valkey 9 node then
  // answers its own range error.
  test('SELECT and MOVE are queued and answer at EXEC', async () => {
    const local = `{multi-gc:${randomKey()}}:k`
    const conn = await connectToRawSlotOwner(ports, local)
    connections.push(conn)
    const notAllowed = (command: string) =>
      `-ERR ${command} is not allowed in cluster mode\r\n`
    const outOfRange = '-ERR DB index is out of range\r\n'
    const notInteger = '-ERR value is not an integer or out of range\r\n'

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SELECT', '1'], QUEUED)
    await expectReply(conn, ['SET', local, 'v'], QUEUED)
    await expectReply(
      conn,
      ['EXEC'],
      `*2\r\n${valkey9 ? outOfRange : notAllowed('SELECT')}+OK\r\n`,
    )
    await queueOne(
      conn,
      ['MOVE', local, '1'],
      QUEUED,
      `*1\r\n${valkey9 ? outOfRange : notAllowed('MOVE')}`,
    )
    await queueOne(
      conn,
      ['MOVE', local, 'x'],
      QUEUED,
      `*1\r\n${valkey9 ? notInteger : notAllowed('MOVE')}`,
    )
    await expectReply(conn, ['DEL', local], ':1\r\n')
  })
})
