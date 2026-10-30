import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * A standalone (`cluster-enabled no`) server still has CLUSTER, READONLY and
 * READWRITE in its command table: it lists and describes them, and answers
 * them with `This instance has cluster support disabled` (#537). Byte for
 * byte against redis-server 7.0.15 (local) and the transcripts of
 * redis-server 6.2.24, 7.0.15, 8.0.6 and valkey 9.0.6 in #537; READONLY /
 * READWRITE per version from the `readonlyCommand` / `readwriteCommand`
 * sources of redis 6.2.24, 8.0.6 and valkey 7.2.5, 8.0.0, 8.0.6, 9.0.0.
 *
 * Profile-aware: run with `REDIS_COMPAT=<preset>` against the mock on that
 * profile or a real server of that version.
 */
const testRunner = new TestRunner()

const legacy = activeProfile === 'redis-6.2'
const valkey8 = activeProfile === 'valkey-8.0' || activeProfile === 'valkey-9.0'
const DISABLED = '-ERR This instance has cluster support disabled\r\n'

describe(`Raw TCP standalone CLUSTER / READONLY / READWRITE (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  test('a known CLUSTER subcommand is refused as disabled', async () => {
    const conn = await connect()

    await expectReply(conn, ['CLUSTER', 'INFO'], DISABLED)
    await expectReply(conn, ['CLUSTER', 'MYID'], DISABLED)
    await expectReply(conn, ['CLUSTER', 'SLOTS'], DISABLED)
    await expectReply(conn, ['cluster', 'nodes'], DISABLED)
    // Real subcommands a cluster node of this server does not implement.
    await expectReply(conn, ['CLUSTER', 'HELP'], DISABLED)
    await expectReply(conn, ['CLUSTER', 'KEYSLOT', 'foo'], DISABLED)
  })

  test('an unknown CLUSTER subcommand, per profile', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['CLUSTER', 'BOGUS'],
      legacy
        ? DISABLED
        : "-ERR unknown subcommand 'BOGUS'. Try CLUSTER HELP.\r\n",
    )
  })

  test('CLUSTER arity, per profile', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['CLUSTER'],
      "-ERR wrong number of arguments for 'cluster' command\r\n",
    )
    // 6.2 has no `cluster|info` entry; only the container's -2 applies.
    await expectReply(
      conn,
      ['CLUSTER', 'INFO', 'extra'],
      legacy
        ? DISABLED
        : "-ERR wrong number of arguments for 'cluster|info' command\r\n",
    )
  })

  test('READONLY / READWRITE, per profile', async () => {
    const conn = await connect()

    // Refused up to Valkey 8.0, which accepts both.
    await expectReply(conn, ['READONLY'], valkey8 ? '+OK\r\n' : DISABLED)
    // 6.2 never checked; Redis 7.0 / Valkey 7.2 added the check.
    await expectReply(
      conn,
      ['READWRITE'],
      legacy || valkey8 ? '+OK\r\n' : DISABLED,
    )
    await expectReply(
      conn,
      ['READONLY', 'x'],
      "-ERR wrong number of arguments for 'readonly' command\r\n",
    )
    await expectReply(
      conn,
      ['READWRITE', 'x'],
      "-ERR wrong number of arguments for 'readwrite' command\r\n",
    )
  })

  test('COMMAND GETKEYS knows CLUSTER', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['COMMAND', 'GETKEYS', 'CLUSTER', 'INFO'],
      '-ERR The command has no key arguments\r\n',
    )
  })

  test('COMMAND INFO describes READONLY / READWRITE, per profile', async () => {
    const conn = await connect()
    const entry = (name: string) =>
      legacy
        ? // Redis 6.2.24: 7 fields, `fast`, `@keyspace @fast`.
          `*1\r\n*7\r\n$${name.length}\r\n${name}\r\n:1\r\n*1\r\n+fast\r\n:0\r\n:0\r\n:0\r\n*2\r\n+@keyspace\r\n+@fast\r\n`
        : `*1\r\n*10\r\n$${name.length}\r\n${name}\r\n:1\r\n*3\r\n+loading\r\n+stale\r\n+fast\r\n:0\r\n:0\r\n:0\r\n*2\r\n+@fast\r\n+@connection\r\n*0\r\n*0\r\n*0\r\n`

    await expectReply(conn, ['COMMAND', 'INFO', 'readonly'], entry('readonly'))
    await expectReply(
      conn,
      ['COMMAND', 'INFO', 'readwrite'],
      entry('readwrite'),
    )
  })

  test(
    'COMMAND INFO describes cluster|info',
    { skip: legacy && '6.2 has no subcommand entries' },
    async () => {
      const conn = await connect()
      // Valkey 8.0 / 9.0 also flag it `loading` (valkey 8.0.11 / 9.0.6).
      const flags = valkey8
        ? '*2\r\n+loading\r\n+stale\r\n'
        : '*1\r\n+stale\r\n'

      await expectReply(
        conn,
        ['COMMAND', 'INFO', 'cluster|info'],
        `*1\r\n*10\r\n$12\r\ncluster|info\r\n:2\r\n${flags}:0\r\n:0\r\n:0\r\n*1\r\n+@slow\r\n*1\r\n$23\r\nnondeterministic_output\r\n*0\r\n*0\r\n`,
      )
    },
  )
})
