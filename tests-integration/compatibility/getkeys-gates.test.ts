import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * `COMMAND GETKEYS` / `GETKEYSANDFLAGS` rows whose reply depends on the
 * profile (#518). 6.2 has no subcommand entries and no key specs: it asks the
 * getkeys procedure or the legacy key range, and answers `Invalid arguments
 * specified for command` when they find nothing. From 7.0 the key specs
 * answer; Valkey 8.0+ marks GEORADIUS's STORE / STOREDIST specs
 * `variable_flags`, so its procedure (last destination wins) does. 7.0 alone
 * needs an argument after the target command. Checked against redis-server
 * 6.2.24, 7.0.15, 7.2.4, 7.4.4, 8.0.0 / 8.0.6 and valkey 8.0.11 / 9.0.6. The
 * Redis 8.0 matrix is `tests-integration/raw-tcp/command-getkeys-matrix.test.ts`.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const valkey = profile.startsWith('valkey-')

const NO_KEYS = '-ERR The command has no key arguments\r\n'
const INVALID_ARGS = '-ERR Invalid arguments specified for command\r\n'
const INVALID_COUNT =
  '-ERR Invalid number of arguments specified for command\r\n'
const INVALID_COMMAND = '-ERR Invalid command specified\r\n'

const bulk = (value: string) => `$${value.length}\r\n${value}\r\n`
const keys = (...names: string[]) =>
  `*${names.length}\r\n${names.map(bulk).join('')}`
const flagged = (...entries: Array<[string, string[]]>) =>
  `*${entries.length}\r\n${entries
    .map(
      ([key, flags]) =>
        `*2\r\n${bulk(key)}*${flags.length}\r\n${flags.map(flag => `+${flag}\r\n`).join('')}`,
    )
    .join('')}`

const geo = ['GEORADIUS', 'k', '0', '0', '1', 'km']
const source: [string, string[]] = ['k', ['RO', 'access']]
const dest = (key: string): [string, string[]] => [key, ['OW', 'update']]

const GETKEYS: Array<[string[], string]> = [
  [
    ['PING'],
    profile === 'redis-7.0'
      ? "-ERR wrong number of arguments for 'command|getkeys' command\r\n"
      : NO_KEYS,
  ],
  [['CLIENT', 'BOGUS'], legacy ? NO_KEYS : INVALID_COMMAND],
  [['XINFO', 'HELP'], legacy ? INVALID_ARGS : NO_KEYS],
  [['XINFO', 'HELP', 'x'], legacy ? keys('x') : NO_KEYS],
  [['XINFO', 'STREAM'], legacy ? INVALID_ARGS : INVALID_COUNT],
  [['XGROUP', 'CREATE', 's', 'g'], legacy ? keys('s') : INVALID_COUNT],
  [['XREAD', 'STREAMS', 'a', 'b', '0'], legacy ? INVALID_ARGS : keys('a')],
  [
    ['ZUNIONSTORE', 'd', '1', 'a', 'BOGUS'],
    legacy ? keys('a', 'd') : keys('d', 'a'),
  ],
  [['EVAL', 's', '2', 'a'], legacy ? INVALID_ARGS : '*0\r\n'],
  [
    [...geo, 'STORE', 'a', 'STOREDIST', 'b'],
    legacy || valkey ? keys('k', 'b') : keys('k', 'a', 'b'),
  ],
  [['SPUBLISH', 'ch', 'msg'], legacy ? INVALID_COMMAND : NO_KEYS],
  [['LMPOP', '1x', 'a', 'LEFT'], legacy ? INVALID_COMMAND : keys('a')],
]

const GETKEYSANDFLAGS: Array<[string[], string]> = [
  [
    [...geo, 'STORE', 'd', 'STORE', 'e'],
    valkey ? flagged(source, dest('e')) : flagged(source, dest('d')),
  ],
  [
    [...geo, 'STORE', 'd', 'STOREDIST', 'e'],
    valkey ? flagged(source, dest('e')) : flagged(source, dest('d'), dest('e')),
  ],
  [['SPUBLISH', 'ch', 'msg'], NO_KEYS],
]

describe(`COMMAND GETKEYS profile rows (${testRunner.getBackendName()}, ${profile})`, () => {
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

  test('COMMAND GETKEYS', async () => {
    const conn = await connect()
    for (const [command, reply] of GETKEYS) {
      await expectReply(conn, ['COMMAND', 'GETKEYS', ...command], reply)
    }
  })

  test(
    'COMMAND GETKEYSANDFLAGS',
    { skip: legacy && 'COMMAND GETKEYSANDFLAGS is 7.0+' },
    async () => {
      const conn = await connect()
      for (const [command, reply] of GETKEYSANDFLAGS) {
        await expectReply(
          conn,
          ['COMMAND', 'GETKEYSANDFLAGS', ...command],
          reply,
        )
      }
    },
  )
})
