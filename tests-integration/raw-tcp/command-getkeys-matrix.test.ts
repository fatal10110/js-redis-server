import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply, send } from './helpers'

/**
 * `COMMAND GETKEYS` / `GETKEYSANDFLAGS`, row by row, byte for byte as real
 * Redis 8.0 answers them (#518). Redis finds keys without running the
 * command: lookup (from 7.0 the `container|subcommand` entry), then whether
 * that entry has keys at all (a `not_key` spec does not count), then its
 * table arity, then the key specs, falling back to the command's getkeys
 * procedure when a spec cannot be applied. Other profiles' rows live in
 * `tests-integration/compatibility/getkeys-gates.test.ts`.
 */
const testRunner = new TestRunner()

const NO_KEYS = '-ERR The command has no key arguments\r\n'
const INVALID_ARGS = '-ERR Invalid arguments specified for command\r\n'
const INVALID_COUNT =
  '-ERR Invalid number of arguments specified for command\r\n'
const INVALID_COMMAND = '-ERR Invalid command specified\r\n'

const bulk = (value: string) => `$${value.length}\r\n${value}\r\n`
const keys = (...names: string[]) =>
  `*${names.length}\r\n${names.map(bulk).join('')}`

// [target command..., reply]
const GETKEYS: Array<[string[], string]> = [
  // Valid calls.
  [['GET', 'k'], keys('k')],
  [['MSET', 'a', '1', 'b', '2'], keys('a', 'b')],
  [['ZUNIONSTORE', 'd', '2', 'a', 'b'], keys('d', 'a', 'b')],
  [['EVAL', 's', '2', 'a', 'b'], keys('a', 'b')],
  [['XREAD', 'COUNT', '1', 'STREAMS', 'a', 'b', '0', '0'], keys('a', 'b')],
  [['SORT', 'k', 'STORE', 'd'], keys('k', 'd')],
  [['LMPOP', '2', 'a', 'b', 'LEFT'], keys('a', 'b')],
  [['XINFO', 'STREAM', 'a'], keys('a')],
  [['XGROUP', 'CREATE', 's', 'g', '$'], keys('s')],
  // Redis 8.0's STORE and STOREDIST specs each find their own keyword.
  [
    ['GEORADIUS', 'k', '0', '0', '1', 'km', 'STORE', 'a', 'STOREDIST', 'b'],
    keys('k', 'a', 'b'),
  ],
  // No key arguments: checked before arity, per subcommand.
  [['PING'], NO_KEYS],
  [['ECHO'], NO_KEYS],
  [['CLIENT', 'REPLY'], NO_KEYS],
  [['CLIENT', 'KILL'], NO_KEYS],
  [['CONFIG', 'GET'], NO_KEYS],
  [['XINFO', 'HELP'], NO_KEYS],
  // Shard channels: Redis's only spec is `not_key`.
  [['SPUBLISH', 'ch', 'msg'], NO_KEYS],
  [['SSUBSCRIBE', 'ch'], NO_KEYS],
  [['SUNSUBSCRIBE'], NO_KEYS],
  // Lookup and arity.
  [['NOSUCHCMD', 'a'], INVALID_COMMAND],
  [['CLIENT', 'BOGUS'], INVALID_COMMAND],
  [['GET', 'a', 'b'], INVALID_COUNT],
  [['XINFO', 'STREAM'], INVALID_COUNT],
  // Calls whose own parser would fail: the key specs still answer, or the
  // getkeys procedure when a spec cannot be applied (numkeys read like atoi).
  [['XREAD', 'STREAMS', 'a', 'b', '0'], keys('a')],
  [['XREAD', 'BOGUS', 'STREAMS', 'a', '0'], keys('a')],
  [['ZUNIONSTORE', 'd', '2abc', 'a', 'b'], keys('a', 'b', 'd')],
  [['ZUNIONSTORE', 'd', '1', 'a', 'BOGUS'], keys('d', 'a')],
  [['ZUNIONSTORE', 'd', '5', 'a'], INVALID_ARGS],
  [['ZUNIONSTORE', 'd', '0', 'a'], INVALID_ARGS],
  [['SINTERCARD', '2', 'a'], INVALID_ARGS],
  [['EVAL', 's', '1x', 'a'], keys('a')],
  // EVAL may have no keys (`no_mandatory_keys`).
  [['EVAL', 's', '2', 'a'], '*0\r\n'],
  [['EVAL', 's', '0'], '*0\r\n'],
  [['SET', 'k', 'v', 'BOGUS'], keys('k')],
  [['MSET', 'a', 'b', 'c'], keys('a', 'c')],
  [['HSET', 'h', 'f', 'v', 'x'], keys('h')],
]

type Flagged = [string, string[]]
// [target command..., keys with flags] or an error reply.
const GETKEYSANDFLAGS: Array<[string[], Flagged[] | string]> = [
  [
    ['ZUNIONSTORE', 'd', '2', 'a', 'b'],
    [
      ['d', ['OW', 'update']],
      ['a', ['RO', 'access']],
      ['b', ['RO', 'access']],
    ],
  ],
  // A numkeys spec that cannot be applied: the procedure's keys, no flags.
  [
    ['ZUNIONSTORE', 'd', '2abc', 'a', 'b'],
    [
      ['a', []],
      ['b', []],
      ['d', []],
    ],
  ],
  [
    ['GEORADIUS', 'k', '0', '0', '1', 'km', 'STORE', 'd', 'STORE', 'e'],
    [
      ['k', ['RO', 'access']],
      ['d', ['OW', 'update']],
    ],
  ],
  [
    ['GEORADIUS', 'k', '0', '0', '1', 'km', 'STORE', 'd', 'STOREDIST', 'e'],
    [
      ['k', ['RO', 'access']],
      ['d', ['OW', 'update']],
      ['e', ['OW', 'update']],
    ],
  ],
  [['XGROUP', 'CREATE', 's', 'g', '$'], [['s', ['RW', 'insert']]]],
  [['XGROUP', 'DESTROY', 's', 'g'], [['s', ['RW', 'delete']]]],
  [['XINFO', 'STREAM', 's'], [['s', ['RO', 'access']]]],
  // SET's spec is variable_flags; its procedure decides by GET.
  [['SET', 'k', 'v'], [['k', ['OW', 'update']]]],
  [['SET', 'k', 'v', 'GET'], [['k', ['RW', 'access', 'update']]]],
  [
    ['SORT', 'k', 'STORE', 'd'],
    [
      ['k', ['RO', 'access']],
      ['d', ['OW', 'update']],
    ],
  ],
  [
    ['EVAL', 's', '2', 'a', 'b'],
    [
      ['a', ['RW', 'access', 'update']],
      ['b', ['RW', 'access', 'update']],
    ],
  ],
  [['XREAD', 'STREAMS', 'a', 'b', '0'], [['a', ['RO', 'access']]]],
  [['SPUBLISH', 'ch', 'msg'], NO_KEYS],
  [['CLIENT', 'REPLY'], NO_KEYS],
]

describe(
  `Raw TCP COMMAND GETKEYS matrix (${testRunner.getBackendName()}, ${activeProfile})`,
  { skip: activeProfile !== 'redis-8.0' && 'rows are Redis 8.0 replies' },
  () => {
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

    async function connect(protocol: '2' | '3'): Promise<RawRedisConnection> {
      const connection = await RawRedisConnection.connect('127.0.0.1', port)
      connections.push(connection)
      await send(connection, ['HELLO', protocol])
      return connection
    }

    test('COMMAND GETKEYS', async () => {
      const conn = await connect('2')
      for (const [command, reply] of GETKEYS) {
        await expectReply(conn, ['COMMAND', 'GETKEYS', ...command], reply)
      }
    })

    // Each key's flags are an array in RESP2 and a set in RESP3.
    for (const protocol of ['2', '3'] as const) {
      test(`COMMAND GETKEYSANDFLAGS (RESP${protocol})`, async () => {
        const conn = await connect(protocol)
        const flagsType = protocol === '3' ? '~' : '*'
        for (const [command, expected] of GETKEYSANDFLAGS) {
          const reply =
            typeof expected === 'string'
              ? expected
              : `*${expected.length}\r\n${expected
                  .map(
                    ([key, flags]) =>
                      `*2\r\n${bulk(key)}${flagsType}${flags.length}\r\n${flags
                        .map(flag => `+${flag}\r\n`)
                        .join('')}`,
                  )
                  .join('')}`
          await expectReply(
            conn,
            ['COMMAND', 'GETKEYSANDFLAGS', ...command],
            reply,
          )
        }
      })
    }
  },
)
