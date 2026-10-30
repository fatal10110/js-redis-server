import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'

/**
 * A known container subcommand given the wrong number of arguments (#437).
 *
 * Redis 7.0+ (every Valkey profile) checks it against the subcommand's own
 * command-table entry: `wrong number of arguments for '<container>|<sub>'`.
 * 6.2 has no such entry; the container answers
 * `addReplySubcommandSyntaxError` itself, echoing the subcommand as typed:
 * `Unknown subcommand or wrong number of arguments for '<SUB>'. Try <C> HELP.`
 *
 * Every expectation follows `REDIS_COMPAT`, so the file runs against a real
 * server of each profile too. Raw TCP: the exact error bytes are under test.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const tableArity = profile !== 'redis-6.2'

const EXECABORT =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'

function syntaxError(container: string, typed: string): string {
  const lead = tableArity ? 'unknown' : 'Unknown'
  return `-ERR ${lead} subcommand or wrong number of arguments for '${typed}'. Try ${container} HELP.\r\n`
}

/** The 7.0+ arity error, or 6.2's subcommand syntax error. */
function arityError(container: string, typed: string): string {
  return tableArity
    ? `-ERR wrong number of arguments for '${container.toLowerCase()}|${typed.toLowerCase()}' command\r\n`
    : syntaxError(container, typed)
}

// [container, subcommand as typed, ...arguments]
const ROWS: string[][] = [
  ['CONFIG', 'GET'],
  ['CONFIG', 'resetstat', 'x'],
  ['CONFIG', 'REWRITE', 'x'],
  ['CONFIG', 'HELP', 'x'],
  ['SCRIPT', 'LOAD'],
  ['SCRIPT', 'KILL', 'x'],
  ['SCRIPT', 'DEBUG'],
  ['SCRIPT', 'HELP', 'x'],
  ['ACL', 'WHOAMI', 'x'],
  ['ACL', 'HELP', 'x'],
  ['SLOWLOG', 'LEN', 'x'],
  ['SLOWLOG', 'reset', 'x'],
  ['SLOWLOG', 'HELP', 'x'],
  ['CLIENT', 'GETNAME', 'x'],
  ['CLIENT', 'Id', 'x'],
  ['CLIENT', 'SETNAME'],
  ['CLIENT', 'INFO', 'x'],
  ['CLIENT', 'HELP', 'x'],
  ['PUBSUB', 'NUMPAT', 'x'],
  ['PUBSUB', 'HELP', 'x'],
]

describe(`container subcommand arity (${testRunner.getBackendName()}, ${profile})`, () => {
  let conn: RawRedisConnection

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
  })

  after(async () => {
    conn.close()
    await testRunner.cleanup()
  })

  async function send(...args: string[]): Promise<string> {
    conn.write(commandFrame(...args))
    return (await conn.readRawFrame()).toString()
  }

  for (const [container, subcommand, ...rest] of ROWS) {
    test(`${container} ${subcommand} ${rest.join(' ')}`.trimEnd(), async () => {
      assert.strictEqual(
        await send(container, subcommand, ...rest),
        arityError(container, subcommand),
      )
    })
  }

  test('SCRIPT EXISTS without a sha: an empty array on 6.2', async () => {
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS'),
      tableArity ? arityError('SCRIPT', 'EXISTS') : '*0\r\n',
    )
  })

  // `slowlog|get` is -2 in the table, so lookup lets any count through and
  // SLOWLOG GET polices it itself, on every profile.
  test('SLOWLOG GET with two arguments is the syntax error', async () => {
    assert.strictEqual(
      await send('SLOWLOG', 'GET', 'a', 'b'),
      syntaxError('SLOWLOG', 'GET'),
    )
  })

  // FUNCTION is 7.0+. Its option parsing, from functions.c.
  test(
    'FUNCTION argument errors',
    { skip: !tableArity && 'no FUNCTION on 6.2' },
    async () => {
      for (const [args, reply] of [
        [['LOAD', 'a', 'b'], '-ERR Unknown option given: a\r\n'],
        [
          ['FLUSH', 'a'],
          '-ERR FUNCTION FLUSH only supports SYNC|ASYNC option\r\n',
        ],
        [['FLUSH', 'a', 'b'], syntaxError('FUNCTION', 'FLUSH')],
        [['RESTORE', 'a', 'b', 'c'], syntaxError('FUNCTION', 'RESTORE')],
        [['LIST', 'a'], '-ERR Unknown argument a\r\n'],
        [
          ['LIST', 'LIBRARYNAME'],
          '-ERR library name argument was not given\r\n',
        ],
        [
          ['LIST', 'WITHCODE', 'withcode'],
          '-ERR Unknown argument withcode\r\n',
        ],
        [
          ['LIST', 'libraryname', 'a', 'LIBRARYNAME', 'b'],
          '-ERR Unknown argument LIBRARYNAME\r\n',
        ],
      ] as const) {
        assert.strictEqual(
          await send('FUNCTION', ...args),
          reply,
          args.join(' '),
        )
      }
    },
  )

  test('6.2 queues the call in MULTI and fails it in EXEC', async () => {
    const error = arityError('CONFIG', 'RESETSTAT')
    assert.strictEqual(await send('MULTI'), '+OK\r\n')
    const queued = await send('CONFIG', 'RESETSTAT', 'x')
    const exec = await send('EXEC')
    if (tableArity) {
      assert.strictEqual(queued, error)
      assert.strictEqual(exec, EXECABORT)
    } else {
      assert.strictEqual(queued, '+QUEUED\r\n')
      assert.strictEqual(exec, `*1\r\n${error}`)
    }
  })

  test('a script pcall gets the same 6.2 reply', async () => {
    const reply = await send(
      'EVAL',
      "return redis.pcall('SLOWLOG', 'LEN', 'x')",
      '0',
    )
    if (tableArity) {
      assert.match(reply, /^-ERR Wrong number of args calling /)
    } else {
      assert.strictEqual(reply, syntaxError('SLOWLOG', 'LEN'))
    }
  })
})
