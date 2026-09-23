import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'

/**
 * *When* an unknown container subcommand is rejected (#435, #436, #439).
 *
 * Redis 7.0 moved container subcommands (`config|get`, `xgroup|create`, ...)
 * into the command table, so from 7.0 an unknown subcommand fails command
 * lookup: before the command is queued in MULTI, before any key is looked up,
 * and before a script's `redis.call` reaches the container. 6.2 had no such
 * lookup — the container itself rejected the subcommand when it ran, and
 * XGROUP/XINFO looked their key up first.
 *
 * Unlike the rest of this directory the suite is not mock-only: every
 * expectation follows `REDIS_COMPAT`, so it runs against a real server of the
 * matching version too. It is byte for byte against real redis-server 8.0.6
 * (the default profile) and 6.2.24 (`REDIS_COMPAT=redis-6.2` with
 * `TEST_BACKEND=real REDIS_STANDALONE_PORT=<a 6.2 server>`); 7.0.15 and
 * Valkey 7.2 answer every row like 8.0.6.
 *
 * Raw TCP because the exact reply bytes are what is under test and MULTI has to
 * be interleaved command by command, which no typed client method can drive.
 */
const testRunner = new TestRunner()
const profile = activeProfile

/** 7.0+ / every Valkey profile: the subcommand is resolved at lookup time. */
const resolvedAtLookup = profile !== 'redis-6.2'

function unknownSubcommand(container: string, echoed = 'BOGUS'): string {
  return resolvedAtLookup
    ? `-ERR unknown subcommand '${echoed}'. Try ${container} HELP.\r\n`
    : `-ERR Unknown subcommand or wrong number of arguments for '${echoed}'. Try ${container} HELP.\r\n`
}

const EXECABORT =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'
const NO_SUCH_KEY = '-ERR no such key\r\n'
const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const XGROUP_MISSING_KEY =
  '-ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.\r\n'

/** A HELP reply: status lines plus the footer, `Print` from Redis 7.2. */
function helpReply(lines: string[]): string {
  const footer =
    profile === 'redis-6.2' || profile === 'redis-7.0'
      ? '    Prints this help.'
      : '    Print this help.'
  const all = [...lines, 'HELP', footer]
  return `*${all.length}\r\n${all.map(line => `+${line}\r\n`).join('')}`
}

const XINFO_HELP = helpReply([
  'XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
  'CONSUMERS <key> <groupname>',
  '    Show consumers of <groupname>.',
  'GROUPS <key>',
  '    Show the stream consumer groups.',
  'STREAM <key> [FULL [COUNT <count>]',
  '    Show information about the stream.',
])

// 6.2's DESTROY line really runs its description onto the same line.
const XGROUP_HELP = helpReply([
  'XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
  'CREATE <key> <groupname> <id|$> [option]',
  '    Create a new consumer group. Options are:',
  '    * MKSTREAM',
  '      Create the empty stream if it does not exist.',
  ...(resolvedAtLookup
    ? [
        '    * ENTRIESREAD entries_read',
        "      Set the group's entries_read counter (internal use).",
      ]
    : []),
  'CREATECONSUMER <key> <groupname> <consumer>',
  '    Create a new consumer in the specified group.',
  'DELCONSUMER <key> <groupname> <consumer>',
  '    Remove the specified consumer.',
  ...(resolvedAtLookup
    ? [
        'DESTROY <key> <groupname>',
        '    Remove the specified group.',
        'SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]',
        '    Set the current group ID and entries_read counter.',
      ]
    : [
        'DESTROY <key> <groupname>    Remove the specified group.',
        'SETID <key> <groupname> <id|$>',
        '    Set the current group ID.',
      ]),
])

function sha1(script: string): string {
  return createHash('sha1').update(script).digest('hex')
}

describe(`unknown container subcommand dispatch timing (${testRunner.getBackendName()}, ${profile})`, () => {
  let conn: RawRedisConnection
  const stream = `dispatch:{${randomKey()}}:stream`
  const string = `dispatch:{${randomKey()}}:string`
  const missing = `dispatch:{${randomKey()}}:missing`

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
    await send('XADD', stream, '*', 'f', 'v')
    await send('SET', string, 'x')
  })

  after(async () => {
    await send('DEL', stream, string)
    conn.close()
    await testRunner.cleanup()
  })

  async function send(...args: string[]): Promise<string> {
    conn.write(commandFrame(...args))
    return (await conn.readRawFrame()).toString()
  }

  /**
   * MULTI / `args` / EXEC. On 7.0+ the command is refused at queue time and
   * dirties the transaction; on 6.2 it queues and fails inside EXEC.
   */
  async function expectTransaction(
    args: string[],
    error: string,
  ): Promise<void> {
    assert.strictEqual(await send('MULTI'), '+OK\r\n')
    const queued = await send(...args)
    const exec = await send('EXEC')

    if (resolvedAtLookup) {
      assert.strictEqual(queued, error, `${args.join(' ')} at queue time`)
      assert.strictEqual(exec, EXECABORT, `${args.join(' ')} EXEC`)
      return
    }

    assert.strictEqual(queued, '+QUEUED\r\n', `${args.join(' ')} queued`)
    assert.strictEqual(exec, `*1\r\n${error}`, `${args.join(' ')} EXEC`)
  }

  describe('MULTI (#435, #436)', () => {
    test('CONFIG and other execute-time containers', async () => {
      for (const container of ['CONFIG', 'SLOWLOG', 'PUBSUB', 'COMMAND']) {
        await expectTransaction(
          [container, 'BOGUS'],
          unknownSubcommand(container),
        )
      }
    })

    test('the echoed subcommand keeps the casing the client sent', async () => {
      await expectTransaction(
        ['config', 'bogus'],
        unknownSubcommand('CONFIG', 'bogus'),
      )
    })

    test('XGROUP and XINFO', async () => {
      await expectTransaction(
        ['XGROUP', 'BOGUS', stream, 'g'],
        unknownSubcommand('XGROUP'),
      )
      await expectTransaction(
        ['XINFO', 'BOGUS', stream],
        unknownSubcommand('XINFO'),
      )
      await expectTransaction(['XINFO', 'BOGUS'], unknownSubcommand('XINFO'))
    })

    test('a rejected subcommand discards the commands queued before it', async () => {
      const key = `dispatch:{${randomKey()}}:discarded`
      assert.strictEqual(await send('MULTI'), '+OK\r\n')
      assert.strictEqual(await send('SET', key, 'v'), '+QUEUED\r\n')
      await send('CONFIG', 'BOGUS')
      const exec = await send('EXEC')

      assert.strictEqual(
        exec,
        resolvedAtLookup
          ? EXECABORT
          : `*2\r\n+OK\r\n${unknownSubcommand('CONFIG')}`,
      )
      assert.strictEqual(
        await send('GET', key),
        resolvedAtLookup ? '$-1\r\n' : '$1\r\nv\r\n',
      )
      await send('DEL', key)
    })

    test('a known subcommand still queues on every profile', async () => {
      assert.strictEqual(await send('MULTI'), '+OK\r\n')
      assert.strictEqual(
        await send('CONFIG', 'GET', 'no-such-parameter-dispatch'),
        '+QUEUED\r\n',
      )
      assert.strictEqual(await send('EXEC'), '*1\r\n*0\r\n')
    })

    test('a container with no subcommand is still an arity error', async () => {
      assert.strictEqual(
        await send('CONFIG'),
        "-ERR wrong number of arguments for 'config' command\r\n",
      )
    })
  })

  // Real 6.2 looks the key up first whenever one is present and only then
  // rejects the subcommand; 7.0+ rejects the subcommand at lookup and never
  // touches the key (#436).
  describe('key before subcommand on 6.2 (#436)', () => {
    test('XINFO', async () => {
      const cases: [string[], string][] = [
        [['XINFO', 'BOGUS', missing], NO_SUCH_KEY],
        [['XINFO', 'BOGUS', missing, 'x', 'y'], NO_SUCH_KEY],
        [['XINFO', 'BOGUS', string], WRONGTYPE],
        [['XINFO', 'BOGUS', stream], unknownSubcommand('XINFO')],
        [['XINFO', 'BOGUS'], unknownSubcommand('XINFO')],
      ]

      for (const [args, legacy] of cases) {
        assert.strictEqual(
          await send(...args),
          resolvedAtLookup ? unknownSubcommand('XINFO') : legacy,
          args.join(' '),
        )
      }
    })

    test('XGROUP', async () => {
      // 6.2 only looks the key up once a group name is present too.
      const cases: [string[], string][] = [
        [['XGROUP', 'BOGUS', missing, 'g'], XGROUP_MISSING_KEY],
        [['XGROUP', 'BOGUS', missing, 'g', 'x', 'y'], XGROUP_MISSING_KEY],
        [['XGROUP', 'BOGUS', string, 'g'], WRONGTYPE],
        [['XGROUP', 'BOGUS', stream, 'g'], unknownSubcommand('XGROUP')],
        [['XGROUP', 'BOGUS', missing], unknownSubcommand('XGROUP')],
        [['XGROUP', 'BOGUS'], unknownSubcommand('XGROUP')],
      ]

      for (const [args, legacy] of cases) {
        assert.strictEqual(
          await send(...args),
          resolvedAtLookup ? unknownSubcommand('XGROUP') : legacy,
          args.join(' '),
        )
      }
    })
  })

  // From 7.0 `redis.call`/`redis.pcall` resolves `container|subcommand`
  // through the command table, so an unknown subcommand fails lookup exactly
  // like an unknown command and never reaches the container (#439).
  describe('scripts (#439)', () => {
    /** Real Redis 7.0+ and Valkey 7.2; Valkey 8.0+ drops the product name. */
    const UNKNOWN_FROM_SCRIPT = profile.startsWith('valkey-')
      ? 'ERR Unknown command called from script'
      : 'ERR Unknown Redis command called from script'

    /**
     * What a script calling a command that does not exist gets back from
     * redis.pcall. Real 6.2 answers `-@user_script: 1: Unknown Redis command
     * called from Lua script`; this server has the 6.2 wording and no code,
     * but not the `@user_script: 1: ` position, which the Lua engine does not
     * pass to the host (fatal10110/lua-redis-wasm#28, #503).
     */
    async function unknownCommandFromScript(): Promise<string> {
      const reply = await send('EVAL', "return redis.pcall('NOPE')", '0')
      assert.strictEqual(
        reply,
        resolvedAtLookup
          ? `-${UNKNOWN_FROM_SCRIPT}\r\n`
          : '-Unknown Redis command called from Lua script\r\n',
      )
      return reply
    }

    test('redis.pcall with an unknown subcommand', async () => {
      const unknownCommand = await unknownCommandFromScript()
      const cases: [string[], string][] = [
        [['PUBSUB', 'BOGUS'], unknownSubcommand('PUBSUB')],
        [['COMMAND', 'BOGUS'], unknownSubcommand('COMMAND')],
        [['SLOWLOG', 'BOGUS'], unknownSubcommand('SLOWLOG')],
        [['XGROUP', 'BOGUS'], unknownSubcommand('XGROUP')],
        [['XINFO', 'BOGUS'], unknownSubcommand('XINFO')],
        // With a key, 6.2 checks it before the subcommand (#436).
        [['XINFO', 'BOGUS', missing], NO_SUCH_KEY],
        [['XINFO', 'BOGUS', string], WRONGTYPE],
      ]

      for (const [call, legacy] of cases) {
        const args = call.map(arg => `'${arg}'`).join(',')
        assert.strictEqual(
          await send('EVAL', `return redis.pcall(${args})`, '0'),
          resolvedAtLookup ? unknownCommand : legacy,
          call.join(' '),
        )
      }
    })

    // 6.2 refuses a `noscript` container before it looks at the subcommand,
    // so an unknown one is refused exactly like a known one.
    test('a noscript container is refused on 6.2 and fails lookup on 7.0+', async () => {
      const unknownCommand = await unknownCommandFromScript()
      const refused = await send(
        'EVAL',
        "return redis.pcall('CONFIG','GET','maxmemory')",
        '0',
      )
      assert.match(refused, /^-.*not allowed from script/)

      assert.strictEqual(
        await send('EVAL', "return redis.pcall('CONFIG','BOGUS')", '0'),
        resolvedAtLookup ? unknownCommand : refused,
      )
    })

    test('redis.call with an unknown subcommand aborts the script', async () => {
      const script = "return redis.call('PUBSUB','BOGUS')"
      assert.strictEqual(
        await send('EVAL', script, '0'),
        resolvedAtLookup
          ? `-${UNKNOWN_FROM_SCRIPT} script: ${sha1(script)}, on @user_script:1.\r\n`
          : `-ERR Error running script (call to f_${sha1(script)}): @user_script:1: ${unknownSubcommand('PUBSUB').slice(1)}`,
      )
    })

    // A known subcommand passes lookup, so the container's own
    // `addReplySubcommandSyntaxError` still comes back on every profile.
    test('a known subcommand still reaches the container', async () => {
      assert.strictEqual(
        await send(
          'EVAL',
          "return redis.pcall('PUBSUB','CHANNELS','a','b')",
          '0',
        ),
        `-ERR ${resolvedAtLookup ? 'u' : 'U'}nknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.\r\n`,
      )
    })
  })

  // HELP is in the real 7.0+ table, so it passes lookup: there it is a
  // keyless subcommand of arity 2. 6.2 answers XINFO HELP whatever follows it
  // and treats XGROUP HELP with arguments like an unknown subcommand.
  describe('XINFO / XGROUP HELP', () => {
    test('XINFO HELP', async () => {
      const cases: [string[], string][] = [
        [['XINFO', 'HELP'], XINFO_HELP],
        [['XINFO', 'help'], XINFO_HELP],
        [['XINFO', 'HELP', missing], XINFO_HELP],
        [['XINFO', 'HELP', string], XINFO_HELP],
        [['XINFO', 'HELP', stream], XINFO_HELP],
      ]

      for (const [args, legacy] of cases) {
        assert.strictEqual(
          await send(...args),
          resolvedAtLookup && args.length > 2
            ? "-ERR wrong number of arguments for 'xinfo|help' command\r\n"
            : legacy,
          args.join(' '),
        )
      }
    })

    test('XGROUP HELP', async () => {
      const cases: [string[], string][] = [
        [['XGROUP', 'HELP'], XGROUP_HELP],
        [['XGROUP', 'HELP', missing], unknownSubcommand('XGROUP', 'HELP')],
        [['XGROUP', 'HELP', missing, 'g'], XGROUP_MISSING_KEY],
        [['XGROUP', 'HELP', string, 'g'], WRONGTYPE],
        [['XGROUP', 'HELP', stream, 'g'], unknownSubcommand('XGROUP', 'HELP')],
      ]

      for (const [args, legacy] of cases) {
        assert.strictEqual(
          await send(...args),
          resolvedAtLookup && args.length > 2
            ? "-ERR wrong number of arguments for 'xgroup|help' command\r\n"
            : legacy,
          args.join(' '),
        )
      }
    })

    test('HELP queues in MULTI on every profile', async () => {
      assert.strictEqual(await send('MULTI'), '+OK\r\n')
      assert.strictEqual(await send('XINFO', 'HELP'), '+QUEUED\r\n')
      assert.strictEqual(await send('EXEC'), `*1\r\n${XINFO_HELP}`)
    })
  })

  // COMMAND GETKEYS and ACL DRYRUN resolve their target through the same
  // lookup, so from 7.0 an unknown subcommand is an unknown command there too.
  describe('commands that look another command up', () => {
    test('COMMAND GETKEYS', async () => {
      const INVALID = '-ERR Invalid command specified\r\n'
      const NO_KEYS = '-ERR The command has no key arguments\r\n'
      const oneKey = (key: string): string =>
        `*1\r\n$${key.length}\r\n${key}\r\n`
      const cases: [string[], string, string][] = [
        [['CONFIG', 'BOGUS'], INVALID, NO_KEYS],
        [['XINFO', 'BOGUS', 'k'], INVALID, oneKey('k')],
        [['XGROUP', 'BOGUS', 'k', 'g'], INVALID, oneKey('k')],
        [['XINFO', 'HELP', 'k'], NO_KEYS, oneKey('k')],
        [['XGROUP', 'HELP', 'k', 'g'], NO_KEYS, oneKey('k')],
      ]

      for (const [args, current, legacy] of cases) {
        assert.strictEqual(
          await send('COMMAND', 'GETKEYS', ...args),
          resolvedAtLookup ? current : legacy,
          `COMMAND GETKEYS ${args.join(' ')}`,
        )
      }
    })

    test(
      'COMMAND GETKEYSANDFLAGS',
      { skip: !resolvedAtLookup && 'no GETKEYSANDFLAGS before 7.0' },
      async () => {
        for (const args of [
          ['CONFIG', 'BOGUS'],
          ['XINFO', 'BOGUS', 'k'],
        ]) {
          assert.strictEqual(
            await send('COMMAND', 'GETKEYSANDFLAGS', ...args),
            '-ERR Invalid command specified\r\n',
            args.join(' '),
          )
        }
      },
    )

    test(
      'ACL DRYRUN',
      { skip: !resolvedAtLookup && 'no ACL DRYRUN before 7.0' },
      async () => {
        const cases: [string[], string][] = [
          [['CONFIG', 'BOGUS'], 'CONFIG'],
          [['config', 'bogus'], 'config'],
          [['XINFO', 'BOGUS', 'k'], 'XINFO'],
        ]
        for (const [args, echoed] of cases) {
          assert.strictEqual(
            await send('ACL', 'DRYRUN', 'default', ...args),
            `-ERR Command '${echoed}' not found\r\n`,
            args.join(' '),
          )
        }
      },
    )
  })
})
