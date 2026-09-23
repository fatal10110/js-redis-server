import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'

import { TestRunner } from '../test-config'
import {
  activeProfile,
  commandFrame,
  randomKey,
  type ProfileName,
} from '../utils'
import {
  RawRedisConnection,
  respMapGet,
  respNumber,
  type RespWireValue,
} from '../raw-tcp/raw-connection'

const testRunner = new TestRunner()
const profile = activeProfile
const expectedVersion: Record<ProfileName, string> = {
  'redis-6.2': '6.2.14',
  'redis-7.0': '7.0.15',
  'redis-7.2': '7.2.4',
  'redis-7.4': '7.4.4',
  'redis-8.0': '8.0.0',
  'valkey-8.0': '8.0.0',
  'valkey-9.0': '9.0.0',
}

const redis62RootCommands = [
  'GETEX',
  'GETDEL',
  'COPY',
  'HRANDFIELD',
  'LMOVE',
  'BLMOVE',
  'RESET',
  'SMISMEMBER',
  'XAUTOCLAIM',
  'ZMSCORE',
]

const redis70RootCommands = [
  'EXPIRETIME',
  'PEXPIRETIME',
  'LMPOP',
  'BLMPOP',
  'SINTERCARD',
  'SSUBSCRIBE',
  'SUNSUBSCRIBE',
  'SPUBLISH',
  'SORT_RO',
  'ZINTERCARD',
  'ZMPOP',
  'BZMPOP',
]

const hashFieldExpirationCommands = [
  'HEXPIRE',
  'HPEXPIRE',
  'HEXPIREAT',
  'HPEXPIREAT',
  'HPERSIST',
  'HTTL',
  'HPTTL',
]

describe(
  `compatibility profile integration (${testRunner.getBackendName()}, ${profile})`,
  { skip: testRunner.backend === 'real' && 'profiles are mock-only' },
  () => {
    let connection: RawRedisConnection

    before(async () => {
      const port = await testRunner.setupRawStandalone()
      connection = await RawRedisConnection.connect('127.0.0.1', port)
    })

    after(async () => {
      connection.close()
      await testRunner.cleanup()
    })

    test('reports the selected profile over INFO and HELLO', async () => {
      const info = await send('INFO', 'server')
      if (profile.startsWith('valkey-')) {
        assert.match(info, /\r\nserver_name:valkey\r\n/)
        assert.match(
          info,
          new RegExp(
            `\\r\\nvalkey_version:${escapeRegExp(expectedVersion[profile])}\\r\\n`,
          ),
        )
      } else {
        assert.match(
          info,
          new RegExp(
            `\\r\\nredis_version:${escapeRegExp(expectedVersion[profile])}\\r\\n`,
          ),
        )
      }

      const hello = await send('HELLO', '2')
      assert.match(hello, bulkStringFrame(expectedVersion[profile]))
    })

    test('applies root command availability gates', async () => {
      for (const command of redis62RootCommands) {
        await expectRootCommand(command, true)
      }

      for (const command of redis70RootCommands) {
        await expectRootCommand(command, supportsRedis70Commands())
      }

      for (const command of hashFieldExpirationCommands) {
        await expectRootCommand(command, supportsHashFieldExpiration())
      }

      await expectRootCommand('HGETEX', supportsHgetex())
      await expectRootCommand('HGETDEL', supportsHgetdel())
      await expectRootCommand('HSETEX', supportsHsetex())
    })

    test('applies parser, subcommand, and behavior gates', async () => {
      const key = `compat:${profile}:key`
      const hash = `compat:${profile}:hash`
      const stream = `compat:${profile}:stream`

      await send('SET', key, 'v')
      await expectGate(supportsExpireConditions(), 'EXPIRE', key, '10', 'NX')
      await expectGate(supportsRedis70Commands(), 'INFO', 'server', 'clients')
      await expectGate(true, 'SET', key, 'next', 'GET')
      await expectGate(supportsSetNxGet(), 'SET', key, 'guarded', 'NX', 'GET')
      await expectGate(true, 'SET', key, 'expires', 'EXAT', '4102444800')
      await expectGate(true, 'SLOWLOG', 'GET', '-1')

      // BITCOUNT/BITPOS BYTE|BIT range modifier is Redis 7.0+ (Valkey 7.2+).
      await expectGate(
        supportsBitByteBitRange(),
        'BITCOUNT',
        key,
        '0',
        '0',
        'BYTE',
      )
      await expectGate(
        supportsBitByteBitRange(),
        'BITPOS',
        key,
        '1',
        '0',
        '-1',
        'BIT',
      )

      await expectGate(supportsCommandDocs(), 'COMMAND', 'DOCS')
      await expectGate(
        supportsCommandDocs(),
        'COMMAND',
        'GETKEYSANDFLAGS',
        'GET',
        key,
      )
      await expectGate(
        supportsClientSetinfo(),
        'CLIENT',
        'SETINFO',
        'lib-name',
        'compat',
      )
      await expectGate(
        supportsClientKillMaxAge(),
        'CLIENT',
        'KILL',
        'MAXAGE',
        '999999',
      )
      await expectGate(supportsRedis70Commands(), 'CLIENT', 'NO-EVICT', 'ON')
      await expectGate(supportsShardedPubSub(), 'PUBSUB', 'SHARDCHANNELS')
      await expectGate(supportsShardedPubSub(), 'PUBSUB', 'SHARDNUMSUB')
      await expectGate(
        supportsShardedPubSub(),
        'SPUBLISH',
        `compat:{${profile}}`,
        'message',
      )
      await expectGate(
        supportsRedis70Commands(),
        'ACL',
        'DRYRUN',
        'default',
        'PING',
      )
      await expectGate(supportsRedis70Commands(), 'EVAL_RO', 'return 1', '0')
      if (supportsRedis70Commands()) {
        await expectGate(
          true,
          'FUNCTION',
          'LOAD',
          '#!lua name=compatlib\nredis.register_function{function_name="compat_echo", callback=function(keys, args) return args[1] end, flags={"no-writes"}}',
        )
        await expectGate(true, 'FCALL', 'compat_echo', '0', 'hello')
        await expectGate(true, 'FCALL_RO', 'compat_echo', '0', 'hello')
      } else {
        await expectGate(false, 'FUNCTION', 'HELP')
        await expectGate(false, 'FCALL', 'missing', '0')
      }
      await expectGate(
        supportsZintercard(),
        'ZINTERCARD',
        '1',
        `compat:${profile}:zset`,
      )

      await send('XADD', stream, '1-1', 'field', 'value')
      await expectGate(supportsXreadPlusId(), 'XREAD', 'STREAMS', stream, '+')

      await send('HSET', hash, 'field', 'value', 'delete-me', 'gone')
      await expectGate(supportsHscanNoValues(), 'HSCAN', hash, '0', 'NOVALUES')
      await expectGate(
        supportsHashFieldExpiration(),
        'HEXPIRE',
        hash,
        '10',
        'FIELDS',
        '1',
        'field',
      )
      await expectGate(
        supportsHashFieldExpiration(),
        'HTTL',
        hash,
        'FIELDS',
        '1',
        'field',
      )
      await expectGate(supportsHgetex(), 'HGETEX', hash, 'FIELDS', '1', 'field')
      await expectGate(
        supportsHsetex(),
        'HSETEX',
        hash,
        'FIELDS',
        '1',
        'field',
        'updated',
      )
      await expectGate(
        supportsHgetdel(),
        'HGETDEL',
        hash,
        'FIELDS',
        '1',
        'delete-me',
      )
    })

    test('CONFIG SET failure wording and overflow handling match the profile', async () => {
      // Redis 7.0 rewrote CONFIG SET, changing the failure prefix; 6.2 also
      // saturates an over-maximum memory value where 7.0+ rejects it.
      const tooSmall = await send('CONFIG', 'SET', 'proto-max-bulk-len', '100')
      const notMemory = await send('CONFIG', 'SET', 'proto-max-bulk-len', 'abc')
      const range =
        'argument must be between 1048576 and 9223372036854775807 inclusive'

      if (supportsConfigSetFailureWording()) {
        assert.strictEqual(
          tooSmall,
          `-ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - ${range}\r\n`,
        )
        assert.strictEqual(
          notMemory,
          "-ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a memory value\r\n",
        )
      } else {
        assert.strictEqual(
          tooSmall,
          `-ERR Invalid argument '100' for CONFIG SET 'proto-max-bulk-len' - ${range}\r\n`,
        )
        assert.strictEqual(
          notMemory,
          "-ERR Invalid argument 'abc' for CONFIG SET 'proto-max-bulk-len' - argument must be a memory value\r\n",
        )
      }

      // A bare literal over int64 max: 7.0+ rejects it, 6.2's strtoll parse
      // saturates and the value is accepted. A unit multiplier is deliberately
      // absent — with one, 6.2 errors too.
      const overflow = await send(
        'CONFIG',
        'SET',
        'proto-max-bulk-len',
        '99999999999999999999',
      )
      if (supportsMemoryValueOverflowRejection()) {
        assert.strictEqual(
          overflow,
          `-ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - ${range}\r\n`,
        )
        return
      }

      // Below the gate the SET succeeded, so the limit is now server-wide at
      // int64 max. Restore it even if the readback assertion fails, or every
      // later test in this file inherits it.
      try {
        assert.strictEqual(overflow, '+OK\r\n')
        const reply = await send('CONFIG', 'GET', 'proto-max-bulk-len')
        assert.match(reply, /9223372036854775807/)

        // Saturation is limited to the bare literal. Once a unit multiplier
        // pushes the product over the maximum, real 6.2 errors as well, so the
        // gate must not swallow these.
        //
        // Only the `-ERR Invalid argument ` prefix is asserted, deliberately:
        // which *detail* follows is a known, documented divergence. Real 6.2
        // computes the product in 64 bits and reports `argument must be a
        // memory value` when it wraps negative, where exact arithmetic here
        // reports the range error — `10000000000g` is such a row, while
        // `17179869184gb` wraps to zero and matches. See parseMemoryValue's
        // docblock. Tightening this to the full message would fail.
        for (const value of ['10000000000g', '17179869184gb']) {
          assert.match(
            await send('CONFIG', 'SET', 'proto-max-bulk-len', value),
            /^-ERR Invalid argument /,
            `CONFIG SET proto-max-bulk-len ${value} on ${profile}`,
          )
        }
      } finally {
        await send('CONFIG', 'SET', 'proto-max-bulk-len', '536870912')
      }
    })

    // The `n` (new-key) class is Redis 7.0+. Real 6.2.14/6.2.24 reject `KEn`
    // through the bare `badfmt` wording (but accept `m` and `d`); 7.0.15,
    // 8.0.x and valkey 8.0/9.0 accept it.
    test('the n notify-keyspace-events class matches the profile', async () => {
      try {
        const reply = await send(
          'CONFIG',
          'SET',
          'notify-keyspace-events',
          'KEn',
        )
        if (supportsNewKeyNotifyClass()) {
          assert.strictEqual(reply, '+OK\r\n')
          assert.strictEqual(
            await send('CONFIG', 'GET', 'notify-keyspace-events'),
            '*2\r\n$22\r\nnotify-keyspace-events\r\n$3\r\nnKE\r\n',
          )
        } else {
          assert.strictEqual(
            reply,
            "-ERR Invalid argument 'KEn' for CONFIG SET 'notify-keyspace-events'\r\n",
          )
        }

        // `m` and `d` exist on every profile.
        assert.strictEqual(
          await send('CONFIG', 'SET', 'notify-keyspace-events', 'KEmd'),
          '+OK\r\n',
        )
      } finally {
        await send('CONFIG', 'SET', 'notify-keyspace-events', '')
      }
    })

    // Invalid-value and unknown-parameter CONFIG SET failures share the one
    // gated template (#416), not only proto-max-bulk-len's. Captured from real
    // 6.2.14 / 7.0.15 / 8.0.0 / valkey 8.0.0 / valkey 9.0.0:
    // - notify-keyspace-events is hand-parsed in 6.2 (`goto badfmt`), so its
    //   6.2 reply carries no ` - <detail>` suffix at all;
    // - 6.2 echoes the parameter name as the client sent it, 7.0+ echoes it
    //   lower-cased;
    // - an unknown parameter has its own 6.2 wording.
    test('every CONFIG SET failure uses the profile wording', async () => {
      const badNotify = await send(
        'CONFIG',
        'SET',
        'Notify-Keyspace-Events',
        'Xz',
      )
      const badMemory = await send('CONFIG', 'SET', 'Proto-Max-Bulk-Len', 'abc')
      const unknown = await send('CONFIG', 'SET', 'Bogus-Param', '1')

      if (supportsConfigSetFailureWording()) {
        assert.strictEqual(
          badNotify,
          "-ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - Invalid event class character. Use 'Ag$lshzxeKEtmdn'.\r\n",
        )
        assert.strictEqual(
          badMemory,
          "-ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a memory value\r\n",
        )
        assert.strictEqual(
          unknown,
          "-ERR Unknown option or number of arguments for CONFIG SET - 'Bogus-Param'\r\n",
        )
        return
      }

      assert.strictEqual(
        badNotify,
        "-ERR Invalid argument 'Xz' for CONFIG SET 'Notify-Keyspace-Events'\r\n",
      )
      assert.strictEqual(
        badMemory,
        "-ERR Invalid argument 'abc' for CONFIG SET 'Proto-Max-Bulk-Len' - argument must be a memory value\r\n",
      )
      assert.strictEqual(
        unknown,
        '-ERR Unsupported CONFIG parameter: Bogus-Param\r\n',
      )
    })

    // `config.set.multi-pair` (#419). Redis 7.0 rewrote CONFIG SET to accept
    // several pairs, splitting its arity errors and adding duplicate
    // detection. Real 6.2 dispatches SET only for exactly one pair
    // (`c->argc == 4`) and answers every other shape — including a repeated
    // parameter — with the legacy subcommand syntax error, echoing the
    // subcommand as sent. Captured from real redis-server 6.2.24; the 7.0+
    // replies are pinned against real 7.0.15 / 8.0.6 / Valkey 7.2.14 by
    // tests-integration/raw-tcp/command-errors-config.test.ts.
    test('CONFIG SET multi-pair form and duplicate detection match the profile', async () => {
      assert.strictEqual(await send('CONFIG', 'SET', 'timeout', '0'), '+OK\r\n')

      const multi = await send(
        'CONFIG',
        'SET',
        'timeout',
        '0',
        'maxmemory',
        '0',
      )
      const lowerCase = await send(
        'config',
        'set',
        'timeout',
        '0',
        'maxmemory',
        '0',
      )
      const mixedCase = await send(
        'config',
        'SeT',
        'timeout',
        '0',
        'maxmemory',
        '0',
      )
      const none = await send('CONFIG', 'SET')
      const nameOnly = await send('CONFIG', 'SET', 'timeout')
      const dangling = await send('CONFIG', 'SET', 'timeout', '0', 'maxmemory')
      const repeated = await send(
        'CONFIG',
        'SET',
        'timeout',
        '0',
        'timeout',
        '0',
      )

      if (supportsConfigSetMultiPair()) {
        assert.strictEqual(multi, '+OK\r\n')
        assert.strictEqual(lowerCase, '+OK\r\n')
        assert.strictEqual(mixedCase, '+OK\r\n')
        const arity =
          "-ERR wrong number of arguments for 'config|set' command\r\n"
        assert.strictEqual(none, arity)
        assert.strictEqual(nameOnly, arity)
        assert.strictEqual(dangling, '-ERR syntax error\r\n')
        assert.strictEqual(
          repeated,
          "-ERR CONFIG SET failed (possibly related to argument 'timeout') - duplicate parameter\r\n",
        )
        return
      }

      const legacy = (subcommand: string): string =>
        `-ERR Unknown subcommand or wrong number of arguments for '${subcommand}'. Try CONFIG HELP.\r\n`
      assert.strictEqual(multi, legacy('SET'))
      assert.strictEqual(lowerCase, legacy('set'))
      assert.strictEqual(mixedCase, legacy('SeT'))
      assert.strictEqual(none, legacy('SET'))
      assert.strictEqual(nameOnly, legacy('SET'))
      assert.strictEqual(dangling, legacy('SET'))
      assert.strictEqual(repeated, legacy('SET'))
    })

    // Redis 7.0 moved container commands into the command table, replacing the
    // 6.2 unknown-subcommand template and adding `%.128s` truncation of the
    // echoed name. Captured from real redis-server 6.2.24, 7.0.15 and 8.0.6.
    // Valkey forked at 7.2, so every Valkey profile gets the newer wording.
    //
    // Every container shares the template, so the sweep covers several of them
    // rather than only CONFIG: before #413 each had its own copy and they had
    // drifted apart (XGROUP still emitted the 6.2 text on every profile).
    test('unknown-subcommand wording matches the profile in every container', async () => {
      for (const container of ['CONFIG', 'XGROUP', 'CLIENT', 'ACL', 'SCRIPT']) {
        const reply = await send(container, 'BOGUS')

        assert.strictEqual(
          reply,
          supportsUnknownSubcommandWording()
            ? `-ERR unknown subcommand 'BOGUS'. Try ${container} HELP.\r\n`
            : `-ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try ${container} HELP.\r\n`,
          `${container} BOGUS on ${profile}`,
        )
      }
    })

    test('the echoed unknown subcommand is truncated only from Redis 7.0', async () => {
      for (const container of ['CONFIG', 'XGROUP']) {
        const reply = await send(container, 'X'.repeat(300))

        // 7.0+ formats it with `%.128s`; 6.2 echoes all 300 bytes.
        const expectedEcho = supportsUnknownSubcommandWording() ? 128 : 300
        const echoed = /'(X+)'/.exec(reply)
        assert.ok(echoed, `expected an echoed subcommand, got ${reply}`)
        assert.strictEqual(echoed[1].length, expectedEcho, container)
      }
    })

    // The echo stops at the first NUL on every profile — both templates render
    // it with a `%s`-family conversion over a C string. On 6.2 that is the only
    // truncation there is, which is why it cannot ride along with the length
    // cut. Captured from 6.2.24 and 8.0.6 as `CONFIG 'A'*200 + \0 + 'A'*200`:
    // 6.2 echoes 200, 8.0 echoes 128 (NUL cut first, then `%.128s`).
    test('the echoed subcommand stops at the first NUL on every profile', async () => {
      const short = await send('CONFIG', 'AA\0BB')
      assert.ok(
        short.includes("'AA'. Try CONFIG HELP."),
        `expected the echo to stop at the NUL, got ${JSON.stringify(short)}`,
      )

      const long = await send(
        'CONFIG',
        `${'A'.repeat(200)}\0${'A'.repeat(200)}`,
      )
      const echoed = /'(A*)'/.exec(long)
      assert.ok(echoed, `expected an echoed subcommand, got ${long}`)
      assert.strictEqual(
        echoed[1].length,
        supportsUnknownSubcommandWording() ? 128 : 200,
      )
    })

    // The unknown-command reply flipped at 7.0 as well (#384): 6.2 quotes with
    // backticks, separates args with `, ` and echoes the whole name; 7.0+
    // quotes with single quotes, separates with a space and cuts the name at
    // 128 bytes. Captured from real redis-server 6.2.24, 7.0 and 8.0.6, and
    // Valkey 7.2 / 8.0. The full byte-level coverage (NUL cuts, the 128-byte
    // args budget) is tests-integration/raw-tcp/unknown-command.test.ts.
    test('unknown-command wording matches the profile', async () => {
      const legacy = profile === 'redis-6.2'
      assert.strictEqual(
        await send('NOSUCHCMD', 'a', 'b'),
        legacy
          ? '-ERR unknown command `NOSUCHCMD`, with args beginning with: `a`, `b`, \r\n'
          : "-ERR unknown command 'NOSUCHCMD', with args beginning with: 'a' 'b' \r\n",
      )

      const name = 'X'.repeat(200)
      assert.strictEqual(
        await send(name),
        legacy
          ? `-ERR unknown command \`${name}\`, with args beginning with: \r\n`
          : `-ERR unknown command '${name.slice(0, 128)}', with args beginning with: \r\n`,
      )
    })

    // `addReplySubcommandSyntaxError` — a *known* subcommand given arguments it
    // cannot use — flipped case at 7.0 too, but kept the `or wrong number of
    // arguments` clause and gained no truncation. Captured from 6.2.24 and
    // 8.0.6 as `PUBSUB CHANNELS a b`.
    test('the subcommand syntax-error wording matches the profile', async () => {
      const reply = await send('PUBSUB', 'CHANNELS', 'a', 'b')

      assert.strictEqual(
        reply,
        supportsUnknownSubcommandWording()
          ? "-ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.\r\n"
          : "-ERR Unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.\r\n",
      )
    })

    // The echoed name is raw client bytes on every profile — real 6.2.24 and
    // 8.0.6 both answer `CONFIG \xff\xfe\xfd` with those three bytes, only the
    // surrounding template differs.
    test('a non-UTF-8 subcommand is echoed byte for byte on every profile', async () => {
      const subcommand = Buffer.from([0xff, 0xfe, 0xfd])
      connection.write(commandFrame('CONFIG', subcommand))
      const reply = await connection.readRawFrame()

      const lead = supportsUnknownSubcommandWording()
        ? 'unknown subcommand'
        : 'Unknown subcommand or wrong number of arguments for'
      assert.deepStrictEqual(
        [...reply],
        [
          ...Buffer.concat([
            Buffer.from(`-ERR ${lead} '`),
            subcommand,
            Buffer.from("'. Try CONFIG HELP.\r\n"),
          ]),
        ],
        `got ${JSON.stringify(reply.toString('latin1'))}`,
      )
    })

    // Before 7.0 a script's `redis.call`/`redis.pcall` runs the container like
    // any client would, so an unknown subcommand comes back as the container's
    // own reply. From 7.0 script command lookup resolves container+subcommand
    // and fails first (`ERR Unknown Redis command called from script`, #439),
    // which is why this runs on `redis-6.2` only. Byte for byte against real
    // 6.2.24. XINFO is called without a key on purpose: with one, 6.2 checks
    // the key before the subcommand (`ERR no such key` / `WRONGTYPE`, #436).
    test(
      'a nested unknown subcommand keeps raw bytes through redis.pcall',
      {
        skip: supportsUnknownSubcommandWording() && 'redis-6.2 only, see above',
      },
      async () => {
        const subcommand = Buffer.from([0xff, 0xfe, 0xfd])

        for (const container of ['PUBSUB', 'XGROUP', 'XINFO']) {
          connection.write(
            commandFrame(
              'EVAL',
              `return redis.pcall('${container}', ARGV[1])`,
              '0',
              subcommand,
            ),
          )
          const reply = await connection.readRawFrame()
          assert.deepStrictEqual(
            [...reply],
            [
              ...Buffer.concat([
                Buffer.from(
                  "-ERR Unknown subcommand or wrong number of arguments for '",
                ),
                subcommand,
                Buffer.from(`'. Try ${container} HELP.\r\n`),
              ]),
            ],
            `${container}: got ${JSON.stringify(reply.toString('latin1'))}`,
          )
        }
      },
    )

    // The same error through a failing `redis.call`, which additionally takes
    // the pre-7.0 script-abort decoration (#442): a prefix naming the script's
    // `f_<sha>` function, with the command's error code folded into the body.
    // Byte for byte against real 6.2.24.
    test(
      'a nested unknown subcommand keeps raw bytes through redis.call',
      {
        skip: supportsUnknownSubcommandWording() && 'redis-6.2 only, see above',
      },
      async () => {
        const script = "return redis.call('PUBSUB', ARGV[1])"
        const subcommand = Buffer.from([0xff, 0xfe, 0xfd])
        connection.write(commandFrame('EVAL', script, '0', subcommand))
        const reply = await connection.readRawFrame()

        // latin1 maps each byte to one code unit, so this is a byte-exact
        // comparison with a readable diff.
        assert.strictEqual(
          reply.toString('latin1'),
          Buffer.concat([
            Buffer.from(
              `-ERR Error running script (call to f_${sha1(script)}): @user_script:1: ERR Unknown subcommand or wrong number of arguments for '`,
            ),
            subcommand,
            Buffer.from("'. Try PUBSUB HELP.\r\n"),
          ]).toString('latin1'),
        )
      },
    )

    // Redis 7.0 (Valkey 7.2) moved the script-abort decoration from a prefix,
    // `Error running script (call to f_<sha>): @user_script:<line>: <error>`,
    // to a suffix, `<error> script: <sha>, on @user_script:<line>.`. Under the
    // prefix form the whole reply is `-ERR`, a failing command's own code
    // (`WRONGTYPE`) becomes part of the body, and a Lua runtime error shows the
    // position twice. Every frame below is byte for byte against real 6.2.24,
    // 7.0.15, 8.0 and Valkey 7.2 / 8.0.
    test('script abort errors take the profile decoration', async () => {
      const key = `compat:${profile}:script-abort:${randomKey()}`
      const cases: Array<{
        args: Array<string | Buffer>
        legacy: (sha: string) => Buffer
        current: (sha: string) => Buffer
      }> = [
        {
          // Lua runtime error carrying raw client bytes.
          args: ['error(ARGV[1])', '0', Buffer.from([0x78, 0xff])],
          legacy: sha =>
            Buffer.concat([
              Buffer.from(
                `-ERR Error running script (call to f_${sha}): @user_script:1: user_script:1: x`,
              ),
              Buffer.from([0xff, 0x0d, 0x0a]),
            ]),
          current: sha =>
            Buffer.concat([
              Buffer.from('-ERR user_script:1: x'),
              Buffer.from([0xff]),
              Buffer.from(` script: ${sha}, on @user_script:1.\r\n`),
            ]),
        },
        {
          // A failing redis.call keeps its own error code on 7.0+ only.
          args: [
            "redis.call('SET', KEYS[1], 'v')\nreturn redis.call('LPUSH', KEYS[1], 'v')",
            '1',
            key,
          ],
          legacy: sha =>
            Buffer.from(
              `-ERR Error running script (call to f_${sha}): @user_script:2: WRONGTYPE Operation against a key holding the wrong kind of value\r\n`,
            ),
          current: sha =>
            Buffer.from(
              `-WRONGTYPE Operation against a key holding the wrong kind of value script: ${sha}, on @user_script:2.\r\n`,
            ),
        },
        {
          // A redis.call error caught by pcall and re-raised with `error(e)`
          // (level 1) is a runtime error: Lua prefixes its own position, and
          // the code stays inside the message. See the level-0 case below.
          args: [
            "local ok, e = pcall(redis.call, 'LPUSH', KEYS[1], 'v')\nerror(e)",
            '1',
            key,
          ],
          legacy: sha =>
            Buffer.from(
              `-ERR Error running script (call to f_${sha}): @user_script:2: user_script:2: WRONGTYPE Operation against a key holding the wrong kind of value\r\n`,
            ),
          current: sha =>
            Buffer.from(
              `-ERR user_script:2: WRONGTYPE Operation against a key holding the wrong kind of value script: ${sha}, on @user_script:2.\r\n`,
            ),
        },
        {
          args: ["local function f()\n  error('deep')\nend\nf()", '0'],
          legacy: sha =>
            Buffer.from(
              `-ERR Error running script (call to f_${sha}): @user_script:2: user_script:2: deep\r\n`,
            ),
          current: sha =>
            Buffer.from(
              `-ERR user_script:2: deep script: ${sha}, on @user_script:2.\r\n`,
            ),
        },
        {
          args: ['return a', '0'],
          legacy: sha =>
            Buffer.from(
              `-ERR Error running script (call to f_${sha}): @user_script:1: user_script:1: Script attempted to access nonexistent global variable 'a'\r\n`,
            ),
          current: sha =>
            Buffer.from(
              `-ERR user_script:1: Script attempted to access nonexistent global variable 'a' script: ${sha}, on @user_script:1.\r\n`,
            ),
        },
      ]

      try {
        for (const { args, legacy, current } of cases) {
          const script = String(args[0])
          connection.write(commandFrame('EVAL', ...args))
          const reply = await connection.readRawFrame()
          const expected = supportsSuffixScriptErrorDecoration()
            ? current(sha1(script))
            : legacy(sha1(script))
          assert.strictEqual(
            reply.toString('latin1'),
            expected.toString('latin1'),
            script,
          )
        }
      } finally {
        await send('DEL', key)
      }
    })

    // `error(e, 0)` re-raises a pcall-caught redis.call error with no position
    // added, so on 6.2 the frame is the same as the uncaught call's. Byte for
    // byte against real 6.2.24. Pinned on 6.2 only: 7.0+ real Redis answers
    // `-ERR WRONGTYPE ...`, but the engine splits the leading code off a Lua
    // error string, so this server answers `-WRONGTYPE ...` there — a separate
    // engine-side divergence (the same one as `error('WRONGTYPE x', 0)`).
    test(
      'a pcall-caught redis.call error re-raised at level 0 keeps the 6.2 frame',
      {
        skip:
          supportsSuffixScriptErrorDecoration() && 'redis-6.2 only, see above',
      },
      async () => {
        const key = `compat:${profile}:script-abort-l0:${randomKey()}`
        const script =
          "local ok, e = pcall(redis.call, 'LPUSH', KEYS[1], 'v')\nerror(e, 0)"
        try {
          await send('SET', key, 'v')
          assert.strictEqual(
            await send('EVAL', script, '1', key),
            `-ERR Error running script (call to f_${sha1(script)}): @user_script:2: WRONGTYPE Operation against a key holding the wrong kind of value\r\n`,
          )
        } finally {
          await send('DEL', key)
        }
      },
    )

    // Errors the scripting layer raises itself, before any command runs, are
    // rendered by real 6.2 through `luaPushError`: 6.2's own wording, no error
    // code, and an inner `@user_script: <line>: ` position. A redis.call
    // rejection aborts the script, so the frame repeats the position inside
    // the abort decoration. Byte for byte against real redis-server 6.2.24.
    const legacyScriptRejections: Array<[string, string]> = [
      [
        "return redis.call('nosuchcmd', 'a')",
        'Unknown Redis command called from Lua script',
      ],
      [
        // 6.2 has no QUIT table entry: command lookup fails.
        "return redis.call('QUIT')",
        'Unknown Redis command called from Lua script',
      ],
      [
        "return redis.call('SUBSCRIBE', 'c')",
        'This Redis command is not allowed from scripts',
      ],
      [
        // A noscript container refuses every subcommand on 6.2 (#474).
        "return redis.call('CLIENT', 'NOPE')",
        'This Redis command is not allowed from scripts',
      ],
      [
        // 6.2 names redis.call() here even when redis.pcall() was called.
        'return redis.call()',
        'Please specify at least one argument for redis.call()',
      ],
      [
        "return redis.call('SET', {}, 'v')",
        'Lua redis() command arguments must be strings or integers',
      ],
      [
        "return redis.call('GET')",
        'Wrong number of args calling Redis command From Lua script',
      ],
    ]

    test(
      'script-level redis.call rejections on 6.2 use its wording and inner position',
      {
        skip:
          supportsSuffixScriptErrorDecoration() && 'redis-6.2 only, see above',
      },
      async () => {
        for (const [call, body] of legacyScriptRejections) {
          const script = `local x = 1\n${call}`
          assert.strictEqual(
            await send('EVAL', script, '0'),
            `-ERR Error running script (call to f_${sha1(script)}): @user_script:2: @user_script: 2: ${body}\r\n`,
            script,
          )
        }
      },
    )

    // KNOWN GAP: real 6.2 hands redis.pcall the same text with the inner
    // position, `-@user_script: 2: <wording>`. The position is the line of the
    // pcall, and the engine (lua-redis-wasm) does not pass the calling line to
    // the host's redis.pcall callback, so this server answers the wording
    // without it. The argument-type rejection is raised by the engine itself,
    // even under pcall, so it aborts the script instead (on every profile).
    // Tighten to the real frames once the engine exposes the line
    // (fatal10110/lua-redis-wasm#28, #503).
    test(
      'script-level redis.pcall rejections on 6.2 (known gap: inner position)',
      {
        skip:
          supportsSuffixScriptErrorDecoration() && 'redis-6.2 only, see above',
      },
      async () => {
        for (const [call, body] of legacyScriptRejections) {
          if (call.includes('{}')) continue
          const script = `local x = 1\n${call.replace('redis.call', 'redis.pcall')}`
          assert.strictEqual(
            await send('EVAL', script, '0'),
            `-${body}\r\n`,
            script,
          )
        }
      },
    )

    // Only a count the command table rejects is the scripting layer's arity
    // error. HSET with a dangling field and an odd MSET pass the table (arity
    // -4 / -3), so the command runs and answers its own error, `ERR` code
    // included, in the profile's decoration. 6.2's MSET (and MSETNX) word it
    // `wrong number of arguments for MSET`. Byte for byte against real
    // redis-server 6.2.24, 7.0, 8.0.6 and Valkey 8.0.
    test("a count the command table accepts gets the command's own arity error", async () => {
      const legacy = !supportsSuffixScriptErrorDecoration()
      const cases: Array<[string, string]> = [
        [
          "redis.call('hset', 'h', 'f', 'v', 'x')",
          "ERR wrong number of arguments for 'hset' command",
        ],
        [
          "redis.call('mset', 'k', 'v', 'x')",
          legacy
            ? 'ERR wrong number of arguments for MSET'
            : "ERR wrong number of arguments for 'mset' command",
        ],
      ]
      for (const [call, error] of cases) {
        const pcall = `return ${call.replace('redis.call', 'redis.pcall')}`
        assert.strictEqual(await send('EVAL', pcall, '0'), `-${error}\r\n`)

        const script = `return ${call}`
        assert.strictEqual(
          await send('EVAL', script, '0'),
          legacy
            ? `-ERR Error running script (call to f_${sha1(script)}): @user_script:1: ${error}\r\n`
            : `-${error} script: ${sha1(script)}, on @user_script:1.\r\n`,
        )
      }

      // From a client, too.
      assert.strictEqual(
        await send('MSETNX', 'k', 'v', 'x'),
        legacy
          ? '-ERR wrong number of arguments for MSET\r\n'
          : "-ERR wrong number of arguments for 'msetnx' command\r\n",
      )
    })

    test('writing a global is rejected by the readonly table', async () => {
      // The Lua engine blocks global writes via Lua's native readonly table, so
      // the wording is version-invariant across profiles.
      const reply = await send('EVAL', 'x = 5', '0')
      assert.ok(reply.startsWith('-'), `expected an error, got ${reply}`)
      assert.match(reply, /Attempt to modify a readonly table/)
    })

    test('Lua sandbox globals (print / os) match the profile', async () => {
      // print: only redis-6.2 still exposes it (returns nil, not an error).
      const printReply = await send('EVAL', "print('x')", '0')
      if (profile === 'redis-6.2') {
        assert.ok(
          !printReply.startsWith('-'),
          `print should be available on ${profile}, got ${printReply}`,
        )
      } else {
        assert.match(printReply, /nonexistent global variable 'print'/)
      }

      // os: exposed only from redis-7.4 / valkey-8.0 onward.
      const osReply = await send('EVAL', 'return type(os)', '0')
      if (supportsLuaOsLib()) {
        assert.match(osReply, /table/)
      } else {
        assert.match(osReply, /nonexistent global variable 'os'/)
      }
    })

    test('a noscript container HELP from a script matches the profile (#452)', async () => {
      // 6.2 flags the whole container noscript; 7.0+ flags each subcommand
      // and leaves HELP runnable from scripts.
      for (const container of ['CLIENT', 'ACL', 'SCRIPT']) {
        const reply = await send(
          'EVAL',
          `return redis.pcall('${container}','HELP')`,
          '0',
        )
        if (profile === 'redis-6.2') {
          assert.match(reply, /^-.*not allowed from script/, container)
        } else {
          assert.ok(reply.startsWith('*'), `${container}: ${reply}`)
        }
      }

      // Every other subcommand stays refused on every profile.
      const refused = await send(
        'EVAL',
        "return redis.pcall('CLIENT','GETNAME')",
        '0',
      )
      assert.match(refused, /^-.*not allowed from script/)

      // 7.0+ resolves `container|subcommand` first, so an unknown (or
      // not-yet-introduced) subcommand fails lookup; 6.2 refuses the container.
      const unknownOnNewer =
        profile === 'redis-6.2'
          ? /not allowed from script/
          : /Unknown .*command/
      const nope = await send(
        'EVAL',
        "return redis.pcall('CLIENT','NOPE')",
        '0',
      )
      assert.match(nope, /^-/)
      assert.match(nope, unknownOnNewer)

      const setinfo = await send(
        'EVAL',
        "return redis.pcall('CLIENT','SETINFO','lib-name','x')",
        '0',
      )
      assert.match(
        setinfo,
        profile === 'redis-7.0'
          ? /Unknown .*command/
          : /not allowed from script/,
      )

      // The lookup is against the real table: a real subcommand this server
      // does not implement is refused, and one the profile's server does not
      // have yet is unknown.
      const cases: Array<[string, boolean]> = [
        ["'ACL','CAT'", true],
        ["'CLIENT','PAUSE','0'", true],
        ["'CLIENT','NO-TOUCH','ON'", profile !== 'redis-7.0'],
        [
          "'CLIENT','CAPA','redirect'",
          !profile.startsWith('redis-') || profile === 'redis-6.2',
        ],
        [
          "'SCRIPT','SHOW','x'",
          !profile.startsWith('redis-') || profile === 'redis-6.2',
        ],
        [
          "'CLIENT','IMPORT-SOURCE','ON'",
          profile === 'valkey-9.0' || profile === 'redis-6.2',
        ],
      ]
      for (const [call, refused] of cases) {
        const reply = await send('EVAL', `return redis.pcall(${call})`, '0')
        assert.match(
          reply,
          refused ? /^-.*not allowed from script/ : /^-.*Unknown .*command/,
          call,
        )
      }

      // QUIT has a command-table entry (and so the noscript refusal) only from
      // 7.0; a 6.2 script sees an unknown command.
      const quit = await send('EVAL', "return redis.pcall('QUIT')", '0')
      assert.match(
        quit,
        profile === 'redis-6.2'
          ? /Unknown .*command/
          : /not allowed from script/,
      )
    })

    test('RESP3 subscribed PUBLISH self-reply order matches the profile', async () => {
      const channel = `compat:${profile}:self-publish`

      connection.write(commandFrame('HELLO', '3'))
      const hello = await connection.readFrame()
      assert.ok(hello instanceof Map)
      assert.strictEqual(respNumber(respMapGet(hello, 'proto')), 3)

      connection.write(commandFrame('SUBSCRIBE', channel))
      assert.deepStrictEqual(normalizeFrame(await connection.readFrame()), [
        'subscribe',
        channel,
        1,
      ])

      connection.write(commandFrame('PUBLISH', channel, 'self'))
      const first = await connection.readFrame()
      const second = await connection.readFrame()
      const message = ['message', channel, 'self']

      if (supportsResp3PublishReplyBeforeSelfMessage()) {
        assert.strictEqual(first, 1)
        assert.deepStrictEqual(normalizeFrame(second), message)
      } else {
        assert.deepStrictEqual(normalizeFrame(first), message)
        assert.strictEqual(second, 1)
      }

      connection.write(commandFrame('UNSUBSCRIBE', channel))
      assert.deepStrictEqual(normalizeFrame(await connection.readFrame()), [
        'unsubscribe',
        channel,
        0,
      ])

      connection.write(commandFrame('HELLO', '2'))
      await connection.readFrame()
    })

    test('XREADGROUP and XAUTOCLAIM create the consumer entry even when nothing is delivered or claimed', async () => {
      const key = `compat:${profile}:consumer-create-on-empty`

      connection.write(commandFrame('XADD', key, '1-1', 'f', 'v'))
      await connection.readFrame()
      connection.write(commandFrame('XGROUP', 'CREATE', key, 'g', '0'))
      await connection.readFrame()
      connection.write(
        commandFrame(
          'XREADGROUP',
          'GROUP',
          'g',
          'alice',
          'COUNT',
          '10',
          'STREAMS',
          key,
          '>',
        ),
      )
      await connection.readFrame()

      // bob delivers nothing (alice already consumed the only entry) but must
      // still be created as a consumer: ensureConsumer() runs unconditionally
      // before the empty-delivery check.
      connection.write(
        commandFrame(
          'XREADGROUP',
          'GROUP',
          'g',
          'bob',
          'COUNT',
          '10',
          'STREAMS',
          key,
          '>',
        ),
      )
      assert.strictEqual(await connection.readFrame(), null)

      const bob = await findConsumer(key, 'g', 'bob')
      assert.ok(
        bob,
        `bob should be created as a consumer for ${profile} despite an empty XREADGROUP delivery`,
      )
      assert.strictEqual(bob.pending, 0)

      // carol's min-idle-time (999999999ms) is never met, so nothing is
      // actually claimed, but ensureConsumer() still runs first.
      connection.write(
        commandFrame(
          'XAUTOCLAIM',
          key,
          'g',
          'carol',
          '999999999',
          '0',
          'COUNT',
          '10',
        ),
      )
      const autoclaim = normalizeFrame(
        await connection.readFrame(),
      ) as RespWireValue[]
      assert.deepStrictEqual(autoclaim[1], [])

      const carol = await findConsumer(key, 'g', 'carol')
      assert.ok(
        carol,
        `carol should be created as a consumer for ${profile} despite an empty XAUTOCLAIM claim`,
      )
      assert.strictEqual(carol.pending, 0)
    })

    test('XINFO CONSUMERS reports idle and inactive fields', async () => {
      const key = `compat:${profile}:consumers-idle-inactive`

      connection.write(commandFrame('XADD', key, '1-1', 'f', 'v'))
      await connection.readFrame()
      connection.write(commandFrame('XGROUP', 'CREATE', key, 'g', '0'))
      await connection.readFrame()
      connection.write(
        commandFrame(
          'XREADGROUP',
          'GROUP',
          'g',
          'alice',
          'COUNT',
          '10',
          'STREAMS',
          key,
          '>',
        ),
      )
      await connection.readFrame()

      const alice = await findConsumer(key, 'g', 'alice')
      assert.ok(alice)
      assert.strictEqual(typeof alice.idle, 'number')
      assert.ok((alice.idle as number) >= 0)
      assert.strictEqual(typeof alice.inactive, 'number')
      assert.ok((alice.inactive as number) >= 0)
    })

    async function findConsumer(
      key: string,
      group: string,
      name: string,
    ): Promise<Record<string, RespWireValue> | undefined> {
      connection.write(commandFrame('XINFO', 'CONSUMERS', key, group))
      const reply = normalizeFrame(await connection.readFrame())
      assert.ok(Array.isArray(reply))
      for (const entry of reply) {
        assert.ok(Array.isArray(entry))
        const record = flatToRecord(entry)
        if (record.name === name) return record
      }
      return undefined
    }

    function flatToRecord(
      flat: RespWireValue[],
    ): Record<string, RespWireValue> {
      const record: Record<string, RespWireValue> = {}
      for (let i = 0; i < flat.length; i += 2) {
        record[String(flat[i])] = flat[i + 1]
      }
      return record
    }

    async function send(...args: string[]): Promise<string> {
      connection.write(commandFrame(...args))
      return (await connection.readRawFrame()).toString()
    }

    async function expectGate(
      available: boolean,
      ...args: string[]
    ): Promise<void> {
      const reply = await send(...args)
      if (available) {
        assert.ok(
          !reply.startsWith('-'),
          `${args.join(' ')} should be available for ${profile}, got ${JSON.stringify(reply)}`,
        )
        return
      }

      assert.ok(
        reply.startsWith('-'),
        `${args.join(' ')} should be gated for ${profile}, got ${JSON.stringify(reply)}`,
      )
    }

    async function expectRootCommand(
      command: string,
      available: boolean,
    ): Promise<void> {
      const reply = await send(command)
      if (available) {
        assert.doesNotMatch(
          reply,
          /unknown command/i,
          `${command} should be registered for ${profile}, got ${JSON.stringify(reply)}`,
        )
        return
      }

      assert.match(
        reply,
        /unknown command/i,
        `${command} should be absent for ${profile}, got ${JSON.stringify(reply)}`,
      )
    }
  },
)

function supportsLuaOsLib(): boolean {
  // The sandboxed Lua `os` library is exposed from Redis 7.4 / Valkey 8.0 on.
  return ['redis-7.4', 'redis-8.0', 'valkey-8.0', 'valkey-9.0'].includes(
    profile,
  )
}

function supportsResp3PublishReplyBeforeSelfMessage(): boolean {
  return !['redis-6.2', 'redis-7.0'].includes(profile)
}

function supportsExpireConditions(): boolean {
  return profile !== 'redis-6.2'
}

function supportsRedis70Commands(): boolean {
  return profile !== 'redis-6.2'
}

function supportsSetNxGet(): boolean {
  return profile !== 'redis-6.2'
}

function supportsConfigSetFailureWording(): boolean {
  return profile !== 'redis-6.2'
}

function supportsConfigSetMultiPair(): boolean {
  return profile !== 'redis-6.2'
}

function supportsMemoryValueOverflowRejection(): boolean {
  return profile !== 'redis-6.2'
}

function supportsNewKeyNotifyClass(): boolean {
  return profile !== 'redis-6.2'
}

function supportsCommandDocs(): boolean {
  return profile !== 'redis-6.2'
}

function supportsUnknownSubcommandWording(): boolean {
  return profile !== 'redis-6.2'
}

function supportsSuffixScriptErrorDecoration(): boolean {
  return profile !== 'redis-6.2'
}

function sha1(script: string): string {
  return createHash('sha1').update(script).digest('hex')
}

function supportsClientSetinfo(): boolean {
  return !['redis-6.2', 'redis-7.0'].includes(profile)
}

function supportsClientKillMaxAge(): boolean {
  return ['redis-7.4', 'redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsShardedPubSub(): boolean {
  return profile !== 'redis-6.2'
}

function supportsZintercard(): boolean {
  return profile !== 'redis-6.2'
}

function supportsBitByteBitRange(): boolean {
  return profile !== 'redis-6.2'
}

function supportsHashFieldExpiration(): boolean {
  return ['redis-7.4', 'redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsHscanNoValues(): boolean {
  return ['redis-7.4', 'redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsXreadPlusId(): boolean {
  return ['redis-7.4', 'redis-8.0'].includes(profile)
}

function supportsHgetex(): boolean {
  return ['redis-8.0', 'valkey-9.0'].includes(profile)
}

function supportsHgetdel(): boolean {
  return profile === 'redis-8.0'
}

function supportsHsetex(): boolean {
  return ['redis-8.0', 'valkey-9.0'].includes(profile)
}

function bulkStringFrame(value: string): RegExp {
  return new RegExp(
    `\\$${Buffer.byteLength(value)}\\r\\n${escapeRegExp(value)}\\r\\n`,
  )
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function normalizeFrame(value: RespWireValue): RespWireValue {
  if (Buffer.isBuffer(value)) {
    return value.toString()
  }

  if (Array.isArray(value)) {
    return value.map(normalizeFrame)
  }

  return value
}
