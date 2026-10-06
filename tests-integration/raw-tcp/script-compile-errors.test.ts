import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * A script that is not valid Lua never runs, so its error has none of the
 * abort decoration: every version replies `-ERR Error compiling script (new
 * function): <Lua's message>`, for EVAL, EVAL_RO and SCRIPT LOAD alike, and
 * does not cache it. A script that compiles is cached by EVAL even when it
 * then fails (#502). The compile errors are byte for byte against
 * redis-server 6.2.24, 7.0.15, 7.2.16, 7.4.11, 8.0.6 and valkey-server
 * 7.2.14, 8.0.11, 9.0.6; the other replies follow the active profile (6.2's
 * abort decoration, no EVAL_RO or shebang on 6.2, Valkey 8.0+'s NOSCRIPT).
 *
 * The script cache is server-wide and the real backend is never flushed, so
 * every script carries this run's tag in a leading comment (which puts the
 * syntax error on line 2).
 */
const testRunner = new TestRunner()
const RUN = randomKey()

const sha1 = (script: string) => createHash('sha1').update(script).digest('hex')
const legacy = activeProfile === 'redis-6.2'
const NOSCRIPT = activeProfile.startsWith('valkey-')
  ? '-NOSCRIPT No matching script.\r\n'
  : '-NOSCRIPT No matching script. Please use EVAL.\r\n'

describe(`Lua compile errors (${testRunner.getBackendName()})`, () => {
  let connection: RawRedisConnection

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    connection = await RawRedisConnection.connect('127.0.0.1', port)
  })

  after(async () => {
    connection.close()
    await testRunner.cleanup()
  })

  async function send(...args: string[]): Promise<string> {
    connection.write(commandFrame(...args))
    return (await connection.readRawFrame()).toString('binary')
  }

  const COMPILE_ERROR =
    "-ERR Error compiling script (new function): user_script:2: unexpected symbol near '+'\r\n"

  test('EVAL of invalid Lua is a compile error and is not cached', async () => {
    const script = `-- ${RUN} eval\nreturn +`
    assert.strictEqual(await send('EVAL', script, '0'), COMPILE_ERROR)
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS', sha1(script)),
      '*1\r\n:0\r\n',
    )
  })

  test(
    'EVAL_RO of invalid Lua is a compile error',
    {
      skip: legacy && 'EVAL_RO is 7.0+',
    },
    async () => {
      const script = `-- ${RUN} eval_ro\nreturn +`
      assert.strictEqual(await send('EVAL_RO', script, '0'), COMPILE_ERROR)
      assert.strictEqual(
        await send('SCRIPT', 'EXISTS', sha1(script)),
        '*1\r\n:0\r\n',
      )
    },
  )

  test('SCRIPT LOAD refuses invalid Lua and does not cache it', async () => {
    const script = `-- ${RUN} load\nreturn +`
    assert.strictEqual(await send('SCRIPT', 'LOAD', script), COMPILE_ERROR)
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS', sha1(script)),
      '*1\r\n:0\r\n',
    )
    assert.strictEqual(await send('EVALSHA', sha1(script), '0'), NOSCRIPT)
  })

  test('SCRIPT LOAD inside MULTI answers the compile error at EXEC', async () => {
    const script = `-- ${RUN} multi\nreturn +`
    assert.strictEqual(await send('MULTI'), '+OK\r\n')
    assert.strictEqual(await send('SCRIPT', 'LOAD', script), '+QUEUED\r\n')
    assert.strictEqual(await send('EXEC'), `*1\r\n${COMPILE_ERROR}`)
  })

  test('an unfinished script names <eof>', async () => {
    const script = `-- ${RUN} eof\nreturn (`
    assert.strictEqual(
      await send('EVAL', script, '0'),
      "-ERR Error compiling script (new function): user_script:2: unexpected symbol near '<eof>'\r\n",
    )
  })

  test('a script that compiles is cached even when it fails at run time', async () => {
    const script = `-- ${RUN} runtime\nerror('boom', 0)`
    assert.strictEqual(
      await send('EVAL', script, '0'),
      legacy
        ? `-ERR Error running script (call to f_${sha1(script)}): @user_script:2: boom\r\n`
        : `-ERR boom script: ${sha1(script)}, on @user_script:2.\r\n`,
    )
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS', sha1(script)),
      '*1\r\n:1\r\n',
    )
  })

  test('SCRIPT LOAD still caches valid Lua without running it', async () => {
    const script = `-- ${RUN} valid\nerror('not run', 0)`
    assert.strictEqual(
      await send('SCRIPT', 'LOAD', script),
      `$40\r\n${sha1(script)}\r\n`,
    )
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS', sha1(script)),
      '*1\r\n:1\r\n',
    )
  })

  test(
    'SCRIPT LOAD skips a shebang line from 7.0',
    {
      skip: legacy && 'shebangs are 7.0+',
    },
    async () => {
      // The shebang must come first, so the run's tag goes on line 2.
      const valid = `#!lua\n-- ${RUN} shebang\nreturn 1`
      assert.strictEqual(
        await send('SCRIPT', 'LOAD', valid),
        `$40\r\n${sha1(valid)}\r\n`,
      )

      // Its line feed is kept, so the error names the body's own line.
      const invalid = `#!lua flags=no-writes\n-- ${RUN} shebang\nreturn +`
      assert.strictEqual(
        await send('SCRIPT', 'LOAD', invalid),
        "-ERR Error compiling script (new function): user_script:3: unexpected symbol near '+'\r\n",
      )
      assert.strictEqual(
        await send('SCRIPT', 'EXISTS', sha1(invalid)),
        '*1\r\n:0\r\n',
      )
    },
  )

  // #536: Redis 7.0+ and Valkey run a shebang script's body. Byte for byte
  // against redis-server 7.0.15 (and the #536 transcripts for 8.0.6 and
  // valkey-server 7.2.14 / 8.0.11 / 9.0.6).
  test(
    'EVAL runs a shebang script, counting the shebang line',
    { skip: legacy && 'shebangs are 7.0+' },
    async () => {
      const valid = `#!lua\n-- ${RUN} eval shebang\nreturn 1`
      assert.strictEqual(await send('EVAL', valid, '0'), ':1\r\n')
      assert.strictEqual(await send('EVALSHA', sha1(valid), '0'), ':1\r\n')

      const invalid = `#!lua\n-- ${RUN} eval shebang\nreturn +`
      assert.strictEqual(
        await send('EVAL', invalid, '0'),
        "-ERR Error compiling script (new function): user_script:3: unexpected symbol near '+'\r\n",
      )

      // Errors name the SHA of the script as sent, shebang included.
      const boom = `#!lua\n-- ${RUN} eval shebang\nerror('boom')`
      assert.strictEqual(
        await send('EVAL', boom, '0'),
        `-ERR user_script:3: boom script: ${sha1(boom)}, on @user_script:3.\r\n`,
      )
    },
  )

  test(
    'a no-writes shebang script refuses writes; EVAL_RO needs no-writes',
    { skip: legacy && 'shebangs are 7.0+' },
    async () => {
      const key = `{script-shebang}:${RUN}`
      const write = `#!lua flags=no-writes\nreturn redis.call('set', KEYS[1], '1')`
      assert.strictEqual(
        await send('EVAL', write, '1', key),
        `-ERR Write commands are not allowed from read-only scripts. script: ${sha1(write)}, on @user_script:2.\r\n`,
      )
      assert.strictEqual(await send('EXISTS', key), ':0\r\n')

      const read = `#!lua flags=no-writes\nreturn redis.call('exists', KEYS[1])`
      assert.strictEqual(await send('EVAL_RO', read, '1', key), ':0\r\n')

      // Without no-writes, EVAL_RO refuses it, but it compiled, so it is
      // cached.
      const writable = `#!lua\n-- ${RUN} eval_ro\nreturn 1`
      assert.strictEqual(
        await send('EVAL_RO', writable, '0'),
        '-ERR Can not execute a script with write flag using *_ro command.\r\n',
      )
      assert.strictEqual(
        await send('SCRIPT', 'EXISTS', sha1(writable)),
        '*1\r\n:1\r\n',
      )
    },
  )

  test(
    'EVAL and SCRIPT LOAD refuse an invalid shebang and do not cache it',
    { skip: legacy && 'shebangs are 7.0+' },
    async () => {
      const cases: Array<[string, string]> = [
        [
          `#!lua flags=bogus\n-- ${RUN}\nreturn 1`,
          'Unexpected flag in script shebang: bogus',
        ],
        [
          `#!lua name=x\n-- ${RUN}\nreturn 1`,
          'Unknown lua shebang option: name=x',
        ],
        [`#!lua flags=${RUN}`, 'Invalid script shebang'],
      ]
      for (const [script, message] of cases) {
        assert.strictEqual(
          await send('EVAL', script, '0'),
          `-ERR ${message}\r\n`,
        )
        assert.strictEqual(
          await send('SCRIPT', 'LOAD', script),
          `-ERR ${message}\r\n`,
        )
        assert.strictEqual(
          await send('SCRIPT', 'EXISTS', sha1(script)),
          '*1\r\n:0\r\n',
        )
      }
    },
  )

  // #538: FUNCTION LOAD compiles the library first. Byte for byte against
  // redis-server 7.0.15 (and the #538 transcripts for 8.0.6 and
  // valkey-server 8.0.11).
  test(
    'FUNCTION LOAD refuses a library that does not compile',
    { skip: legacy && 'functions are 7.0+' },
    async () => {
      const name = `badlib_${RUN}`
      const compileError =
        "-ERR Error compiling function: user_function:2: unexpected symbol near '+'\r\n"
      assert.strictEqual(
        await send('FUNCTION', 'LOAD', `#!lua name=${name}\nreturn +`),
        compileError,
      )
      assert.strictEqual(
        await send(
          'FUNCTION',
          'LOAD',
          `#!lua name=${name}\nredis.register_function('f_${RUN}', function() return 1 end) +`,
        ),
        compileError,
      )
      assert.strictEqual(
        await send('FUNCTION', 'LIST', 'LIBRARYNAME', name),
        '*0\r\n',
      )
    },
  )

  test(
    'FUNCTION LOAD REPLACE with code that does not compile keeps the library',
    { skip: legacy && 'functions are 7.0+' },
    async () => {
      const name = `keeplib_${RUN}`
      const fn = `keep_${RUN}`
      const good = `#!lua name=${name}\nredis.register_function('${fn}', function() return 1 end)`
      assert.strictEqual(
        await send('FUNCTION', 'LOAD', good),
        `$${name.length}\r\n${name}\r\n`,
      )
      try {
        // Without REPLACE the existing library is refused first.
        assert.strictEqual(
          await send('FUNCTION', 'LOAD', `#!lua name=${name}\nreturn +`),
          `-ERR Library '${name}' already exists\r\n`,
        )
        assert.strictEqual(
          await send(
            'FUNCTION',
            'LOAD',
            'REPLACE',
            `#!lua name=${name}\nreturn +`,
          ),
          "-ERR Error compiling function: user_function:2: unexpected symbol near '+'\r\n",
        )
        assert.strictEqual(await send('FCALL', fn, '0'), ':1\r\n')
      } finally {
        await send('FUNCTION', 'DELETE', name)
      }
    },
  )
})
