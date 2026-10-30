import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'

/**
 * CONFIG SET value validation and CONFIG GET name handling per profile (#506).
 *
 * Real servers type every parameter (config.c `createBoolConfig`,
 * `createIntConfig`, ...) and reject a value of the wrong shape; the failure
 * wording is 6.2's `Invalid argument '<value>' for CONFIG SET '<name as
 * sent>' - <detail>` or 7.0+'s `CONFIG SET failed (possibly related to
 * argument '<table name>') - <detail>`. Redis 7.0 also renamed the ziplist
 * parameters to listpack (the old names stay as aliases), made immutable
 * parameters an error of their own, and let CONFIG GET take several
 * patterns. Redis 7.2 / Valkey added `loglevel nothing`; Redis 7.4 / Valkey
 * 8.0 capped `maxmemory-samples` at 64.
 *
 * Every expectation follows `REDIS_COMPAT`, so the file runs against a real
 * server of each profile. The real containers are shared and keep CONFIG
 * state: a test that changes a parameter restores the value it read first.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const v7 = profile !== 'redis-6.2'
const loglevelNothing = profile !== 'redis-6.2' && profile !== 'redis-7.0'
const samplesMax64 = !['redis-6.2', 'redis-7.0', 'redis-7.2'].includes(profile)

const KELVIN = String.fromCodePoint(0x212a)

const INTEGER = "argument couldn't be parsed into an integer"
const MEMORY = 'argument must be a memory value'
const BOOLEAN = "argument must be 'yes' or 'no'"
const POLICIES =
  'volatile-lru, volatile-lfu, volatile-random, volatile-ttl, allkeys-lru, allkeys-lfu, allkeys-random, noeviction'
const LOGLEVELS = `debug, verbose, notice, warning${loglevelNothing ? ', nothing' : ''}`

function between(min: string, max: string): string {
  return `argument must be between ${min} and ${max} inclusive`
}

function oneOf(list: string): string {
  return `${v7 ? 'argument(s)' : 'argument'} must be one of the following: ${list}`
}

/**
 * The value-error reply. `table` is the name 7.0+ echoes (the table spelling
 * the sent name matched); 6.2 echoes `sent`.
 */
function failed(
  sent: string,
  table: string,
  value: string,
  detail: string,
  { legacyDetail = true } = {},
): string {
  if (!v7) {
    const suffix = legacyDetail ? ` - ${detail}` : ''
    return `-ERR Invalid argument '${value}' for CONFIG SET '${sent}'${suffix}\r\n`
  }
  return `-ERR CONFIG SET failed (possibly related to argument '${table}') - ${detail}\r\n`
}

function unknownParameter(sent: string): string {
  return v7
    ? `-ERR Unknown option or number of arguments for CONFIG SET - '${sent}'\r\n`
    : `-ERR Unsupported CONFIG parameter: ${sent}\r\n`
}

function bulk(text: string): string {
  return `$${Buffer.byteLength(text)}\r\n${text}\r\n`
}

function pair(name: string, value: string): string {
  return `*2\r\n${bulk(name)}${bulk(value)}`
}

// The 6.2 name of each renamed parameter is the only one 6.2 knows.
const LIST_SIZE = v7 ? 'list-max-listpack-size' : 'list-max-ziplist-size'

// [name as sent, value, detail]
const VALUE_ERRORS: [string, string, string][] = [
  ['TIMEOUT', 'abc', INTEGER],
  ['timeout', '0x10', INTEGER],
  ['timeout', ' 5', INTEGER],
  ['timeout', '+5', INTEGER],
  ['timeout', '-1', between('0', '2147483647')],
  ['tcp-keepalive', 'abc', INTEGER],
  ['maxclients', '0', between('1', '4294967295')],
  ['set-max-intset-entries', '-1', between('0', '9223372036854775807')],
  [LIST_SIZE, 'abc', INTEGER],
  [LIST_SIZE, '2147483648', between('-2147483648', '2147483647')],
  ['maxmemory-samples', '0', between('1', samplesMax64 ? '64' : '2147483647')],
  ['appendonly', 'maybe', BOOLEAN],
  ['maxmemory', 'abc', MEMORY],
  ['maxmemory', '5q', MEMORY],
  ['maxmemory', '1.5', MEMORY],
  ['maxmemory', '-1', MEMORY],
  ['hash-max-ziplist-value', 'abc', MEMORY],
  ['maxmemory-policy', 'foo', oneOf(POLICIES)],
  ['loglevel', 'bogus', oneOf(LOGLEVELS)],
]

describe(`CONFIG fidelity (${testRunner.getBackendName()}, ${profile})`, () => {
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

  /** The value of an exact-name CONFIG GET (7.0+ echoes the name as sent). */
  async function get(name: string): Promise<string> {
    const reply = await send('CONFIG', 'GET', name)
    const match = /^\*2\r\n\$\d+\r\n[^\r]*\r\n\$\d+\r\n([^\r]*)\r\n$/.exec(
      reply,
    )
    assert.ok(match, `CONFIG GET ${name}: ${JSON.stringify(reply)}`)
    return match[1]
  }

  /** Run `body`, then put each parameter back to the value it had. */
  async function restoring(
    names: string[],
    body: () => Promise<void>,
  ): Promise<void> {
    const saved: [string, string][] = []
    for (const name of names) {
      saved.push([name, await get(name)])
    }
    try {
      await body()
    } finally {
      for (const [name, value] of saved) {
        assert.strictEqual(await send('CONFIG', 'SET', name, value), '+OK\r\n')
      }
    }
  }

  for (const [name, value, detail] of VALUE_ERRORS) {
    test(`CONFIG SET ${name} ${JSON.stringify(value)} is rejected`, async () => {
      assert.strictEqual(
        await send('CONFIG', 'SET', name, value),
        failed(name, name.toLowerCase(), value, detail),
      )
    })
  }

  test('save takes <seconds> <changes> pairs', async () => {
    for (const value of ['x y', '100', '0 1', '100 -1']) {
      assert.strictEqual(
        await send('CONFIG', 'SET', 'save', value),
        failed('save', 'save', value, 'Invalid save parameters', {
          legacyDetail: false,
        }),
        value,
      )
    }
    await restoring(['save'], async () => {
      assert.strictEqual(
        await send('CONFIG', 'SET', 'save', '900 1 300 10'),
        '+OK\r\n',
      )
      assert.strictEqual(await get('save'), '900 1 300 10')
    })
  })

  test('valid values are accepted and read back normalized', async () => {
    await restoring(
      ['maxmemory', 'maxmemory-policy', 'appendonly', 'timeout', 'loglevel'],
      async () => {
        for (const [name, value, readBack] of [
          ['maxmemory', '1gb', '1073741824'],
          ['maxmemory-policy', 'Allkeys-LRU', 'allkeys-lru'],
          ['appendonly', 'NO', 'no'],
          ['timeout', '300', '300'],
          ['loglevel', 'WARNING', 'warning'],
        ]) {
          assert.strictEqual(
            await send('CONFIG', 'SET', name, value),
            '+OK\r\n',
            `${name} ${value}`,
          )
          assert.strictEqual(await get(name), readBack, name)
        }
      },
    )
  })

  test('loglevel nothing: Redis 7.2+ and Valkey', async () => {
    await restoring(['loglevel'], async () => {
      assert.strictEqual(
        await send('CONFIG', 'SET', 'loglevel', 'nothing'),
        loglevelNothing
          ? '+OK\r\n'
          : failed('loglevel', 'loglevel', 'nothing', oneOf(LOGLEVELS)),
      )
      if (loglevelNothing) {
        assert.strictEqual(await get('loglevel'), 'nothing')
      }
    })
  })

  test('maxmemory-samples tops out at 64 on Redis 7.4+ / Valkey 8.0+', async () => {
    await restoring(['maxmemory-samples'], async () => {
      assert.strictEqual(
        await send('CONFIG', 'SET', 'maxmemory-samples', '64'),
        '+OK\r\n',
      )
      assert.strictEqual(
        await send('CONFIG', 'SET', 'maxmemory-samples', '65'),
        samplesMax64
          ? failed(
              'maxmemory-samples',
              'maxmemory-samples',
              '65',
              between('1', '64'),
            )
          : '+OK\r\n',
      )
    })
  })

  test('immutable parameters: their own error on 7.0+, unsupported on 6.2', async () => {
    for (const [name, value] of [
      ['DATABASES', '5'],
      ['daemonize', 'yes'],
    ]) {
      assert.strictEqual(
        await send('CONFIG', 'SET', name, value),
        v7
          ? `-ERR CONFIG SET failed (possibly related to argument '${name}') - can't set immutable config\r\n`
          : unknownParameter(name),
      )
    }
    assert.strictEqual(await get('databases'), '16')
  })

  test('the ziplist names: aliases on 7.0+, the only names on 6.2', async () => {
    await restoring(['hash-max-ziplist-entries'], async () => {
      assert.strictEqual(
        await send('CONFIG', 'SET', 'Hash-Max-Ziplist-Entries', 'abc'),
        failed(
          'Hash-Max-Ziplist-Entries',
          'hash-max-ziplist-entries',
          'abc',
          INTEGER,
        ),
      )
      assert.strictEqual(
        await send('CONFIG', 'SET', 'Hash-Max-Ziplist-Entries', '100'),
        '+OK\r\n',
      )
      assert.strictEqual(
        await send('CONFIG', 'GET', 'hash-max-ziplist-entries'),
        pair('hash-max-ziplist-entries', '100'),
      )

      const names = (await send('CONFIG', 'GET', 'hash-max-*-entries'))
        .split('\r\n')
        .filter(line => line.startsWith('hash-'))
        .sort()
      if (!v7) {
        assert.strictEqual(
          await send('CONFIG', 'GET', 'hash-max-listpack-entries'),
          '*0\r\n',
        )
        assert.strictEqual(
          await send('CONFIG', 'SET', 'hash-max-listpack-entries', '100'),
          unknownParameter('hash-max-listpack-entries'),
        )
        assert.deepStrictEqual(names, ['hash-max-ziplist-entries'])
        return
      }

      assert.strictEqual(
        await send('CONFIG', 'GET', 'HASH-MAX-LISTPACK-ENTRIES'),
        pair('HASH-MAX-LISTPACK-ENTRIES', '100'),
      )
      assert.deepStrictEqual(names, [
        'hash-max-listpack-entries',
        'hash-max-ziplist-entries',
      ])
      // A parameter and its alias are not a duplicate.
      assert.strictEqual(
        await send(
          'CONFIG',
          'SET',
          'hash-max-listpack-entries',
          '200',
          'hash-max-ziplist-entries',
          '300',
        ),
        '+OK\r\n',
      )
      assert.strictEqual(await get('hash-max-listpack-entries'), '300')
    })
  })

  test('CONFIG GET of an exact name: echoed as sent on 7.0+', async () => {
    assert.strictEqual(
      await send('CONFIG', 'GET', 'MAXMEMORY'),
      pair(v7 ? 'MAXMEMORY' : 'maxmemory', await get('maxmemory')),
    )
  })

  test('CONFIG GET takes one pattern on 6.2, several on 7.0+', async () => {
    const reply = await send('CONFIG', 'GET', 'timeout', 'maxmemory')
    if (!v7) {
      assert.strictEqual(
        reply,
        "-ERR Unknown subcommand or wrong number of arguments for 'GET'. Try CONFIG HELP.\r\n",
      )
      return
    }
    // The order is the server's dict order on 7.0: compare as a set.
    assert.match(reply, /^\*4\r\n/)
    assert.deepStrictEqual(
      reply
        .split('\r\n')
        .filter(line => line === 'timeout' || line === 'maxmemory')
        .sort(),
      ['maxmemory', 'timeout'],
    )
    assert.strictEqual(
      await send('CONFIG', 'GET', 'no-such-a', 'no-such-b'),
      '*0\r\n',
    )
  })

  test('a CONFIG SET value error inside MULTI fails in EXEC', async () => {
    assert.strictEqual(await send('MULTI'), '+OK\r\n')
    assert.strictEqual(
      await send('CONFIG', 'SET', 'timeout', 'abc'),
      '+QUEUED\r\n',
    )
    assert.strictEqual(
      await send('EXEC'),
      `*1\r\n${failed('timeout', 'timeout', 'abc', INTEGER)}`,
    )
  })

  test('CONFIG SET with a missing value inside MULTI', async () => {
    assert.strictEqual(await send('MULTI'), '+OK\r\n')
    const queued = await send('CONFIG', 'SET', 'timeout')
    const exec = await send('EXEC')
    if (v7) {
      assert.strictEqual(
        queued,
        "-ERR wrong number of arguments for 'config|set' command\r\n",
      )
      assert.strictEqual(
        exec,
        '-EXECABORT Transaction discarded because of previous errors.\r\n',
      )
    } else {
      assert.strictEqual(queued, '+QUEUED\r\n')
      assert.strictEqual(
        exec,
        "*1\r\n-ERR Unknown subcommand or wrong number of arguments for 'SET'. Try CONFIG HELP.\r\n",
      )
    }
  })

  // #508: parameter names fold ASCII only. U+212A KELVIN SIGN lower-cases to
  // `k` in JavaScript, never in C `strcasecmp`.
  test('a Kelvin-sign parameter name is unknown', async () => {
    const name = `tcp-${KELVIN}eepalive`
    conn.write(commandFrame('CONFIG', 'SET', name, '10'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from(unknownParameter(name)),
    )
    assert.strictEqual(await send('CONFIG', 'GET', name), '*0\r\n')
  })
})
