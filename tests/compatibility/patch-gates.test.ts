import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
  encodeRedisValue,
  gateSatisfied,
  resolveCompatibilityProfile,
  type CompatibilitySpec,
  type RedisFlavor,
  type VersionGate,
} from '../../src/internal'
import {
  Resp2CommandDecoder,
  Resp2ParseError,
} from '../../src/core/transports/resp2/decoder'

/**
 * Patch-level backports (#521): gates with per-branch minimums, and the two
 * behaviors they gate, pinned on custom `{ flavor, version }` profiles (the
 * presets are versioned below every boundary).
 */

function satisfied(gate: VersionGate, flavor: RedisFlavor, version: string) {
  return gateSatisfied(gate, resolveCompatibilityProfile({ flavor, version }))
}

describe('per-branch version gates', () => {
  const gate: VersionGate = { redis: ['7.2.16', '7.4.11', '8.2.9'] }

  for (const [version, expected] of [
    ['6.2.14', false],
    ['7.0.15', false],
    ['7.2.15', false],
    ['7.2.16', true],
    ['7.2.17', true],
    ['7.4.10', false],
    ['7.4.11', true],
    ['8.0.0', false],
    ['8.0.6', false],
    ['8.2.8', false],
    ['8.2.9', true],
    ['8.4.0', true],
    ['9.0.0', true],
  ] as const) {
    test(`redis ${version}: ${expected}`, () => {
      assert.strictEqual(satisfied(gate, 'redis', version), expected)
    })
  }

  test('a flavor the gate does not name never satisfies it', () => {
    assert.strictEqual(satisfied(gate, 'valkey', '9.0.0'), false)
    assert.strictEqual(satisfied({ valkey: [] }, 'valkey', '9.0.0'), false)
  })

  test('a single minimum is met by that version and every later one', () => {
    for (const minimum of ['7.2.0', ['7.2.0']]) {
      const single: VersionGate = { redis: minimum }
      assert.strictEqual(satisfied(single, 'redis', '7.0.15'), false)
      assert.strictEqual(satisfied(single, 'redis', '7.2.0'), true)
      assert.strictEqual(satisfied(single, 'redis', '7.4.4'), true)
      assert.strictEqual(satisfied(single, 'redis', '10.0.0'), true)
    }
  })

  test('list order does not matter', () => {
    const shuffled: VersionGate = { redis: ['8.2.9', '7.2.16', '7.4.11'] }
    assert.strictEqual(satisfied(shuffled, 'redis', '7.2.15'), false)
    assert.strictEqual(satisfied(shuffled, 'redis', '7.4.11'), true)
    assert.strictEqual(satisfied(shuffled, 'redis', '8.0.6'), false)
    assert.strictEqual(satisfied(shuffled, 'redis', '8.4.0'), true)
  })

  test('the presets stay below both patch-level gates', () => {
    for (const preset of [
      'redis-6.2',
      'redis-7.0',
      'redis-7.2',
      'redis-7.4',
      'redis-8.0',
      'valkey-8.0',
      'valkey-9.0',
    ] as const) {
      const profile = resolveCompatibilityProfile(preset)
      assert.strictEqual(
        profile.has('command.incomplete-stream-geo-key-specs'),
        false,
        preset,
      )
      assert.strictEqual(profile.has('protocol.strict-crlf'), false, preset)
    }
  })
})

describe('incomplete XREAD / GEORADIUS key specs', () => {
  const NEW: readonly string[] = [
    '7.2.16',
    '7.2.17',
    '7.4.11',
    '7.4.12',
    '8.2.9',
    '8.2.10',
    '8.4.6',
    '8.6.6',
    '8.8.2',
    '8.10.0',
  ]
  const OLD: readonly CompatibilitySpec[] = [
    { flavor: 'redis', version: '7.0.15' },
    { flavor: 'redis', version: '7.2.15' },
    { flavor: 'redis', version: '7.4.10' },
    { flavor: 'redis', version: '8.0.5' },
    { flavor: 'redis', version: '8.0.6' },
    { flavor: 'redis', version: '8.2.0' },
    { flavor: 'redis', version: '8.2.8' },
    { flavor: 'redis', version: '8.4.5' },
    { flavor: 'redis', version: '8.6.5' },
    { flavor: 'redis', version: '8.8.1' },
    'redis-7.2',
    'redis-7.4',
    'redis-8.0',
  ]

  function session(compatibility: CompatibilitySpec): ClientSession {
    const server = new RedisServerState({ compatibility, databaseCount: 16 })
    const executor = createRedisCommandExecutor({
      compatibility: server.profile,
    })
    return new ClientSession({ server, executor })
  }

  async function command(s: ClientSession, ...args: string[]) {
    const result = await s.execute(
      'COMMAND',
      args.map(arg => Buffer.from(arg)),
    )
    return encodeRedisValue(result.value).toString()
  }

  const INVALID = '-ERR Invalid arguments specified for command\r\n'
  const keys = (...names: string[]) =>
    `*${names.length}\r\n` + names.map(n => `$${n.length}\r\n${n}\r\n`).join('')
  const RO = '*2\r\n+RO\r\n+access\r\n'
  const OW = '*2\r\n+OW\r\n+update\r\n'
  const flagged = (...pairs: [string, string][]) =>
    `*${pairs.length}\r\n` +
    pairs.map(([k, f]) => `*2\r\n$${k.length}\r\n${k}\r\n${f}`).join('')

  const GEO = ['GEORADIUS', 'k', '0', '0', '1', 'km']
  const BYMEMBER = ['GEORADIUSBYMEMBER', 'k', 'm', '1', 'km']

  // [args, before the backport, from it on]; real replies from
  // redis-server 7.2.15 / 7.2.16, 7.4.10 / 7.4.11, 8.0.6, 8.2.8 / 8.2.9 and
  // 8.4.5 / 8.4.6.
  const ROWS: [string[], string, string][] = [
    [['GETKEYS', 'XREAD', 'STREAMS', 'a', 'b', '0'], keys('a'), INVALID],
    [
      ['GETKEYSANDFLAGS', 'XREAD', 'STREAMS', 'a', 'b', '0'],
      flagged(['a', RO]),
      INVALID,
    ],
    [
      ['GETKEYS', 'XREAD', 'COUNT', '1', 'STREAMS', 'STREAMS', 'a', '0'],
      keys('STREAMS'),
      INVALID,
    ],
    [
      ['GETKEYSANDFLAGS', 'XREAD', 'STREAMS', 'a', 'b', '0', '0'],
      flagged(['a', RO], ['b', RO]),
      flagged(['a', RO], ['b', RO]),
    ],
    [
      ['GETKEYS', 'XREAD', 'BLOCK', '10', 'STREAMS', 'a', '0'],
      keys('a'),
      keys('a'),
    ],
    [
      ['GETKEYS', 'XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', 'a', 'b', '0'],
      keys('a'),
      INVALID,
    ],
    [
      ['GETKEYSANDFLAGS', 'XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', 'a', '0'],
      flagged(['a', RO]),
      flagged(['a', RO]),
    ],
    [
      ['GETKEYSANDFLAGS', ...GEO, 'STORE', 'd', 'STORE', 'e'],
      flagged(['k', RO], ['d', OW]),
      flagged(['k', RO], ['e', OW]),
    ],
    [
      ['GETKEYSANDFLAGS', ...GEO, 'STORE', 'd', 'STOREDIST', 'e'],
      flagged(['k', RO], ['d', OW], ['e', OW]),
      flagged(['k', RO], ['e', OW]),
    ],
    [
      ['GETKEYSANDFLAGS', ...BYMEMBER, 'STORE', 'd', 'STORE', 'e'],
      flagged(['k', RO], ['d', OW]),
      flagged(['k', RO], ['e', OW]),
    ],
    [
      ['GETKEYSANDFLAGS', ...GEO, 'STORE', 'd'],
      flagged(['k', RO], ['d', OW]),
      flagged(['k', RO], ['d', OW]),
    ],
    [
      ['GETKEYSANDFLAGS', ...BYMEMBER, 'STOREDIST', 'd'],
      flagged(['k', RO], ['d', OW]),
      flagged(['k', RO], ['d', OW]),
    ],
    [
      ['GETKEYSANDFLAGS', ...GEO, 'STORE'],
      flagged(['k', RO]),
      flagged(['k', RO]),
    ],
  ]

  for (const version of NEW) {
    test(`redis ${version} falls back to the getkeys procedure`, async () => {
      const s = session({ flavor: 'redis', version })
      for (const [args, , expected] of ROWS) {
        assert.strictEqual(await command(s, ...args), expected, args.join(' '))
      }
    })
  }

  for (const spec of OLD) {
    test(`${JSON.stringify(spec)} reads the key specs`, async () => {
      const s = session(spec)
      for (const [args, expected] of ROWS) {
        assert.strictEqual(await command(s, ...args), expected, args.join(' '))
      }
    })
  }

  const XREAD_SPEC =
    '*8\r\n$5\r\nnotes\r\n$125\r\nIncomplete because a stream key named STREAMS (or options before it) can shift the STREAMS keyword; fall back to xreadGetKeys\r\n$5\r\nflags\r\n*3\r\n+RO\r\n+access\r\n+incomplete\r\n$12\r\nbegin_search\r\n'
  // COMMAND INFO xreadgroup georadiusbymember, byte for byte the same on
  // redis-server 7.2.16, 7.4.11 and 8.4.6.
  const INFO_NEW =
    '*2\r\n*10\r\n$10\r\nxreadgroup\r\n:-7\r\n*3\r\n+write\r\n+blocking\r\n+movablekeys\r\n:0\r\n:0\r\n:0\r\n*4\r\n+@write\r\n+@stream\r\n+@slow\r\n+@blocking\r\n*0\r\n*1\r\n*8\r\n$5\r\nnotes\r\n$132\r\nIncomplete because a consumer/group named STREAMS (or options before GROUP) can shift the STREAMS keyword; fall back to xreadGetKeys\r\n$5\r\nflags\r\n*3\r\n+RO\r\n+access\r\n+incomplete\r\n$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$7\r\nkeyword\r\n$4\r\nspec\r\n*4\r\n$7\r\nkeyword\r\n$7\r\nSTREAMS\r\n$9\r\nstartfrom\r\n:4\r\n$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:-1\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:2\r\n*0\r\n' +
    '*10\r\n$17\r\ngeoradiusbymember\r\n:-5\r\n*3\r\n+write\r\n+denyoom\r\n+movablekeys\r\n:1\r\n:1\r\n:1\r\n*3\r\n+@write\r\n+@geo\r\n+@slow\r\n*0\r\n*3\r\n*6\r\n$5\r\nflags\r\n*2\r\n+RO\r\n+access\r\n$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n*2\r\n$5\r\nindex\r\n:1\r\n$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n' +
    '*8\r\n$5\r\nnotes\r\n$87\r\nIncomplete because duplicate STORE options use last-wins; fall back to georadiusGetKeys\r\n$5\r\nflags\r\n*3\r\n+OW\r\n+update\r\n+incomplete\r\n$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$7\r\nkeyword\r\n$4\r\nspec\r\n*4\r\n$7\r\nkeyword\r\n$5\r\nSTORE\r\n$9\r\nstartfrom\r\n:5\r\n$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n' +
    '*8\r\n$5\r\nnotes\r\n$91\r\nIncomplete because duplicate STOREDIST options use last-wins; fall back to georadiusGetKeys\r\n$5\r\nflags\r\n*3\r\n+OW\r\n+update\r\n+incomplete\r\n$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$7\r\nkeyword\r\n$4\r\nspec\r\n*4\r\n$7\r\nkeyword\r\n$9\r\nSTOREDIST\r\n$9\r\nstartfrom\r\n:5\r\n$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n'

  for (const version of ['7.2.16', '7.4.11', '8.4.6']) {
    test(`redis ${version} COMMAND INFO shows the incomplete specs`, async () => {
      const s = session({ flavor: 'redis', version })
      assert.strictEqual(
        await command(s, 'INFO', 'xreadgroup', 'georadiusbymember'),
        INFO_NEW,
      )
      assert.ok((await command(s, 'INFO', 'xread')).includes(XREAD_SPEC))
      const georadius = await command(s, 'INFO', 'georadius', 'georadius_ro')
      assert.strictEqual(georadius.split('+incomplete').length - 1, 2)
    })
  }

  for (const spec of OLD) {
    test(`${JSON.stringify(spec)} COMMAND INFO has no incomplete specs`, async () => {
      const s = session(spec)
      const info = await command(
        s,
        'INFO',
        'xread',
        'xreadgroup',
        'georadius',
        'georadiusbymember',
      )
      assert.ok(!info.includes('incomplete'))
      assert.ok(!info.includes('notes'))
    })
  }

  test('valkey 7.2.15 and later keep their own key specs', async () => {
    // valkey-server 7.2.15 answers `*1 a` here and has no `incomplete` spec.
    for (const version of ['7.2.15', '8.0.11', '9.0.6']) {
      const s = session({ flavor: 'valkey', version })
      assert.strictEqual(
        await command(s, 'GETKEYS', 'XREAD', 'STREAMS', 'a', 'b', '0'),
        keys('a'),
        version,
      )
      assert.ok(!(await command(s, 'INFO', 'xread')).includes('incomplete'))
    }
  })
})

describe('Valkey CRLF check', () => {
  const INVALID_CRLF = 'Protocol error: invalid CRLF in request'
  // A wrong byte after the count line's CR, after a bulk length's CR, and in
  // place of a bulk's terminator.
  const FRAMES = [
    '*1\rX$4\r\nPING\r\n',
    '*1\r\n$4\rXPING\r\n',
    '*1\r\n$4\r\nPINGXX',
  ]

  function decoder(spec: CompatibilitySpec): Resp2CommandDecoder {
    return new Resp2CommandDecoder({
      maxBulkLength: () => 512n * 1024n * 1024n,
      profile: resolveCompatibilityProfile(spec),
    })
  }

  // Refused on valkey-server 7.2.14, 8.0.10, 8.1.8 and 9.0.5; 8.0.8 and 9.1.0
  // from the tagged source.
  for (const version of [
    '7.2.14',
    '7.2.15',
    '8.0.8',
    '8.0.11',
    '8.1.8',
    '9.0.5',
    '9.1.0',
  ]) {
    test(`valkey ${version} refuses a wrong CRLF`, () => {
      for (const frame of FRAMES) {
        const d = decoder({ flavor: 'valkey', version })
        d.push(Buffer.from(frame))
        assert.throws(
          () => d.next(),
          (err: unknown) =>
            err instanceof Resp2ParseError && err.message === INVALID_CRLF,
          frame,
        )
      }
    })
  }

  // Answered `+PONG` by valkey-server 7.2.13, 8.0.7, 8.1.7, 9.0.0, 9.0.4 and
  // every redis-server.
  for (const spec of [
    { flavor: 'valkey', version: '7.2.13' },
    { flavor: 'valkey', version: '8.0.7' },
    { flavor: 'valkey', version: '8.1.7' },
    { flavor: 'valkey', version: '9.0.4' },
    { flavor: 'redis', version: '7.2.16' },
    { flavor: 'redis', version: '8.4.6' },
    'valkey-8.0',
    'valkey-9.0',
    'redis-8.0',
  ] as const) {
    test(`${JSON.stringify(spec)} skips the bytes unread`, () => {
      for (const frame of FRAMES) {
        const d = decoder(spec)
        d.push(Buffer.from(frame))
        const parsed = d.next()
        assert.ok(parsed && 'command' in parsed, frame)
        assert.strictEqual(parsed.command.toString(), 'PING', frame)
      }
    })
  }

  test('a well-formed request still parses on valkey 9.0.5', () => {
    const d = decoder({ flavor: 'valkey', version: '9.0.5' })
    d.push(Buffer.from('*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n'))
    const parsed = d.next()
    assert.ok(parsed && 'command' in parsed)
    assert.strictEqual(parsed.args[0]!.toString(), 'hi')
  })

  test('valkey 9.0.5 checks the terminator of a reserved large bulk', () => {
    const length = 1024 * 1024
    const header = `*2\r\n$4\r\nECHO\r\n$${length}\r\n`
    const good = decoder({ flavor: 'valkey', version: '9.0.5' })
    good.push(Buffer.from(header))
    assert.strictEqual(good.next(), null)
    good.push(Buffer.alloc(length, 'a'))
    good.push(Buffer.from('\r\n'))
    const parsed = good.next()
    assert.ok(parsed && 'command' in parsed)
    assert.strictEqual(parsed.args[0]!.length, length)

    const bad = decoder({ flavor: 'valkey', version: '9.0.5' })
    bad.push(Buffer.from(header))
    assert.strictEqual(bad.next(), null)
    bad.push(Buffer.alloc(length, 'a'))
    bad.push(Buffer.from('XX'))
    assert.throws(
      () => bad.next(),
      (err: unknown) =>
        err instanceof Resp2ParseError && err.message === INVALID_CRLF,
    )
  })
})
