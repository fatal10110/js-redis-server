import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
  encodeRedisValue,
  resolveCompatibilityProfile,
  type CompatibilitySpec,
} from '../../src/internal'
import { Resp2CommandDecoder } from '../../src/core/transports/resp2/decoder'

/**
 * Where the valkey-8.0 / valkey-9.0 presets (8.0.0 / 9.0.0) answer unlike the
 * later Valkey patches the command table is captured from (#522). Every
 * reply was probed on the named valkey-server releases.
 */

function session(compatibility: CompatibilitySpec): ClientSession {
  const server = new RedisServerState({ compatibility, databaseCount: 16 })
  const executor = createRedisCommandExecutor({
    compatibility: server.profile,
  })
  return new ClientSession({ server, executor })
}

async function run(
  s: ClientSession,
  version: 2 | 3,
  name: string,
  ...args: string[]
): Promise<string> {
  const result = await s.execute(
    name,
    args.map(arg => Buffer.from(arg)),
  )
  return encodeRedisValue(result.value, { version }).toString('latin1')
}

const valkey = (version: string): CompatibilitySpec => ({
  flavor: 'valkey',
  version,
})

describe('COMMAND INFO subcommands field (RESP3)', () => {
  // valkey-server 8.0.9 / 8.0.10, 9.0.4 / 9.0.5, 9.1.0 / 9.1.1.
  for (const [spec, tail] of [
    ['valkey-8.0', '~0\r\n'],
    ['valkey-9.0', '~0\r\n'],
    [valkey('8.0.9'), '~0\r\n'],
    [valkey('8.0.10'), '*0\r\n'],
    [valkey('9.0.4'), '~0\r\n'],
    [valkey('9.0.5'), '*0\r\n'],
    [valkey('9.1.0'), '~0\r\n'],
    [valkey('9.1.1'), '*0\r\n'],
    ['redis-8.0', '~0\r\n'],
  ] as const) {
    test(`${JSON.stringify(spec)} ends GET's entry in ${JSON.stringify(tail)}`, async () => {
      const reply = await run(session(spec), 3, 'COMMAND', 'INFO', 'get')
      assert.ok(reply.endsWith(`:0\r\n${tail}`), reply)
    })
  }
})

describe('GEORADIUS STORE / STOREDIST getkeys', () => {
  const GEO = ['GEORADIUS', 'k', '0', '0', '1', 'km']
  const keys = (...names: string[]) =>
    `*${names.length}\r\n` + names.map(n => `$${n.length}\r\n${n}\r\n`).join('')
  const RO = '*2\r\n+RO\r\n+access\r\n'
  const OW = '*2\r\n+OW\r\n+update\r\n'
  const flagged = (...pairs: [string, string][]) =>
    `*${pairs.length}\r\n` +
    pairs.map(([k, f]) => `*2\r\n$${k.length}\r\n${k}\r\n${f}`).join('')

  // valkey-server 8.0.0 - 8.0.10 and 9.0.0 - 9.0.5 read both key specs;
  // 8.0.11 / 9.0.6 ask georadiusGetKeys (last destination wins).
  for (const spec of ['valkey-8.0', 'valkey-9.0', valkey('9.0.5')] as const) {
    test(`${JSON.stringify(spec)} reads both key specs`, async () => {
      const s = session(spec)
      assert.strictEqual(
        await run(
          s,
          2,
          'COMMAND',
          'GETKEYS',
          ...GEO,
          'STORE',
          'a',
          'STOREDIST',
          'b',
        ),
        keys('k', 'a', 'b'),
      )
      assert.strictEqual(
        await run(
          s,
          2,
          'COMMAND',
          'GETKEYSANDFLAGS',
          ...GEO,
          'STORE',
          'd',
          'STORE',
          'e',
        ),
        flagged(['k', RO], ['d', OW]),
      )
      assert.ok(
        !(await run(s, 2, 'COMMAND', 'INFO', 'georadius')).includes(
          'variable_flags',
        ),
      )
    })
  }

  for (const spec of [valkey('8.0.11'), valkey('9.0.6')]) {
    test(`${JSON.stringify(spec)} asks georadiusGetKeys`, async () => {
      const s = session(spec)
      assert.strictEqual(
        await run(
          s,
          2,
          'COMMAND',
          'GETKEYS',
          ...GEO,
          'STORE',
          'a',
          'STOREDIST',
          'b',
        ),
        keys('k', 'b'),
      )
      assert.strictEqual(
        await run(
          s,
          2,
          'COMMAND',
          'GETKEYSANDFLAGS',
          ...GEO,
          'STORE',
          'd',
          'STORE',
          'e',
        ),
        flagged(['k', RO], ['e', OW]),
      )
    })
  }
})

describe('CLIENT INFO multi= after a transaction', () => {
  // valkey-server 8.0.0 / 8.0.12 show -1; 8.1.0 and 9.0.x show 0 once MULTI
  // or WATCH allocated the multi state.
  for (const [spec, afterward] of [
    ['valkey-8.0', -1],
    ['redis-8.0', -1],
    ['valkey-9.0', 0],
    [valkey('8.1.0'), 0],
  ] as const) {
    test(`${JSON.stringify(spec)}: ${afterward}`, async () => {
      const watched = session(spec)
      assert.strictEqual(watched.clientInfoMultiCount, -1)
      await run(watched, 2, 'WATCH', 'k')
      assert.strictEqual(watched.clientInfoMultiCount, afterward)

      const multi = session(spec)
      await run(multi, 2, 'MULTI')
      await run(multi, 2, 'PING')
      assert.strictEqual(multi.clientInfoMultiCount, 1)
      await run(multi, 2, 'EXEC')
      assert.strictEqual(multi.clientInfoMultiCount, afterward)
    })
  }
})

describe('inline requests and raw 0xFF bytes', () => {
  function inlineArgs(line: string, spec: CompatibilitySpec): string[] {
    const d = new Resp2CommandDecoder({
      maxBulkLength: () => 512n * 1024n * 1024n,
      profile: resolveCompatibilityProfile(spec),
    })
    d.push(Buffer.from(`${line}\r\n`, 'latin1'))
    const frame = d.next()
    assert.ok(frame)
    return [frame.command, ...frame.args].map(b => b.toString('latin1'))
  }

  // valkey-server 8.1.0, 9.0.0 - 9.0.7 and 9.1.3 drop them; 8.0 keeps them.
  for (const spec of ['valkey-9.0', valkey('8.1.0')] as const) {
    test(`${JSON.stringify(spec)} drops them`, () => {
      assert.deepStrictEqual(inlineArgs('ECHO \xe9\xff', spec), [
        'ECHO',
        '\xe9',
      ])
      assert.deepStrictEqual(inlineArgs('ECHO a\xffb', spec), ['ECHO', 'ab'])
      assert.deepStrictEqual(inlineArgs('ECHO "\xe9\xff"', spec), [
        'ECHO',
        '\xe9',
      ])
      assert.deepStrictEqual(inlineArgs("ECHO '\xff'", spec), ['ECHO', ''])
      assert.deepStrictEqual(inlineArgs('ECHO "\\\xff"', spec), ['ECHO', ''])
      assert.deepStrictEqual(inlineArgs('ECHO "\\xff"', spec), ['ECHO', '\xff'])
    })
  }

  for (const spec of ['valkey-8.0', 'redis-8.0'] as const) {
    test(`${spec} keeps them`, () => {
      assert.deepStrictEqual(inlineArgs('ECHO \xe9\xff', spec), [
        'ECHO',
        '\xe9\xff',
      ])
      assert.deepStrictEqual(inlineArgs('ECHO "\\\xff"', spec), [
        'ECHO',
        '\xff',
      ])
    })
  }
})
