import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  atoi,
  keysFromKeySpecs,
  numkeysGetKeys,
  rawCommandKeys,
} from '../../src/core/key-specs'
import { lookupTableArity } from '../../src/core/command-arity'
import { containerSubcommandArity } from '../../src/core/compatibility/subcommand-gates'
import { resolveCompatibilityProfile } from '../../src/core/compatibility'
import {
  defineCommand,
  type CommandKeySpec,
} from '../../src/core/command-definition'
import { t } from '../../src/core/command-schema'
import { RedisResult } from '../../src/core/redis-result'
import { redisCommandDefinitions } from '../../src/commands'

const redis62 = resolveCompatibilityProfile('redis-6.2')
const redis70 = resolveCompatibilityProfile('redis-7.0')
const redis80 = resolveCompatibilityProfile('redis-8.0')
const valkey90 = resolveCompatibilityProfile('valkey-9.0')

function argv(...args: string[]): Buffer[] {
  return args.map(arg => Buffer.from(arg))
}

function definition(name: string) {
  const found = redisCommandDefinitions.find(entry => entry.name === name)
  assert.ok(found, name)
  return found
}

function routed(name: string, args: string[], profile = redis80): string[] {
  return rawCommandKeys(definition(name), name, argv(...args), profile).map(
    key => key.toString(),
  )
}

// Rows follow what a real redis-server 8.0.6 cluster routes for a command
// queued inside MULTI whose own parser rejects it (#518): Redis's
// getKeysFromCommand, i.e. the getkeys proc, else the legacy key range.
describe('rawCommandKeys', () => {
  test('numkeys procs read the count with atoi', () => {
    assert.deepStrictEqual(routed('zunionstore', ['d', '2abc', 'a', 'b']), [
      'a',
      'b',
      'd',
    ])
    assert.deepStrictEqual(routed('eval', ['return 1', '1x', 'a']), ['a'])
    assert.deepStrictEqual(routed('zunion', ['1x', 'a']), ['a'])
    assert.deepStrictEqual(routed('lmpop', ['1x', 'a', 'BOGUS']), ['a'])
  })

  test('a count below 1 or past the end of the command routes nothing', () => {
    assert.deepStrictEqual(routed('zunionstore', ['d', '5', 'a']), [])
    assert.deepStrictEqual(routed('zunionstore', ['d', '0', 'a']), [])
    assert.deepStrictEqual(routed('zunionstore', ['d', 'x', 'a']), [])
    assert.deepStrictEqual(routed('eval', ['return 1', '2', 'a']), [])
  })

  test('XREAD / XREADGROUP: the first half after STREAMS, or nothing', () => {
    assert.deepStrictEqual(
      routed('xread', ['COUNT', 'x', 'STREAMS', 'a', '0']),
      ['a'],
    )
    // An odd tail, an unknown option, or STREAMS as a key: keyless.
    assert.deepStrictEqual(routed('xread', ['STREAMS', 'a', 'b', '0']), [])
    assert.deepStrictEqual(routed('xread', ['BOGUS', 'STREAMS', 'a', '0']), [])
    assert.deepStrictEqual(
      routed('xread', ['COUNT', '1', 'STREAMS', 'STREAMS', 'a', '0']),
      [],
    )
    assert.deepStrictEqual(
      routed('xreadgroup', ['GROUP', 'g', 'c', 'STREAMS', 'a', 'b', '0']),
      [],
    )
    assert.deepStrictEqual(
      routed('xreadgroup', ['GROUP', 'g', 'c', 'STREAMS', 'a', '>']),
      ['a'],
    )
  })

  test('GEORADIUS* add the STORE destination found from argument 5', () => {
    assert.deepStrictEqual(
      routed('georadius', ['k', '0', '0', '1', 'km', 'STORE', 'd', 'X']),
      ['k', 'd'],
    )
    assert.deepStrictEqual(
      routed('georadiusbymember', ['k', 'm', 'x', 'km', 'STORE', 'd']),
      ['k', 'd'],
    )
    // `STORE` at argument 4 is before the search window.
    assert.deepStrictEqual(
      routed('georadiusbymember', ['k', 'm', '1', 'STORE', 'd']),
      ['k'],
    )
  })

  test('without a proc, the legacy key range of the entry lookup resolves', () => {
    assert.deepStrictEqual(routed('mset', ['a', 'b', 'c']), ['a', 'c'])
    assert.deepStrictEqual(routed('hset', ['h', 'f', 'v', 'x']), ['h'])
    // 7.0+: the subcommand entry's range; 6.2: the container's own 2,2,1.
    assert.deepStrictEqual(routed('xinfo', ['STREAM', 'a', 'x']), ['a'])
    assert.deepStrictEqual(routed('xinfo', ['STREAM', 'a', 'x'], redis62), [
      'a',
    ])
    assert.deepStrictEqual(routed('xinfo', ['HELP']), [])
  })

  test('atoi', () => {
    assert.strictEqual(atoi(Buffer.from('2abc')), 2)
    assert.strictEqual(atoi(Buffer.from('  -3')), -3)
    assert.strictEqual(atoi(Buffer.from('x')), 0)
    assert.deepStrictEqual(numkeysGetKeys(0, 1, 2)(argv('c', '9', 'a')), [])
  })
})

describe('keysFromKeySpecs', () => {
  test('each key carries the flags of the spec that found it', () => {
    const specs = definition('zunionstore').introspection?.keySpecs ?? []
    assert.deepStrictEqual(
      keysFromKeySpecs(specs, argv('zunionstore', 'd', '2', 'a', 'b'))?.map(
        ({ key, flags }) => [key.toString(), flags],
      ),
      [
        ['d', ['OW', 'update']],
        ['a', ['RO', 'access']],
        ['b', ['RO', 'access']],
      ],
    )
  })

  test('a spec that cannot be applied fails the lookup', () => {
    const specs = definition('zunionstore').introspection?.keySpecs ?? []
    assert.strictEqual(
      keysFromKeySpecs(specs, argv('zunionstore', 'd', '2abc', 'a', 'b')),
      null,
    )
    // A step below 1 is invalid rather than an endless loop.
    const zeroStep: CommandKeySpec = {
      flags: [],
      beginSearchIndex: 1,
      lastKey: -1,
      keyStep: 0,
    }
    assert.strictEqual(keysFromKeySpecs([zeroStep], argv('c', 'a', 'b')), null)
  })

  test('a keyword searched backwards from the end (MIGRATE-style)', () => {
    const spec: CommandKeySpec = {
      flags: [],
      beginSearchIndex: 0,
      beginSearchKeyword: { keyword: 'KEYS', startFrom: -3 },
      lastKey: -1,
      keyStep: 1,
    }
    assert.deepStrictEqual(
      keysFromKeySpecs([spec], argv('cmd', 'x', 'y', 'KEYS', 'a', 'b'))?.map(
        ({ key }) => key.toString(),
      ),
      ['a', 'b'],
    )
  })
})

describe('container subcommand arity', () => {
  test('lookup uses the real subcommand entry from 7.0', () => {
    const xinfo = definition('xinfo')
    assert.deepStrictEqual(lookupTableArity(xinfo, argv('STREAM'), redis80), {
      name: 'xinfo|stream',
      arity: -3,
    })
    // 6.2 has only the container's entry.
    assert.deepStrictEqual(lookupTableArity(xinfo, argv('STREAM'), redis62), {
      name: 'xinfo',
      arity: -2,
    })
  })

  test('a custom container falls back to the subcommands it declares', () => {
    const custom = defineCommand({
      name: 'mycontainer',
      schema: t.object({ args: t.variadic(t.bulk()) }),
      flags: [],
      introspection: {
        arity: -2,
        subcommands: [{ name: 'mycontainer|get', arity: 3 }],
      },
      keys: () => [],
      execute: () => RedisResult.ok(),
    })
    assert.deepStrictEqual(lookupTableArity(custom, argv('GET'), redis80), {
      name: 'mycontainer|get',
      arity: 3,
    })
    assert.deepStrictEqual(lookupTableArity(custom, argv('GET'), redis62), {
      name: 'mycontainer',
      arity: -2,
    })
  })

  test('version differences', () => {
    assert.strictEqual(
      containerSubcommandArity('command', 'getkeys', redis70),
      -4,
    )
    assert.strictEqual(
      containerSubcommandArity('command', 'getkeys', redis80),
      -3,
    )
    assert.strictEqual(
      containerSubcommandArity('cluster', 'replicate', redis80),
      3,
    )
    assert.strictEqual(
      containerSubcommandArity('cluster', 'replicate', valkey90),
      -3,
    )
    assert.strictEqual(
      containerSubcommandArity('client', 'capa', redis80),
      undefined,
    )
    assert.strictEqual(containerSubcommandArity('client', 'capa', valkey90), -3)
    // Redis 8.4-only entries have their arity too (8.4.7).
    const redis84 = resolveCompatibilityProfile({
      flavor: 'redis',
      version: '8.4.0',
    })
    assert.strictEqual(
      containerSubcommandArity('cluster', 'migration', redis84),
      -4,
    )
  })

  test('every COMMAND INFO subcommand entry agrees with the real table', () => {
    for (const profile of [redis70, redis80, valkey90]) {
      for (const command of redisCommandDefinitions) {
        for (const sub of command.introspection?.subcommands ?? []) {
          const [container, name] = (sub.name ?? '').split('|')
          if (!name) continue
          const real = containerSubcommandArity(container, name, profile)
          if (real === undefined) continue
          const declared =
            typeof sub.arity === 'function' ? sub.arity(profile) : sub.arity
          assert.strictEqual(
            declared,
            real,
            `${sub.name} on ${profile.version}`,
          )
        }
      }
    }
  })
})
