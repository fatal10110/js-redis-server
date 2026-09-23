import { describe, test } from 'node:test'
import assert from 'node:assert'
import { keysFromKeySpecs, rawCommandKeys } from '../../src/core/key-specs'
import { lookupTableArity } from '../../src/core/command-arity'
import { containerSubcommandArity } from '../../src/core/compatibility/subcommand-gates'
import { resolveCompatibilityProfile } from '../../src/core/compatibility'
import type { CommandKeySpec } from '../../src/core/command-definition'
import { redisCommandDefinitions } from '../../src/commands'

function argv(...args: string[]): Buffer[] {
  return args.map(arg => Buffer.from(arg))
}

function keys(specs: readonly CommandKeySpec[], ...args: string[]): string[] {
  return keysFromKeySpecs(specs, argv(...args)).map(key => key.toString())
}

function definition(name: string) {
  const found = redisCommandDefinitions.find(entry => entry.name === name)
  assert.ok(found, name)
  return found
}

function rawKeys(name: string, ...args: string[]): string[] {
  return rawCommandKeys(definition(name), name, argv(...args)).map(key =>
    key.toString(),
  )
}

// Rows follow what real redis-server 8.0.6 routes in a cluster for a command
// whose own parser would reject it (#518).
describe('keysFromKeySpecs', () => {
  test('numkeys: the destination and the counted keys', () => {
    assert.deepStrictEqual(rawKeys('zunionstore', 'd', '2', 'a', 'b', 'X'), [
      'd',
      'a',
      'b',
    ])
  })

  test('a numkeys that runs past the end makes the command keyless', () => {
    assert.deepStrictEqual(rawKeys('zunionstore', 'd', '5', 'a'), [])
    assert.deepStrictEqual(rawKeys('eval', 'return 1', '2', 'a'), [])
  })

  test('a numkeys that is not a non-negative integer, or zero, is invalid', () => {
    assert.deepStrictEqual(rawKeys('zunionstore', 'd', 'x', 'a'), [])
    assert.deepStrictEqual(rawKeys('zunionstore', 'd', '-1', 'a'), [])
    assert.deepStrictEqual(rawKeys('zunionstore', 'd', '0', 'a'), [])
  })

  test('STREAMS: the first half of the arguments after the keyword', () => {
    assert.deepStrictEqual(
      rawKeys('xread', 'COUNT', 'x', 'STREAMS', 'a', 'b', '0', '0'),
      ['a', 'b'],
    )
    assert.deepStrictEqual(
      rawKeys('xread', 'COUNT', 'x', 'streams', 'a', 'b', '0'),
      ['a'],
    )
    assert.deepStrictEqual(rawKeys('xread', 'COUNT', 'x', 'STREAMS', 'a'), [])
    assert.deepStrictEqual(rawKeys('xread', 'COUNT', 'x', 'a', '0'), [])
  })

  test('STORE / STOREDIST add their destination when present', () => {
    assert.deepStrictEqual(
      rawKeys('georadius', 'k', '0', '0', '1', 'km', 'STORE', 'd', 'BOGUS'),
      ['k', 'd'],
    )
    assert.deepStrictEqual(rawKeys('georadius', 'k', '0', '0', 'x', 'km'), [
      'k',
    ])
  })

  test('without key specs, the schema legacy range', () => {
    assert.deepStrictEqual(rawKeys('mset', 'a', 'b', 'c'), ['a', 'c'])
    assert.deepStrictEqual(rawKeys('hset', 'h', 'f', 'v', 'x'), ['h'])
  })

  test('a keyword searched backwards from the end (MIGRATE-style)', () => {
    const spec: CommandKeySpec = {
      flags: [],
      beginSearchIndex: 0,
      beginSearchKeyword: { keyword: 'KEYS', startFrom: -3 },
      lastKey: -1,
      keyStep: 1,
    }
    assert.deepStrictEqual(keys([spec], 'cmd', 'x', 'y', 'KEYS', 'a', 'b'), [
      'a',
      'b',
    ])
    // Like Redis, the search stops before index 1.
    assert.deepStrictEqual(keys([spec], 'cmd', 'KEYS', 'a', 'b'), [])
  })
})

describe('container subcommand arity', () => {
  const redis80 = resolveCompatibilityProfile('redis-8.0')
  const redis70 = resolveCompatibilityProfile('redis-7.0')
  const redis62 = resolveCompatibilityProfile('redis-6.2')
  const valkey90 = resolveCompatibilityProfile('valkey-9.0')

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
