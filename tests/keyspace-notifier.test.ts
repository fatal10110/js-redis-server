import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  keyspaceNotifyFlagsToString,
  normalizeKeyspaceNotifyConfig,
  parseKeyspaceNotifyFlags,
} from '../src/state/keyspace-notifier'

describe('keyspace notify flag parsing', () => {
  test('parses individual class characters', () => {
    const flags = parseKeyspaceNotifyFlags('KEg$x')
    assert.strictEqual(flags.keyspace, true)
    assert.strictEqual(flags.keyevent, true)
    assert.strictEqual(flags.generic, true)
    assert.strictEqual(flags.string, true)
    assert.strictEqual(flags.expired, true)
    assert.strictEqual(flags.list, false)
  })

  test("'A' expands to every class except m and n (module IS included)", () => {
    const flags = parseKeyspaceNotifyFlags('A')
    for (const key of [
      'generic',
      'string',
      'list',
      'set',
      'hash',
      'zset',
      'expired',
      'evicted',
      'stream',
      'module',
    ] as const) {
      assert.strictEqual(flags[key], true, `${key} should be set by A`)
    }
    assert.strictEqual(flags.keyMiss, false)
    assert.strictEqual(flags.newKey, false)
  })

  test('rejects an unknown class character with the Redis error', () => {
    assert.throws(
      () => parseKeyspaceNotifyFlags('Z'),
      /Invalid event class character. Use 'Ag\$lshzxeKEtmdn'\./,
    )
  })
})

describe('keyspace notify flag normalization', () => {
  // Each pair mirrors output observed from a real Redis CONFIG SET/GET probe.
  const cases: [string, string][] = [
    ['', ''],
    ['KEA', 'AKE'],
    ['AKE', 'AKE'],
    ['gxE', 'gxE'],
    ['KEg$', 'g$KE'],
    ['Elx', 'lxE'],
    ['KExe', 'xeKE'],
    ['AKEt', 'AKE'],
    // m / n / d edge cases (module is part of 'A'; n only when not collapsed;
    // m always last). Verified against redis-server 7.2.14.
    ['Km', 'Km'],
    ['KEm', 'KEm'],
    ['Ad', 'A'],
    ['g$lshzxet', 'g$lshzxet'],
    ['g$lshzxetd', 'A'],
    ['KEn', 'nKE'],
    ['And', 'A'],
    ['KEgnd$', 'g$dnKE'],
    ['Adm', 'Am'],
    ['dKEmn', 'dnKEm'],
  ]

  for (const [input, expected] of cases) {
    test(`'${input}' normalizes to '${expected}'`, () => {
      assert.strictEqual(normalizeKeyspaceNotifyConfig(input), expected)
    })
  }

  test('round-trips a parsed value back to canonical form', () => {
    const flags = parseKeyspaceNotifyFlags('Ex')
    assert.strictEqual(keyspaceNotifyFlagsToString(flags), 'xE')
  })
})
