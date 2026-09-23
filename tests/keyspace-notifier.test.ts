import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  keyspaceNotifyFlagsToString,
  parseKeyspaceNotifyFlags,
} from '../src/state/keyspace-notifier'

describe('keyspace notify flag parsing', () => {
  test('parses individual class characters', () => {
    const flags = parseKeyspaceNotifyFlags('KEg$x')
    assert.deepStrictEqual(flags, new Set(['K', 'E', 'g', '$', 'x']))
  })

  test("'A' expands to every class except m and n (module IS included)", () => {
    assert.deepStrictEqual(
      parseKeyspaceNotifyFlags('A'),
      new Set(['g', '$', 'l', 's', 'h', 'z', 'x', 'e', 't', 'd']),
    )
  })

  test('the empty string parses to no flags', () => {
    assert.deepStrictEqual(parseKeyspaceNotifyFlags(''), new Set())
  })

  // The caller owns the (profile-specific) CONFIG SET error.
  test('an unknown class character is rejected', () => {
    for (const value of ['Z', 'Xz', 'KEy', 'K E', 'KEA!']) {
      assert.strictEqual(parseKeyspaceNotifyFlags(value), undefined, value)
    }
  })

  // `n` is Redis 7.0+; 6.2 rejects it but accepts `m` and `d`.
  test('newKeyClass: false rejects only the n flag', () => {
    for (const value of ['n', 'KEn', 'And', 'KEnd']) {
      const flags = parseKeyspaceNotifyFlags(value, { newKeyClass: false })
      assert.strictEqual(flags, undefined, value)
    }
    assert.deepStrictEqual(
      parseKeyspaceNotifyFlags('KEmd', { newKeyClass: false }),
      new Set(['K', 'E', 'm', 'd']),
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
      const flags = parseKeyspaceNotifyFlags(input)
      assert.ok(flags)
      assert.strictEqual(keyspaceNotifyFlagsToString(flags), expected)
    })
  }
})
