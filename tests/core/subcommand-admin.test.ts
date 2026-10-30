import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  containerSubcommandExists,
  containerSubcommandIsAdmin,
} from '../../src/core/compatibility/subcommand-gates'
import { resolveCompatibilityProfile } from '../../src/core/compatibility'
import { commandTableEntry } from '../../src/core/compatibility/command-table'
import { redisCommandDefinitions } from '../../src/commands'

const PROFILES = [
  'redis-7.0',
  'redis-7.2',
  'redis-7.4',
  'redis-8.0',
  'valkey-8.0',
  'valkey-9.0',
] as const

describe('containerSubcommandIsAdmin', () => {
  test('agrees with every captured subcommand entry', () => {
    let checked = 0
    for (const definition of redisCommandDefinitions) {
      for (const sub of definition.introspection?.subcommands ?? []) {
        const [container, subcommand] = sub.name.split('|')
        for (const name of PROFILES) {
          const profile = resolveCompatibilityProfile(name)
          const entry = commandTableEntry(sub.name, profile)
          if (
            !entry ||
            containerSubcommandExists(container, subcommand, profile) !== true
          ) {
            continue
          }
          assert.strictEqual(
            containerSubcommandIsAdmin(container, subcommand),
            entry.flags.includes('admin'),
            `${sub.name} on ${name}`,
          )
          checked++
        }
      }
    }
    assert.ok(checked > 0)
  })

  test('flags the admin subcommands without a captured entry (#613)', () => {
    const redis80 = resolveCompatibilityProfile('redis-8.0')
    for (const name of [
      'cluster|meet',
      'cluster|count-failure-reports',
      'cluster|setslot',
      'acl|setuser',
      'client|unblock',
    ]) {
      assert.strictEqual(commandTableEntry(name, redis80), undefined, name)
      const [container, subcommand] = name.split('|')
      assert.strictEqual(
        containerSubcommandIsAdmin(container, subcommand),
        true,
      )
    }
    for (const name of ['cluster|keyslot', 'cluster|links', 'acl|whoami']) {
      const [container, subcommand] = name.split('|')
      assert.strictEqual(
        containerSubcommandIsAdmin(container, subcommand),
        false,
        name,
      )
    }
    assert.strictEqual(containerSubcommandIsAdmin('CLUSTER', 'Meet'), true)
    assert.strictEqual(containerSubcommandIsAdmin('get', 'x'), false)
    assert.strictEqual(containerSubcommandIsAdmin('cluster', 'toString'), false)
  })
})
