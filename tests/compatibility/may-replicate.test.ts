import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  createRedisCommandExecutor,
  resolveCompatibilityProfile,
  type CompatibilitySpec,
} from '../../src/internal'
import { commandTableEntry } from '../../src/core/compatibility/command-table'
import {
  mayReplicate,
  mayReplicateCommands,
} from '../../src/core/compatibility/may-replicate'

// The commands each profile's server flags CMD_MAY_REPLICATE (#568), read
// from redis 6.2.14 server.c, 7.0.15 commands.c, 7.2.4 / 7.4.4 / 8.0.0 and
// valkey 7.2.5 / 8.0.0 / 9.0.0 commands.def.
const REDIS7 = ['eval', 'evalsha', 'fcall', 'pfcount', 'publish', 'spublish']

const EXPECTED: [string, string[]][] = [
  ['redis-6.2', ['eval', 'evalsha', 'pfcount', 'publish', 'script']],
  ['redis-7.0', REDIS7],
  ['redis-7.2', REDIS7],
  ['redis-7.4', REDIS7],
  ['redis-8.0', REDIS7],
  ['valkey-7.2', REDIS7],
  ['valkey-8.0', [...REDIS7, 'cluster|setslot']],
  ['valkey-9.0', [...REDIS7, 'cluster|setslot', 'cluster|syncslots']],
]

function profileOf(name: string) {
  return resolveCompatibilityProfile(
    name === 'valkey-7.2'
      ? { flavor: 'valkey', version: '7.2.5' }
      : (name as CompatibilitySpec),
  )
}

describe('may-replicate commands per profile (#568)', () => {
  for (const [name, expected] of EXPECTED) {
    test(name, () => {
      const profile = profileOf(name)
      assert.deepStrictEqual(
        [...mayReplicateCommands(profile)].sort(),
        [...expected].sort(),
      )
      for (const command of expected) {
        assert.strictEqual(mayReplicate(command, profile), true, command)
      }
      assert.strictEqual(mayReplicate('get', profile), false)
      assert.strictEqual(mayReplicate('set', profile), false)
    })
  }

  test('redis-6.2: the captured table flags exactly these', () => {
    const profile = resolveCompatibilityProfile('redis-6.2')
    const executor = createRedisCommandExecutor({ compatibility: profile })
    const flagged = executor
      .getCommandDefinitions()
      .map(definition => definition.name)
      .filter(command =>
        commandTableEntry(command, profile)?.flags.includes('may_replicate'),
      )
    assert.deepStrictEqual(
      [...new Set(flagged)].sort(),
      [...mayReplicateCommands(profile)].sort(),
    )
  })
})
