import { describe, test } from 'node:test'
import assert from 'node:assert'

import { defineCommand } from '../../src/core/command-definition'
import { CommandExecutor } from '../../src/core/command-executor'
import { CommandRegistry } from '../../src/core/command-registry'
import { t } from '../../src/core/command-schema'
import { RedisResult } from '../../src/core/redis-result'
import { RedisServerState } from '../../src/state'

describe('compatibility profile plumbing', () => {
  test('RedisServerState stores a resolved compatibility profile', () => {
    const server = new RedisServerState({ compatibility: 'redis-6.2' })
    assert.strictEqual(server.profile.flavor, 'redis')
    assert.strictEqual(server.profile.version, '6.2.14')
  })

  test('CommandExecutor passes its profile into argument parsing', () => {
    const registry = new CommandRegistry()
    registry.register(
      defineCommand({
        name: 'probe',
        schema: t.custom((_input, index, ctx) => ({
          value: ctx.profile.has('expire.conditions'),
          nextIndex: index,
        })),
        flags: ['readonly'],
        keys: () => [],
        execute: () => RedisResult.ok(),
      }),
    )

    const executor = new CommandExecutor({
      registry,
      profile: new RedisServerState({ compatibility: 'redis-6.2' }).profile,
    })

    assert.deepStrictEqual(executor.plan('probe', []).args, false)
  })
})
