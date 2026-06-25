import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  createRedisCommandExecutor,
  createRedisCommandRegistry,
} from '../../src/commands'
import { defineCommand } from '../../src/core/command-definition'
import { t } from '../../src/core/command-schema'
import { RedisResult } from '../../src/core/redis-result'
import { resolveCompatibilityProfile } from '../../src/core/compatibility'

const futureCommand = defineCommand({
  name: 'future',
  since: { redis: '7.0.0' },
  schema: t.object({}),
  flags: ['readonly'],
  keys: () => [],
  execute: () => RedisResult.ok(),
})

describe('compatibility registry filtering', () => {
  test('filters command definitions whose version gate is not satisfied', () => {
    const redis62 = createRedisCommandRegistry(
      [futureCommand],
      resolveCompatibilityProfile('redis-6.2'),
    )
    assert.strictEqual(redis62.has('future'), false)

    const redis70 = createRedisCommandRegistry(
      [futureCommand],
      resolveCompatibilityProfile('redis-7.0'),
    )
    assert.strictEqual(redis70.has('future'), true)
  })

  test('passes one resolved profile to the registry and executor', () => {
    const executor = createRedisCommandExecutor({
      extraCommands: [futureCommand],
      compatibility: 'redis-6.2',
    })

    assert.strictEqual(executor.profile.version, '6.2.14')
    assert.strictEqual(executor.getCommandDefinition('future'), undefined)
  })

  test('filters implemented root commands by Redis version', () => {
    const redis60 = createRedisCommandRegistry(
      [],
      resolveCompatibilityProfile({ flavor: 'redis', version: '6.0.0' }),
    )
    for (const command of [
      'getex',
      'getdel',
      'copy',
      'hrandfield',
      'lmove',
      'blmove',
      'smismember',
      'xautoclaim',
      'zmscore',
      'reset',
    ]) {
      assert.strictEqual(redis60.has(command), false, command)
    }

    const redis62 = createRedisCommandRegistry(
      [],
      resolveCompatibilityProfile('redis-6.2'),
    )
    for (const command of ['getex', 'copy', 'hrandfield', 'lmove', 'zmscore']) {
      assert.strictEqual(redis62.has(command), true, command)
    }
    for (const command of [
      'expiretime',
      'pexpiretime',
      'lmpop',
      'blmpop',
      'zmpop',
      'bzmpop',
      'sintercard',
      'spublish',
      'ssubscribe',
      'sunsubscribe',
      'zintercard',
      'sort_ro',
    ]) {
      assert.strictEqual(redis62.has(command), false, command)
    }

    const redis70 = createRedisCommandRegistry(
      [],
      resolveCompatibilityProfile('redis-7.0'),
    )
    for (const command of [
      'expiretime',
      'lmpop',
      'zmpop',
      'sintercard',
      'spublish',
      'ssubscribe',
      'sunsubscribe',
      'zintercard',
      'sort_ro',
    ]) {
      assert.strictEqual(redis70.has(command), true, command)
    }

    const redis74 = createRedisCommandRegistry(
      [],
      resolveCompatibilityProfile('redis-7.4'),
    )
    for (const command of [
      'hpersist',
      'hexpire',
      'hpexpire',
      'hexpireat',
      'hpexpireat',
      'httl',
      'hpttl',
    ]) {
      assert.strictEqual(redis74.has(command), true, command)
    }
    for (const command of ['hgetdel', 'hgetex']) {
      assert.strictEqual(redis74.has(command), false, command)
    }

    const redis80 = createRedisCommandRegistry(
      [],
      resolveCompatibilityProfile('redis-8.0'),
    )
    for (const command of ['hgetdel', 'hgetex']) {
      assert.strictEqual(redis80.has(command), true, command)
    }
  })
})
