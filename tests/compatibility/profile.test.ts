import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  gateSatisfied,
  parseVersion,
  resolveCompatibilityProfile,
} from '../../src/core/compatibility'

describe('compatibility profiles', () => {
  test('parses semantic versions into comparable numbers', () => {
    assert.strictEqual(parseVersion('6.2'), 60200)
    assert.strictEqual(parseVersion('7.4.4'), 70404)
    assert.strictEqual(parseVersion('10.0.1'), 100001)
  })

  test('resolves the default and named presets', () => {
    const defaultProfile = resolveCompatibilityProfile()
    assert.strictEqual(defaultProfile.flavor, 'redis')
    assert.strictEqual(defaultProfile.version, '8.0.0')
    assert.strictEqual(defaultProfile.versionNum, 80000)

    const redis62 = resolveCompatibilityProfile('redis-6.2')
    assert.strictEqual(redis62.flavor, 'redis')
    assert.strictEqual(redis62.version, '6.2.14')

    const valkey9 = resolveCompatibilityProfile('valkey-9.0')
    assert.strictEqual(valkey9.flavor, 'valkey')
    assert.strictEqual(valkey9.version, '9.0.0')

    const redis80 = resolveCompatibilityProfile('redis-8.0')
    assert.strictEqual(redis80.flavor, 'redis')
    assert.strictEqual(redis80.version, '8.0.0')
  })

  test('rejects unknown preset strings at runtime', () => {
    assert.throws(
      () =>
        resolveCompatibilityProfile(
          'redis-9.9' as Parameters<typeof resolveCompatibilityProfile>[0],
        ),
      /Unknown compatibility profile redis-9\.9/,
    )
  })

  test('evaluates gates per flavor', () => {
    assert.strictEqual(
      gateSatisfied(
        { redis: '7.0.0' },
        resolveCompatibilityProfile('redis-6.2'),
      ),
      false,
    )
    assert.strictEqual(
      gateSatisfied(
        { redis: '7.0.0' },
        resolveCompatibilityProfile('redis-7.0'),
      ),
      true,
    )
    assert.strictEqual(
      gateSatisfied(
        { valkey: '9.0.0' },
        resolveCompatibilityProfile('redis-7.4'),
      ),
      false,
    )
  })

  test('precomputes feature support from the profile', () => {
    const redis62 = resolveCompatibilityProfile('redis-6.2')
    assert.strictEqual(redis62.has('expire.conditions'), false)
    assert.strictEqual(redis62.has('set.get'), true)
    assert.strictEqual(redis62.has('client.setinfo'), false)

    const valkey72 = resolveCompatibilityProfile({
      flavor: 'valkey',
      version: '7.2.4',
    })
    assert.strictEqual(valkey72.has('command.docs'), true)
    assert.strictEqual(valkey72.has('cluster.multi-db'), false)

    const valkey9 = resolveCompatibilityProfile('valkey-9.0')
    assert.strictEqual(valkey9.has('cluster.multi-db'), true)
  })
})
