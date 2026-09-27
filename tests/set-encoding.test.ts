import { afterEach, describe, mock, test } from 'node:test'
import assert from 'node:assert'
import { createInMemoryClient, seedStandalone } from '../src'
import type { InMemoryRedisClient } from '../src'
import { RedisServerState, createSetData } from '../src/internal'
import {
  addSetMember,
  convertToIntsetIfPossible,
  intsetValue,
  prepareSetForAdd,
  type SetEncodingRules,
} from '../src/state/set-encoding'

// Real Redis stores an integer set as an intset, sorted by value, and every
// reader walks a set in storage order (#504). Verified against redis-server
// 6.2.24, 7.0.15, 7.2.16, 7.4, 8.0.6 and Valkey 8.0 / 9.0.

const RULES: SetEncodingRules = { maxIntsetEntries: 512, listpack: true }
const LEGACY_RULES: SetEncodingRules = {
  maxIntsetEntries: 512,
  listpack: false,
}

function members(set: ReturnType<typeof createSetData>): string[] {
  return Array.from(set.members.values(), member => member.toString())
}

function add(
  set: ReturnType<typeof createSetData>,
  values: string[],
  rules: SetEncodingRules = RULES,
): void {
  prepareSetForAdd(set, Buffer.from(values[0]), values.length, rules)
  for (const value of values) addSetMember(set, Buffer.from(value), rules)
}

describe('set encoding helpers', () => {
  test('intsetValue accepts only what string2ll() does', () => {
    assert.strictEqual(intsetValue(Buffer.from('0')), 0n)
    assert.strictEqual(intsetValue(Buffer.from('-12')), -12n)
    assert.strictEqual(
      intsetValue(Buffer.from('-9223372036854775808')),
      -(2n ** 63n),
    )
    assert.strictEqual(
      intsetValue(Buffer.from('9223372036854775807')),
      2n ** 63n - 1n,
    )
    for (const text of [
      '',
      '-0',
      '010',
      '+1',
      ' 1',
      '1.0',
      '9223372036854775808',
      '-9223372036854775809',
      'a',
    ]) {
      assert.strictEqual(intsetValue(Buffer.from(text)), null, text)
    }
  })

  test('an integer set is kept in ascending value order', () => {
    const set = createSetData()
    add(set, ['10', '-5', '2', '7', '-5'])
    assert.strictEqual(set.intset, true)
    assert.deepStrictEqual(members(set), ['-5', '2', '7', '10'])
  })

  test('a non-integer converts the set and keeps the integers sorted ahead of it', () => {
    const set = createSetData()
    add(set, ['3', '1', 'a', '2'])
    assert.strictEqual(set.intset, false)
    assert.deepStrictEqual(members(set), ['1', '3', 'a', '2'])
  })

  test('a set created from a non-integer keeps insertion order', () => {
    const set = createSetData()
    add(set, ['a', '3', '1'])
    assert.strictEqual(set.intset, false)
    set.members.delete(Buffer.from('a').toString('hex'))
    add(set, ['0'])
    assert.deepStrictEqual(members(set), ['3', '1', '0'])
  })

  test('an intset holding more than set-max-intset-entries converts away', () => {
    const rules: SetEncodingRules = { maxIntsetEntries: 2, listpack: true }
    const set = createSetData()
    add(set, ['3', '1'], rules)
    assert.strictEqual(set.intset, true)
    add(set, ['2'], rules)
    assert.strictEqual(set.intset, false)
    add(set, ['0'], rules)
    assert.deepStrictEqual(members(set), ['1', '2', '3', '0'])
  })

  test('from 7.2 the size hint decides whether a new set is an intset', () => {
    const rules: SetEncodingRules = { maxIntsetEntries: 2, listpack: true }
    const set = createSetData()
    add(set, ['3', '1', '3'], rules)
    assert.strictEqual(set.intset, false)
    assert.deepStrictEqual(members(set), ['3', '1'])

    const legacy = createSetData()
    add(legacy, ['3', '1', '3'], { ...rules, listpack: false })
    assert.strictEqual(legacy.intset, true)
    assert.deepStrictEqual(members(legacy), ['1', '3'])
  })

  test('from 7.2 an intset about to take more members than the limit converts first', () => {
    const rules: SetEncodingRules = { maxIntsetEntries: 2, listpack: true }
    const set = createSetData()
    add(set, ['3', '1'], rules)
    add(set, ['1', '1', '1'], rules)
    assert.strictEqual(set.intset, false)

    const legacy = createSetData()
    add(legacy, ['3', '1'], { ...rules, listpack: false })
    add(legacy, ['1', '1', '1'], { ...rules, listpack: false })
    assert.strictEqual(legacy.intset, true)
  })

  test('convertToIntsetIfPossible sorts an all-integer set that fits', () => {
    const set = createSetData()
    add(set, ['x', '5', '3', '1'], LEGACY_RULES)
    set.members.delete(Buffer.from('x').toString('hex'))
    convertToIntsetIfPossible(set, { maxIntsetEntries: 2, listpack: false })
    assert.strictEqual(set.intset, false)
    convertToIntsetIfPossible(set, LEGACY_RULES)
    assert.strictEqual(set.intset, true)
    assert.deepStrictEqual(members(set), ['1', '3', '5'])
  })
})

describe('set storage order through commands', () => {
  let client: InMemoryRedisClient | undefined

  afterEach(() => {
    client?.close()
    client = undefined
    mock.restoreAll()
  })

  test('SMEMBERS, SSCAN and SORT BY nosort walk the storage order', async () => {
    client = await createInMemoryClient()
    await client.command('SADD', 'ints', '3', '1')
    await client.command('SADD', 'mixed', '3', '1', 'a')
    await client.command('SADD', 'lp', 'a', '3', '1')
    await client.command('SREM', 'lp', 'a')

    for (const [key, expected] of [
      ['ints', ['1', '3']],
      ['mixed', ['1', '3', 'a']],
      ['lp', ['3', '1']],
    ] as const) {
      assert.deepStrictEqual(await client.command('SMEMBERS', key), expected)
      assert.deepStrictEqual(await client.command('SSCAN', key, '0'), [
        '0',
        expected,
      ])
      assert.deepStrictEqual(
        await client.command('SORT', key, 'BY', 'nosort'),
        expected,
      )
    }
  })

  test('set-max-intset-entries is read live from CONFIG SET', async () => {
    client = await createInMemoryClient()
    await client.command('CONFIG', 'SET', 'set-max-intset-entries', '2')
    await client.command('SADD', 's', '3', '1', '3')
    assert.deepStrictEqual(await client.command('SMEMBERS', 's'), ['3', '1'])
  })

  test('SADD creates an intset for a hinted size past the limit only before 7.2', async () => {
    for (const [profile, expected] of [
      ['redis-6.2', ['1', '3']],
      ['redis-7.0', ['1', '3']],
      ['redis-7.2', ['3', '1']],
      ['redis-8.0', ['3', '1']],
      ['valkey-8.0', ['3', '1']],
    ] as const) {
      const profileClient = await createInMemoryClient({
        compatibility: profile,
      })
      try {
        await profileClient.command(
          'CONFIG',
          'SET',
          'set-max-intset-entries',
          '2',
        )
        await profileClient.command('SADD', 's', '3', '1', '3')
        assert.deepStrictEqual(
          await profileClient.command('SMEMBERS', 's'),
          expected,
          profile,
        )
      } finally {
        profileClient.close()
      }
    }
  })

  test("SPOP's rebuild makes integer survivors an intset only before 7.2", async () => {
    // With Math.random() at 0 the first `count` members in storage order are
    // popped, so the survivors are the last two: 3 then 1.
    mock.method(Math, 'random', () => 0)
    for (const [profile, expected] of [
      ['redis-6.2', ['1', '2', '3']],
      ['redis-7.0', ['1', '2', '3']],
      ['redis-7.2', ['3', '1', '2']],
      ['redis-8.0', ['3', '1', '2']],
    ] as const) {
      const profileClient = await createInMemoryClient({
        compatibility: profile,
      })
      try {
        const letters = 'abcdefghij'.split('')
        await profileClient.command('SADD', 's', ...letters, '3', '1')
        assert.deepStrictEqual(
          await profileClient.command('SPOP', 's', '10'),
          letters,
        )
        await profileClient.command('SADD', 's', '2')
        assert.deepStrictEqual(
          await profileClient.command('SMEMBERS', 's'),
          expected,
          profile,
        )
      } finally {
        profileClient.close()
      }
    }
  })

  test('SPOP with few survivors leaves a non-intset set alone', async () => {
    // remaining * 5 > count: members are popped one by one, no rebuild.
    mock.method(Math, 'random', () => 0)
    client = await createInMemoryClient({ compatibility: 'redis-6.2' })
    await client.command('SADD', 's', 'a', 'b', '3', '1')
    assert.deepStrictEqual(await client.command('SPOP', 's', '2'), ['a', 'b'])
    await client.command('SADD', 's', '2')
    assert.deepStrictEqual(await client.command('SMEMBERS', 's'), [
      '3',
      '1',
      '2',
    ])
  })

  test('seeding a set of integers builds an intset', async () => {
    const server = new RedisServerState()
    await seedStandalone(server, [
      { key: 's', type: 'set', value: [10, -5, 'x', 2] },
    ])
    const set = server.getDatabase(0).getSet(Buffer.from('s'))
    assert.deepStrictEqual(
      Array.from(set!.members.values(), member => member.toString()),
      ['-5', '10', 'x', '2'],
    )
  })
})
