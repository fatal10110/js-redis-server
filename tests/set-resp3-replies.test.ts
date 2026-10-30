import { afterEach, describe, test } from 'node:test'
import assert from 'node:assert'
import { createInMemoryClient } from '../src'
import type { InMemoryRedisClient } from '../src'

/**
 * SMEMBERS, SINTER, SUNION, SDIFF and SPOP with a count are RESP3 sets (#501);
 * the exact wire bytes per protocol are pinned in
 * tests-integration/raw-tcp/set-resp3-replies.test.ts. Here: the in-memory
 * client still decodes a set to a plain array at either protocol, and a script
 * that called `redis.setresp(3)` sees a `{set=…}` table, as in Redis.
 */
describe('set replies through the in-memory client', () => {
  let client: InMemoryRedisClient

  afterEach(() => {
    client?.close()
  })

  async function seeded(resp: 2 | 3): Promise<InMemoryRedisClient> {
    client = await createInMemoryClient()
    if (resp === 3) await client.command('HELLO', 3)
    await client.command('SADD', 's', 'a')
    await client.command('SADD', 'i', '1', '2')
    await client.command('SADD', 'j', '2', '3')
    return client
  }

  for (const resp of [2, 3] as const) {
    test(`a set decodes to an array on RESP${resp}`, async () => {
      const c = await seeded(resp)
      assert.deepStrictEqual(await c.command('SMEMBERS', 's'), ['a'])
      assert.deepStrictEqual(await c.command('SMEMBERS', 'missing'), [])
      assert.deepStrictEqual(await c.command('SINTER', 'i', 'j'), ['2'])
      assert.deepStrictEqual(await c.command('SUNION', 's', 'missing'), ['a'])
      assert.deepStrictEqual(await c.command('SDIFF', 'i', 'j'), ['1'])
      assert.deepStrictEqual(await c.command('SPOP', 's', 5), ['a'])
      assert.deepStrictEqual(await c.command('SPOP', 's', 0), [])
    })
  }

  test('a script after redis.setresp(3) sees a {set=...} table', async () => {
    const c = await seeded(2)
    assert.strictEqual(
      await c.command(
        'EVAL',
        "redis.setresp(3); local r = redis.call('SMEMBERS', KEYS[1]); return tostring(r.set.a)",
        1,
        's',
      ),
      'true',
    )
    assert.strictEqual(
      await c.command(
        'EVAL',
        "redis.setresp(3); return type(redis.call('SINTER', KEYS[1], KEYS[2]).set)",
        2,
        's',
        'i',
      ),
      'table',
    )
    // Returned as-is to a RESP2 caller, the set is flattened to an array.
    assert.deepStrictEqual(
      await c.command(
        'EVAL',
        "redis.setresp(3); return redis.call('SMEMBERS', KEYS[1])",
        1,
        'i',
      ),
      ['1', '2'],
    )
  })

  test('FUNCTION LIST keeps its RESP2 layout, flags as strings', async () => {
    const c = await seeded(2)
    await c.command(
      'FUNCTION',
      'LOAD',
      "#!lua name=lib\nredis.register_function{function_name='f', callback=function() return 1 end, flags={'no-writes'}}",
    )
    assert.deepStrictEqual(await c.command('FUNCTION', 'LIST'), [
      [
        'library_name',
        'lib',
        'engine',
        'LUA',
        'functions',
        [['name', 'f', 'description', null, 'flags', ['no-writes']]],
      ],
    ])
  })
})
