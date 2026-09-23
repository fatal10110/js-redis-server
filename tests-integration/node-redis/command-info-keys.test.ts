import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType } from 'redis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

// The mock-vs-real sweep over every command and the per-profile EXPIRE arity
// check live in the ioredis twin: they compare servers, not client behavior.
const testRunner = new TestRunner()

// `[arity, first key, last key, key step]` as real Redis 8.0 reports them.
const EXPECTED: Record<string, [number, number, number, number]> = {
  lpush: [-3, 1, 1, 1],
  zadd: [-4, 1, 1, 1],
  hset: [-4, 1, 1, 1],
  get: [2, 1, 1, 1],
  set: [-3, 1, 1, 1],
  mget: [-2, 1, -1, 1],
  del: [-2, 1, -1, 1],
  eval: [-3, 0, 0, 0],
  xadd: [-5, 1, 1, 1],
  mset: [-3, 1, -1, 2],
  rename: [3, 1, 2, 1],
  blpop: [-3, 1, -2, 1],
  bitop: [-4, 2, -1, 1],
  zunionstore: [-4, 1, 1, 1],
  xread: [-4, 0, 0, 0],
  ping: [-1, 0, 0, 0],
}

describe(`COMMAND INFO arity and key positions (node-redis, ${testRunner.getBackendName()})`, () => {
  let client: RedisClientType

  before(async () => {
    client = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('reports arity and first/last/step keys like Redis', async () => {
    const names = Object.keys(EXPECTED)
    const infos = await client.commandInfo(names)

    assert.strictEqual(infos.length, names.length)
    for (const [i, name] of names.entries()) {
      const info = infos[i]
      assert.ok(info, `${name} missing`)
      assert.strictEqual(info.name, name)
      assert.deepStrictEqual(
        [info.arity, info.firstKeyIndex, info.lastKeyIndex, info.step],
        EXPECTED[name],
        name,
      )
    }
  })

  test('GEOPOS and GEOHASH accept a key alone (arity -2)', async () => {
    const key = `geo:${randomKey()}`
    assert.deepStrictEqual(await client.geoPos(key, []), [])
    assert.deepStrictEqual(await client.geoHash(key, []), [])

    await client.geoAdd(key, {
      longitude: 13.361389,
      latitude: 38.115556,
      member: 'Palermo',
    })
    assert.deepStrictEqual(await client.geoPos(key, []), [])
    assert.deepStrictEqual(await client.geoHash(key, []), [])
    assert.deepStrictEqual(await client.geoHash(key, 'Palermo'), [
      'sqc8b49rny0',
    ])
  })
})
