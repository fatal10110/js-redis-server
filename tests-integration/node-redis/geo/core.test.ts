import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, flushNodeRedisCluster, randomKey } from '../../utils'

const testRunner = new TestRunner()

// Redis' own GEOADD documentation example: Sicily with Palermo and Catania.
const PALERMO = { lon: 13.361389, lat: 38.115556, member: 'Palermo' }
const CATANIA = { lon: 15.087269, lat: 37.502669, member: 'Catania' }

function assertCloseTo(actual: number, expected: number, epsilon = 1e-6) {
  assert.ok(
    Math.abs(actual - expected) < epsilon,
    `expected ${actual} to be close to ${expected}`,
  )
}

describe(`Geo Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('GEOADD adds members and GEOPOS/GEODIST/GEOHASH read them back', async () => {
    const key = `{geo-core:${randomKey()}}`

    try {
      const added = await redisClient.geoAdd(key, [
        {
          longitude: PALERMO.lon,
          latitude: PALERMO.lat,
          member: PALERMO.member,
        },
        {
          longitude: CATANIA.lon,
          latitude: CATANIA.lat,
          member: CATANIA.member,
        },
      ])
      assert.strictEqual(added, 2)

      const score = await redisClient.zScore(key, PALERMO.member)
      assert.strictEqual(Number(score), 3479099956230698)

      const positions = await redisClient.geoPos(key, [
        PALERMO.member,
        CATANIA.member,
        'NonExisting',
      ])
      assert.strictEqual(positions.length, 3)
      assertCloseTo(positions[0]!.longitude, PALERMO.lon, 1e-5)
      assertCloseTo(positions[0]!.latitude, PALERMO.lat, 1e-5)
      assertCloseTo(positions[1]!.longitude, CATANIA.lon, 1e-5)
      assertCloseTo(positions[1]!.latitude, CATANIA.lat, 1e-5)
      assert.strictEqual(positions[2], null)

      const distM = await redisClient.geoDist(
        key,
        PALERMO.member,
        CATANIA.member,
      )
      assertCloseTo(Number(distM), 166274.1516, 0.01)

      const distKm = await redisClient.geoDist(
        key,
        PALERMO.member,
        CATANIA.member,
        'km',
      )
      assertCloseTo(Number(distKm), 166.2742, 0.001)

      const distMi = await redisClient.geoDist(
        key,
        PALERMO.member,
        CATANIA.member,
        'mi',
      )
      assertCloseTo(Number(distMi), 103.3182, 0.001)

      const distFt = await redisClient.geoDist(
        key,
        PALERMO.member,
        CATANIA.member,
        'ft',
      )
      assertCloseTo(Number(distFt), 545518.87, 0.5)

      const missingDist = await redisClient.geoDist(
        key,
        PALERMO.member,
        'NonExisting',
      )
      assert.strictEqual(missingDist, null)

      const hashes = await redisClient.geoHash(key, [
        PALERMO.member,
        CATANIA.member,
        'NonExisting',
      ])
      assert.deepStrictEqual(hashes, ['sqc8b49rny0', 'sqdtr74hyu0', null])
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEOADD NX/XX/CH option flags match Redis', async () => {
    const key = `{geo-options:${randomKey()}}`

    try {
      assert.strictEqual(
        await redisClient.geoAdd(
          key,
          { longitude: 13.361389, latitude: 38.115556, member: 'one' },
          { condition: 'NX' },
        ),
        1,
      )
      assert.strictEqual(
        await redisClient.geoAdd(
          key,
          { longitude: 20, latitude: 40, member: 'one' },
          { condition: 'NX' },
        ),
        0,
      )
      assertCloseTo(
        (await redisClient.geoPos(key, 'one'))[0]!.longitude,
        13.361389,
        1e-5,
      )

      assert.strictEqual(
        await redisClient.geoAdd(
          key,
          { longitude: 14, latitude: 39, member: 'one' },
          { condition: 'XX' },
        ),
        0,
      )
      assertCloseTo(
        (await redisClient.geoPos(key, 'one'))[0]!.longitude,
        14,
        1e-5,
      )
      assert.strictEqual(
        await redisClient.geoAdd(
          key,
          { longitude: 1, latitude: 1, member: 'two' },
          { condition: 'XX' },
        ),
        0,
      )
      assert.deepStrictEqual(await redisClient.geoPos(key, 'two'), [null])

      assert.strictEqual(
        await redisClient.geoAdd(
          key,
          [
            { longitude: 15, latitude: 40, member: 'one' },
            { longitude: 2, latitude: 2, member: 'two' },
          ],
          { CH: true },
        ),
        2,
      )
      assert.strictEqual(
        await redisClient.geoAdd(
          key,
          { longitude: 15, latitude: 40, member: 'one' },
          { CH: true },
        ),
        0,
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEOADD option/argument errors match Redis', async () => {
    const key = `{geo-errors:${randomKey()}}`
    const send = (args: string[]) => redisClient.sendCommand(key, false, args)

    try {
      await assert.rejects(
        () => send(['GEOADD', key, 'NX', 'XX', '1', '2', 'm']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => send(['GEOADD', key, 'GT', '1', '2', 'm']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => send(['GEOADD', key, 'abc', '38', 'm1']),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => send(['GEOADD', key, '200', '38.115556', 'Invalid']),
        errorWithMessage(
          'ERR invalid longitude,latitude pair 200.000000,38.115556',
        ),
      )
      await assert.rejects(
        () => send(['GEOADD', key, '13.361389', '-86', 'Invalid']),
        errorWithMessage(
          'ERR invalid longitude,latitude pair 13.361389,-86.000000',
        ),
      )
      await assert.rejects(
        () => send(['GEOADD', key]),
        errorWithMessage("ERR wrong number of arguments for 'geoadd' command"),
      )
      await assert.rejects(
        () => send(['GEOADD', key, '1', '2']),
        errorWithMessage("ERR wrong number of arguments for 'geoadd' command"),
      )
      await assert.rejects(
        () => send(['GEODIST', key, 'a', 'b', 'xyz']),
        errorWithMessage(
          'ERR unsupported unit provided. please use M, KM, FT, MI',
        ),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEO commands on missing key return nil/empty per Redis semantics', async () => {
    const key = `{geo-missing:${randomKey()}}`

    const positions = await redisClient.geoPos(key, ['m1', 'm2'])
    assert.deepStrictEqual(positions, [null, null])

    const dist = await redisClient.geoDist(key, 'm1', 'm2')
    assert.strictEqual(dist, null)

    const hashes = await redisClient.geoHash(key, ['m1', 'm2'])
    assert.deepStrictEqual(hashes, [null, null])
  })

  test('GEO commands reject wrong type keys', async () => {
    const key = `{geo-wrongtype:${randomKey()}}`

    try {
      await redisClient.set(key, 'foo')

      await assert.rejects(
        () =>
          redisClient.geoAdd(key, { longitude: 1, latitude: 2, member: 'm' }),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient.geoPos(key, 'm'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient.geoDist(key, 'm1', 'm2'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient.geoHash(key, 'm'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient.del(key)
    }
  })
})
