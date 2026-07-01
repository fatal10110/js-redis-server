import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

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

describe(`Geo Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('geo-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('GEOADD adds members and GEOPOS/GEODIST/GEOHASH read them back', async () => {
    const key = `{geo-core:${randomKey()}}`

    try {
      const added = await redisClient?.geoadd(
        key,
        PALERMO.lon,
        PALERMO.lat,
        PALERMO.member,
        CATANIA.lon,
        CATANIA.lat,
        CATANIA.member,
      )
      assert.strictEqual(added, 2)

      const score = await redisClient?.zscore(key, PALERMO.member)
      assert.strictEqual(score, '3479099956230698')

      const positions = await redisClient?.geopos(
        key,
        PALERMO.member,
        CATANIA.member,
        'NonExisting',
      )
      assert.strictEqual(positions?.length, 3)
      assertCloseTo(Number(positions![0]![0]), PALERMO.lon, 1e-5)
      assertCloseTo(Number(positions![0]![1]), PALERMO.lat, 1e-5)
      assertCloseTo(Number(positions![1]![0]), CATANIA.lon, 1e-5)
      assertCloseTo(Number(positions![1]![1]), CATANIA.lat, 1e-5)
      assert.strictEqual(positions![2], null)

      const distM = await redisClient?.geodist(
        key,
        PALERMO.member,
        CATANIA.member,
      )
      assertCloseTo(Number(distM), 166274.1516, 0.01)

      const distKm = await redisClient?.geodist(
        key,
        PALERMO.member,
        CATANIA.member,
        'km',
      )
      assertCloseTo(Number(distKm), 166.2742, 0.001)

      const distMi = await redisClient?.geodist(
        key,
        PALERMO.member,
        CATANIA.member,
        'mi',
      )
      assertCloseTo(Number(distMi), 103.3182, 0.001)

      const distFt = await redisClient?.geodist(
        key,
        PALERMO.member,
        CATANIA.member,
        'ft',
      )
      assertCloseTo(Number(distFt), 545518.87, 0.5)

      const missingDist = await redisClient?.geodist(
        key,
        PALERMO.member,
        'NonExisting',
      )
      assert.strictEqual(missingDist, null)

      const hashes = await redisClient?.geohash(
        key,
        PALERMO.member,
        CATANIA.member,
        'NonExisting',
      )
      assert.deepStrictEqual(hashes, ['sqc8b49rny0', 'sqdtr74hyu0', null])
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEOADD NX/XX/CH option flags match Redis', async () => {
    const key = `{geo-options:${randomKey()}}`

    try {
      assert.strictEqual(
        await redisClient?.geoadd(key, 'NX', 13.361389, 38.115556, 'one'),
        1,
      )
      assert.strictEqual(await redisClient?.geoadd(key, 'NX', 20, 40, 'one'), 0)
      assertCloseTo(
        (await redisClient?.geopos(key, 'one'))![0]![0] as unknown as number,
        13.361389,
        1e-5,
      )

      assert.strictEqual(await redisClient?.geoadd(key, 'XX', 14, 39, 'one'), 0)
      assertCloseTo(
        Number((await redisClient?.geopos(key, 'one'))![0]![0]),
        14,
        1e-5,
      )
      assert.strictEqual(await redisClient?.geoadd(key, 'XX', 1, 1, 'two'), 0)
      assert.deepStrictEqual(await redisClient?.geopos(key, 'two'), [null])

      assert.strictEqual(
        await redisClient?.geoadd(key, 'CH', 15, 40, 'one', 2, 2, 'two'),
        2,
      )
      assert.strictEqual(await redisClient?.geoadd(key, 'CH', 15, 40, 'one'), 0)
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEOADD option/argument errors match Redis', async () => {
    const key = `{geo-errors:${randomKey()}}`

    try {
      await assert.rejects(
        () => redisClient?.geoadd(key, 'NX', 'XX', 1, 2, 'm'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => redisClient?.geoadd(key, 'GT', 1, 2, 'm'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => redisClient?.geoadd(key, 'abc', 38, 'm1'),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => redisClient?.geoadd(key, 200, 38.115556, 'Invalid'),
        errorWithMessage(
          'ERR invalid longitude,latitude pair 200.000000,38.115556',
        ),
      )
      await assert.rejects(
        () => redisClient?.geoadd(key, 13.361389, -86, 'Invalid'),
        errorWithMessage(
          'ERR invalid longitude,latitude pair 13.361389,-86.000000',
        ),
      )
      await assert.rejects(
        () => redisClient?.geoadd(key),
        errorWithMessage("ERR wrong number of arguments for 'geoadd' command"),
      )
      await assert.rejects(
        () => redisClient?.geoadd(key, 1, 2),
        errorWithMessage("ERR wrong number of arguments for 'geoadd' command"),
      )
      await assert.rejects(
        () => redisClient?.geodist(key, 'a', 'b', 'xyz'),
        errorWithMessage(
          'ERR unsupported unit provided. please use M, KM, FT, MI',
        ),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEO commands on missing key return nil/empty per Redis semantics', async () => {
    const key = `{geo-missing:${randomKey()}}`

    const positions = await redisClient?.geopos(key, 'm1', 'm2')
    assert.deepStrictEqual(positions, [null, null])

    const dist = await redisClient?.geodist(key, 'm1', 'm2')
    assert.strictEqual(dist, null)

    const hashes = await redisClient?.geohash(key, 'm1', 'm2')
    assert.deepStrictEqual(hashes, [null, null])
  })

  test('GEO commands reject wrong type keys', async () => {
    const key = `{geo-wrongtype:${randomKey()}}`

    try {
      await redisClient?.set(key, 'foo')

      await assert.rejects(
        () => redisClient?.geoadd(key, 1, 2, 'm'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient?.geopos(key, 'm'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient?.geodist(key, 'm1', 'm2'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient?.geohash(key, 'm'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(key)
    }
  })
})
