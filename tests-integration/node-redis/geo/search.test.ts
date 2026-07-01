import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { GEO_REPLY_WITH, RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, flushNodeRedisCluster, randomKey } from '../../utils'

const testRunner = new TestRunner()

// Same fixture as core.test.ts: Redis' own GEOADD documentation example.
const PALERMO = { lon: 13.361389, lat: 38.115556, member: 'Palermo' }
const CATANIA = { lon: 15.087269, lat: 37.502669, member: 'Catania' }

describe(`Geo Search Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function seedSicily(key: string) {
    await redisClient.geoAdd(key, [
      { longitude: PALERMO.lon, latitude: PALERMO.lat, member: PALERMO.member },
      { longitude: CATANIA.lon, latitude: CATANIA.lat, member: CATANIA.member },
    ])
  }

  test('GEOSEARCH FROMLONLAT BYRADIUS with WITHCOORD/WITHDIST/WITHHASH', async () => {
    const key = `{geosearch:${randomKey()}}`
    try {
      await seedSicily(key)

      const plain = await redisClient.geoSearch(
        key,
        { longitude: 15, latitude: 37 },
        { radius: 200, unit: 'km' },
        { SORT: 'ASC' },
      )
      assert.deepStrictEqual(plain, ['Catania', 'Palermo'])

      const withAll = await redisClient.geoSearchWith(
        key,
        { longitude: 15, latitude: 37 },
        { radius: 200, unit: 'km' },
        [
          GEO_REPLY_WITH.COORDINATES,
          GEO_REPLY_WITH.DISTANCE,
          GEO_REPLY_WITH.HASH,
        ],
        { SORT: 'ASC' },
      )

      assert.strictEqual(withAll[0]!.member, 'Catania')
      assert.strictEqual(Number(withAll[0]!.distance), 56.4413)
      assert.strictEqual(Number(withAll[0]!.hash), 3479447370796909)
      assert.strictEqual(withAll[1]!.member, 'Palermo')
      assert.strictEqual(Number(withAll[1]!.distance), 190.4424)
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEOSEARCH FROMMEMBER BYBOX matches real Redis component-wise box test', async () => {
    const key = `{geosearch-box:${randomKey()}}`
    try {
      await redisClient.geoAdd(key, {
        longitude: 15,
        latitude: 37,
        member: 'center',
      })
      await redisClient.geoAdd(key, {
        longitude: 15,
        latitude: 37.5,
        member: 'north',
      }) // ~55.5km north
      await redisClient.geoAdd(key, {
        longitude: 15.7,
        latitude: 37,
        member: 'east',
      }) // ~62km east at lat 37

      const tall = await redisClient.geoSearch(
        key,
        'center',
        { width: 50, height: 200, unit: 'km' },
        { SORT: 'ASC' },
      )
      assert.deepStrictEqual(tall.sort(), ['center', 'north'].sort())

      const wide = await redisClient.geoSearch(
        key,
        'center',
        { width: 200, height: 50, unit: 'km' },
        { SORT: 'ASC' },
      )
      assert.deepStrictEqual(wide.sort(), ['center', 'east'].sort())
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEOSEARCH COUNT / COUNT ANY semantics', async () => {
    const key = `{geosearch-count:${randomKey()}}`
    try {
      await seedSicily(key)
      await redisClient.geoAdd(key, {
        longitude: 14,
        latitude: 38,
        member: 'Mid1',
      })
      await redisClient.geoAdd(key, {
        longitude: 14.5,
        latitude: 38,
        member: 'Mid2',
      })

      // No ASC/DESC + COUNT defaults to nearest-first.
      const nearest2 = await redisClient.geoSearch(
        key,
        { longitude: 15, latitude: 37 },
        { radius: 500, unit: 'km' },
        { COUNT: 2 },
      )
      assert.deepStrictEqual(nearest2, ['Catania', 'Mid2'])

      // COUNT ANY without ASC/DESC returns the first match in zset score
      // order, not necessarily the closest.
      const any1 = await redisClient.geoSearch(
        key,
        { longitude: 15, latitude: 37 },
        { radius: 500, unit: 'km' },
        { COUNT: { value: 1, ANY: true } },
      )
      assert.deepStrictEqual(any1, ['Palermo'])
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEOSEARCH argument and edge-case errors match Redis', async () => {
    const key = `{geosearch-errors:${randomKey()}}`
    const send = (args: string[]) => redisClient.sendCommand(key, true, args)

    try {
      await seedSicily(key)

      await assert.rejects(
        () =>
          redisClient.geoSearch(key, 'NoSuchMember', {
            radius: 200,
            unit: 'km',
          }),
        errorWithMessage('ERR could not decode requested zset member'),
      )
      await assert.rejects(
        () =>
          send([
            'GEOSEARCH',
            key,
            'FROMMEMBER',
            PALERMO.member,
            'FROMLONLAT',
            '1',
            '1',
            'BYRADIUS',
            '1',
            'km',
          ]),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => send(['GEOSEARCH', key, 'FROMLONLAT', '15', '37']),
        errorWithMessage(
          "ERR wrong number of arguments for 'geosearch' command",
        ),
      )
      await assert.rejects(
        () =>
          redisClient.geoSearch(
            key,
            { longitude: 15, latitude: 37 },
            { radius: 1, unit: 'parsec' as never },
          ),
        errorWithMessage(
          'ERR unsupported unit provided. please use M, KM, FT, MI',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.geoSearch(
            key,
            { longitude: 15, latitude: 37 },
            { radius: -1, unit: 'km' },
          ),
        errorWithMessage('ERR radius cannot be negative'),
      )
      await assert.rejects(
        () =>
          redisClient.geoSearch(
            key,
            { longitude: 15, latitude: 37 },
            { width: -1, height: 5, unit: 'km' },
          ),
        errorWithMessage('ERR height or width cannot be negative'),
      )
      // node-redis' typed COUNT option silently drops `0` (falsy check in
      // the client's own parser), so this needs the raw command form.
      await assert.rejects(
        () =>
          send([
            'GEOSEARCH',
            key,
            'FROMLONLAT',
            '15',
            '37',
            'BYRADIUS',
            '500',
            'km',
            'COUNT',
            '0',
          ]),
        errorWithMessage('ERR COUNT must be > 0'),
      )
      await assert.rejects(
        () =>
          send([
            'GEOSEARCH',
            key,
            'FROMLONLAT',
            '15',
            '37',
            'BYRADIUS',
            '500',
            'km',
            'ANY',
          ]),
        errorWithMessage('ERR the ANY argument requires COUNT argument'),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEOSEARCH on missing key returns empty array', async () => {
    const key = `{geosearch-missing:${randomKey()}}`
    const result = await redisClient.geoSearch(
      key,
      { longitude: 1, latitude: 1 },
      { radius: 1, unit: 'km' },
    )
    assert.deepStrictEqual(result, [])
  })

  test('GEOSEARCH rejects wrong type key', async () => {
    const key = `{geosearch-wrongtype:${randomKey()}}`
    try {
      await redisClient.set(key, 'v')
      await assert.rejects(
        () =>
          redisClient.geoSearch(
            key,
            { longitude: 1, latitude: 1 },
            { radius: 1, unit: 'km' },
          ),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEOSEARCHSTORE stores geohash score, STOREDIST stores distance', async () => {
    const tag = `{geosearchstore:${randomKey()}}`
    const key = `${tag}:src`
    const dest = `${tag}:dst`
    const destDist = `${tag}:dstdist`
    try {
      await seedSicily(key)

      const count = await redisClient.geoSearchStore(
        dest,
        key,
        { longitude: 15, latitude: 37 },
        { radius: 200, unit: 'km' },
      )
      assert.strictEqual(count, 2)
      assert.strictEqual(
        Number(await redisClient.zScore(dest, CATANIA.member)),
        3479447370796909,
      )

      const countDist = await redisClient.geoSearchStore(
        destDist,
        key,
        { longitude: 15, latitude: 37 },
        { radius: 200, unit: 'km' },
        { STOREDIST: true },
      )
      assert.strictEqual(countDist, 2)
      const distScore = Number(
        await redisClient.zScore(destDist, CATANIA.member),
      )
      assert.ok(Math.abs(distScore - 56.44125787015819) < 0.001)

      // Empty result deletes a pre-existing destination key.
      await redisClient.set(`${tag}:empty`, 'pre')
      await redisClient.geoSearchStore(
        `${tag}:empty`,
        key,
        { longitude: 1, latitude: 1 },
        { radius: 1, unit: 'km' },
      )
      assert.strictEqual(await redisClient.exists(`${tag}:empty`), 0)
    } finally {
      await redisClient.del([key, dest, destDist])
    }
  })

  test('GEOSEARCHSTORE rejects WITH* options', async () => {
    const tag = `{geosearchstore-errors:${randomKey()}}`
    const key = `${tag}:src`
    const dest = `${tag}:dst`
    const send = (args: string[]) => redisClient.sendCommand(dest, false, args)
    try {
      await seedSicily(key)
      await assert.rejects(
        () =>
          send([
            'GEOSEARCHSTORE',
            dest,
            key,
            'FROMLONLAT',
            '15',
            '37',
            'BYRADIUS',
            '200',
            'km',
            'WITHCOORD',
          ]),
        errorWithMessage(
          'ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options',
        ),
      )
    } finally {
      await redisClient.del([key, dest])
    }
  })

  test('GEORADIUS (deprecated) matches GEOSEARCH-equivalent behavior', async () => {
    const key = `{georadius:${randomKey()}}`
    try {
      await seedSicily(key)

      const withAll = await redisClient.geoRadiusWith(
        key,
        { longitude: 15, latitude: 37 },
        200,
        'km',
        [
          GEO_REPLY_WITH.COORDINATES,
          GEO_REPLY_WITH.DISTANCE,
          GEO_REPLY_WITH.HASH,
        ],
        { SORT: 'ASC' },
      )
      assert.strictEqual(withAll[0]!.member, 'Catania')
      assert.strictEqual(withAll[1]!.member, 'Palermo')
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEORADIUS STORE / STOREDIST write results, reject combining with WITH*', async () => {
    const tag = `{georadius-store:${randomKey()}}`
    const key = `${tag}:src`
    const dest = `${tag}:dst`
    const send = (args: string[]) => redisClient.sendCommand(key, false, args)
    try {
      await seedSicily(key)

      const count = await redisClient.geoRadiusStore(
        key,
        { longitude: 15, latitude: 37 },
        200,
        'km',
        dest,
      )
      assert.strictEqual(count, 2)
      assert.strictEqual(await redisClient.zCard(dest), 2)

      // node-redis has no typed method that combines STORE with WITH* (the
      // client design forbids constructing the invalid combination), so this
      // is exercised via sendCommand like the other raw-syntax assertions.
      await assert.rejects(
        () =>
          send([
            'GEORADIUS',
            key,
            '15',
            '37',
            '200',
            'km',
            'STORE',
            dest,
            'WITHCOORD',
          ]),
        errorWithMessage(
          'ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options',
        ),
      )
    } finally {
      await redisClient.del([key, dest])
    }
  })

  test('GEORADIUS_RO rejects STORE', async () => {
    const key = `{georadius-ro:${randomKey()}}`
    const send = (args: string[]) => redisClient.sendCommand(key, true, args)
    try {
      await seedSicily(key)
      await assert.rejects(
        () =>
          send(['GEORADIUS_RO', key, '15', '37', '200', 'km', 'STORE', 'x']),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEORADIUSBYMEMBER matches GEOSEARCH FROMMEMBER-equivalent behavior', async () => {
    const key = `{georadiusbymember:${randomKey()}}`
    try {
      await seedSicily(key)

      const result = await redisClient.geoRadiusByMember(
        key,
        PALERMO.member,
        200,
        'km',
      )
      assert.deepStrictEqual(result.sort(), ['Catania', 'Palermo'].sort())

      await assert.rejects(
        () => redisClient.geoRadiusByMember(key, 'NoSuchMember', 200, 'km'),
        errorWithMessage('ERR could not decode requested zset member'),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('GEORADIUSBYMEMBER_RO rejects STORE', async () => {
    const key = `{georadiusbymember-ro:${randomKey()}}`
    const send = (args: string[]) => redisClient.sendCommand(key, true, args)
    try {
      await seedSicily(key)
      await assert.rejects(
        () =>
          send([
            'GEORADIUSBYMEMBER_RO',
            key,
            PALERMO.member,
            '200',
            'km',
            'STORE',
            'x',
          ]),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await redisClient.del(key)
    }
  })
})
