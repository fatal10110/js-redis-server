import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

// Same fixture as core.test.ts: Redis' own GEOADD documentation example.
const PALERMO = { lon: 13.361389, lat: 38.115556, member: 'Palermo' }
const CATANIA = { lon: 15.087269, lat: 37.502669, member: 'Catania' }

describe(`Geo Search Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('geo-search-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function seedSicily(key: string) {
    await redisClient?.geoadd(
      key,
      PALERMO.lon,
      PALERMO.lat,
      PALERMO.member,
      CATANIA.lon,
      CATANIA.lat,
      CATANIA.member,
    )
  }

  test('GEOSEARCH FROMLONLAT BYRADIUS with WITHCOORD/WITHDIST/WITHHASH', async () => {
    const key = `{geosearch:${randomKey()}}`
    try {
      await seedSicily(key)

      const plain = await redisClient?.geosearch(
        key,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '200',
        'km',
        'ASC',
      )
      assert.deepStrictEqual(plain, ['Catania', 'Palermo'])

      const withAll = (await redisClient?.geosearch(
        key,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '200',
        'km',
        'ASC',
        'WITHCOORD',
        'WITHDIST',
        'WITHHASH',
      )) as unknown as [string, string, number, [string, string]][]

      assert.strictEqual(withAll[0]![0], 'Catania')
      assert.strictEqual(withAll[0]![1], '56.4413')
      assert.strictEqual(withAll[0]![2], 3479447370796909)
      assert.strictEqual(withAll[1]![0], 'Palermo')
      assert.strictEqual(withAll[1]![1], '190.4424')
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEOSEARCH FROMMEMBER BYBOX matches real Redis component-wise box test', async () => {
    const key = `{geosearch-box:${randomKey()}}`
    try {
      await redisClient?.geoadd(key, 15, 37, 'center')
      await redisClient?.geoadd(key, 15, 37.5, 'north') // ~55.5km north
      await redisClient?.geoadd(key, 15.7, 37, 'east') // ~62km east at lat 37

      const tall = await redisClient?.geosearch(
        key,
        'FROMMEMBER',
        'center',
        'BYBOX',
        '50',
        '200',
        'km',
        'ASC',
      )
      assert.deepStrictEqual(tall?.sort(), ['center', 'north'].sort())

      const wide = await redisClient?.geosearch(
        key,
        'FROMMEMBER',
        'center',
        'BYBOX',
        '200',
        '50',
        'km',
        'ASC',
      )
      assert.deepStrictEqual(wide?.sort(), ['center', 'east'].sort())
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEOSEARCH COUNT / COUNT ANY semantics', async () => {
    const key = `{geosearch-count:${randomKey()}}`
    try {
      await seedSicily(key)
      await redisClient?.geoadd(key, 14, 38, 'Mid1')
      await redisClient?.geoadd(key, 14.5, 38, 'Mid2')

      // No ASC/DESC + COUNT defaults to nearest-first.
      const nearest2 = await redisClient?.geosearch(
        key,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '500',
        'km',
        'COUNT',
        '2',
      )
      assert.deepStrictEqual(nearest2, ['Catania', 'Mid2'])

      // COUNT ANY without ASC/DESC returns the first match in zset score
      // order, not necessarily the closest.
      const any1 = await redisClient?.geosearch(
        key,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '500',
        'km',
        'COUNT',
        '1',
        'ANY',
      )
      assert.deepStrictEqual(any1, ['Palermo'])
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEOSEARCH argument and edge-case errors match Redis', async () => {
    const key = `{geosearch-errors:${randomKey()}}`
    try {
      await seedSicily(key)

      await assert.rejects(
        () =>
          redisClient?.geosearch(
            key,
            'FROMMEMBER',
            'NoSuchMember',
            'BYRADIUS',
            '200',
            'km',
          ),
        errorWithMessage('ERR could not decode requested zset member'),
      )
      await assert.rejects(
        () =>
          redisClient?.geosearch(
            key,
            'FROMMEMBER',
            PALERMO.member,
            'FROMLONLAT',
            '1',
            '1',
            'BYRADIUS',
            '1',
            'km',
          ),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => redisClient?.geosearch(key, 'FROMLONLAT', '15', '37'),
        errorWithMessage(
          "ERR wrong number of arguments for 'geosearch' command",
        ),
      )
      await assert.rejects(
        () =>
          redisClient?.geosearch(
            key,
            'FROMLONLAT',
            '15',
            '37',
            'BYRADIUS',
            '1',
            'parsec',
          ),
        errorWithMessage(
          'ERR unsupported unit provided. please use M, KM, FT, MI',
        ),
      )
      await assert.rejects(
        () =>
          redisClient?.geosearch(
            key,
            'FROMLONLAT',
            '15',
            '37',
            'BYRADIUS',
            '-1',
            'km',
          ),
        errorWithMessage('ERR radius cannot be negative'),
      )
      await assert.rejects(
        () =>
          redisClient?.geosearch(
            key,
            'FROMLONLAT',
            '15',
            '37',
            'BYBOX',
            '-1',
            '5',
            'km',
          ),
        errorWithMessage('ERR height or width cannot be negative'),
      )
      await assert.rejects(
        () =>
          redisClient?.geosearch(
            key,
            'FROMLONLAT',
            '15',
            '37',
            'BYRADIUS',
            '500',
            'km',
            'COUNT',
            '0',
          ),
        errorWithMessage('ERR COUNT must be > 0'),
      )
      await assert.rejects(
        () =>
          redisClient?.geosearch(
            key,
            'FROMLONLAT',
            '15',
            '37',
            'BYRADIUS',
            '500',
            'km',
            'ANY',
          ),
        errorWithMessage('ERR the ANY argument requires COUNT argument'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEOSEARCH on missing key returns empty array', async () => {
    const key = `{geosearch-missing:${randomKey()}}`
    const result = await redisClient?.geosearch(
      key,
      'FROMLONLAT',
      '1',
      '1',
      'BYRADIUS',
      '1',
      'km',
    )
    assert.deepStrictEqual(result, [])
  })

  test('GEOSEARCH rejects wrong type key', async () => {
    const key = `{geosearch-wrongtype:${randomKey()}}`
    try {
      await redisClient?.set(key, 'v')
      await assert.rejects(
        () =>
          redisClient?.geosearch(
            key,
            'FROMLONLAT',
            '1',
            '1',
            'BYRADIUS',
            '1',
            'km',
          ),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEOSEARCHSTORE stores geohash score, STOREDIST stores distance', async () => {
    const tag = `geosearchstore:${randomKey()}`
    const key = `{${tag}}src`
    const dest = `{${tag}}dst`
    const destDist = `{${tag}}dstdist`
    try {
      await seedSicily(key)

      const count = await redisClient?.geosearchstore(
        dest,
        key,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '200',
        'km',
      )
      assert.strictEqual(count, 2)
      assert.strictEqual(
        await redisClient?.zscore(dest, CATANIA.member),
        '3479447370796909',
      )

      const countDist = await redisClient?.geosearchstore(
        destDist,
        key,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '200',
        'km',
        'STOREDIST',
      )
      assert.strictEqual(countDist, 2)
      const distScore = Number(
        await redisClient?.zscore(destDist, CATANIA.member),
      )
      assert.ok(Math.abs(distScore - 56.44125787015819) < 0.001)

      // Empty result deletes a pre-existing destination key.
      await redisClient?.set(`{${tag}}empty`, 'pre')
      await redisClient?.geosearchstore(
        `{${tag}}empty`,
        key,
        'FROMLONLAT',
        '1',
        '1',
        'BYRADIUS',
        '1',
        'km',
      )
      assert.strictEqual(await redisClient?.exists(`{${tag}}empty`), 0)
    } finally {
      await redisClient?.del(key, dest, destDist)
    }
  })

  test('GEOSEARCHSTORE rejects WITH* options', async () => {
    const tag = `geosearchstore-errors:${randomKey()}`
    const key = `{${tag}}src`
    const dest = `{${tag}}dst`
    try {
      await seedSicily(key)
      await assert.rejects(
        () =>
          redisClient?.geosearchstore(
            dest,
            key,
            'FROMLONLAT',
            '15',
            '37',
            'BYRADIUS',
            '200',
            'km',
            'WITHCOORD',
          ),
        errorWithMessage(
          'ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options',
        ),
      )
    } finally {
      await redisClient?.del(key, dest)
    }
  })

  test('GEORADIUS (deprecated) matches GEOSEARCH-equivalent behavior', async () => {
    const key = `{georadius:${randomKey()}}`
    try {
      await seedSicily(key)

      const withAll = (await redisClient?.georadius(
        key,
        '15',
        '37',
        '200',
        'km',
        'ASC',
        'WITHCOORD',
        'WITHDIST',
        'WITHHASH',
      )) as unknown as [string, string, string, [string, string]][]
      assert.strictEqual(withAll[0]![0], 'Catania')
      assert.strictEqual(withAll[1]![0], 'Palermo')
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEORADIUS STORE / STOREDIST write results, reject combining with WITH*', async () => {
    const tag = `georadius-store:${randomKey()}`
    const key = `{${tag}}src`
    const dest = `{${tag}}dst`
    try {
      await seedSicily(key)

      const count = await redisClient?.georadius(
        key,
        '15',
        '37',
        '200',
        'km',
        'STORE',
        dest,
      )
      assert.strictEqual(count, 2)
      assert.strictEqual(await redisClient?.zcard(dest), 2)

      await assert.rejects(
        () =>
          redisClient?.georadius(
            key,
            '15',
            '37',
            '200',
            'km',
            'STORE',
            dest,
            'WITHCOORD',
          ),
        errorWithMessage(
          'ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options',
        ),
      )
    } finally {
      await redisClient?.del(key, dest)
    }
  })

  test('GEORADIUS_RO rejects STORE', async () => {
    const key = `{georadius-ro:${randomKey()}}`
    try {
      await seedSicily(key)
      await assert.rejects(
        () =>
          (
            redisClient as unknown as {
              georadius_ro: (...a: unknown[]) => Promise<unknown>
            }
          )?.georadius_ro(key, '15', '37', '200', 'km', 'STORE', 'x'),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEORADIUSBYMEMBER matches GEOSEARCH FROMMEMBER-equivalent behavior', async () => {
    const key = `{georadiusbymember:${randomKey()}}`
    try {
      await seedSicily(key)

      const result = await redisClient?.georadiusbymember(
        key,
        PALERMO.member,
        '200',
        'km',
      )
      assert.deepStrictEqual(result?.sort(), ['Catania', 'Palermo'].sort())

      await assert.rejects(
        () => redisClient?.georadiusbymember(key, 'NoSuchMember', '200', 'km'),
        errorWithMessage('ERR could not decode requested zset member'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('GEORADIUSBYMEMBER_RO rejects STORE', async () => {
    const key = `{georadiusbymember-ro:${randomKey()}}`
    try {
      await seedSicily(key)
      await assert.rejects(
        () =>
          (
            redisClient as unknown as {
              georadiusbymember_ro: (...a: unknown[]) => Promise<unknown>
            }
          )?.georadiusbymember_ro(
            key,
            PALERMO.member,
            '200',
            'km',
            'STORE',
            'x',
          ),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })
})
