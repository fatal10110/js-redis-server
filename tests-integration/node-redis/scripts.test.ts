import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { Cluster } from 'ioredis'
import assert from 'node:assert'

const testRunner = new TestRunner()

describe(`Redis commands (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  describe('eval', () => {
    test('passes utf8 args correctly', async () => {
      const script = `
      if ARGV[1] == "фвфв" then
        return 'yes'
      else
        return 'no'
      end
      `

      const res = await redisClient?.eval(script, 0, 'фвфв')

      assert.strictEqual(res, 'yes')
    })

    test('passes utf8 keys correctly', async () => {
      const script = `
      if KEYS[1] == "фвфв" then
        return 'yes'
      else
        return 'no'
      end
      `

      const res = await redisClient?.eval(script, 1, 'фвфв')

      assert.strictEqual(res, 'yes')
    })

    test('gets utf8 value correctly', async () => {
      redisClient?.set('myKey', 'фвфв')
      const script = `
        local val = redis.call("get", KEYS[1])

        if val == "фвфв" then
          return 'yes'
        else
          return 'no'
        end
        `

      const res = await redisClient?.eval(script, 1, 'myKey')

      assert.strictEqual(res, 'yes')
    })

    test('sets utf8 value from args', async () => {
      const script = `
        local val = redis.call("set", KEYS[1], ARGV[1])
        return ''
        `

      await redisClient?.eval(script, 1, 'myKey', 'фвфв')
      const res = await redisClient?.get('myKey')

      assert.strictEqual(res, 'фвфв')
    })

    test('returns binary data without loosing bytes', async () => {
      const dataHex =
        '89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489000000017352474200aece1ce90000000d49444154185763f8bf94e13f0006ef02a42609d4340000000049454e44ae426082'
      const buff = Buffer.from(dataHex, 'hex')

      await redisClient?.set('myKey', buff)

      const script = `
        return redis.call("get", KEYS[1])
        `

      // @ts-expect-error evalBuffer method exists but not in types
      const res = await redisClient?.evalBuffer(script, 1, 'myKey')

      assert.strictEqual(res.toString('hex'), dataHex)
    })
  })
})
