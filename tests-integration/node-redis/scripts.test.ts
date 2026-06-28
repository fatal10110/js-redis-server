import assert from 'node:assert'
import { createHash } from 'node:crypto'
import { after, before, describe, test } from 'node:test'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { errorWithMessage } from '../utils'

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

    test('matches Redis script error format', async () => {
      const script = "return redis.set('x', 1)"
      const sha = createHash('sha1').update(script).digest('hex')

      await assert.rejects(
        () => redisClient?.eval(script, 0),
        errorWithMessage(
          `ERR user_script:1: attempt to call field 'set' (a nil value) script: ${sha}, on @user_script:1.`,
        ),
      )
    })
  })

  describe('Lua value and sandbox compatibility', () => {
    test('non-integer number return is truncated to an integer', async () => {
      assert.strictEqual(await redisClient?.eval('return 3.7', 0), 3)
      assert.strictEqual(await redisClient?.eval('return 3.3', 0), 3)
    })

    test('script with no return value replies with nil', async () => {
      assert.strictEqual(await redisClient?.eval('local a = 1', 0), null)
      assert.strictEqual(await redisClient?.eval('return', 0), null)
    })

    test('boolean true returns 1, false returns nil', async () => {
      assert.strictEqual(await redisClient?.eval('return true', 0), 1)
      assert.strictEqual(await redisClient?.eval('return false', 0), null)
    })

    test('array reply stops at the first nil', async () => {
      assert.deepStrictEqual(
        await redisClient?.eval('return {1,2,nil,4}', 0),
        [1, 2],
      )
    })

    test('a returned table with both ok and err is an error (err wins)', async () => {
      await assert.rejects(
        () => redisClient!.eval("return {ok='STAT', err='ERRR'}", 0),
        errorWithMessage('ERRR'),
      )
    })

    test('redis.error_reply prepends ERR only when there is no code', async () => {
      await assert.rejects(
        () => redisClient!.eval("return redis.error_reply('foo')", 0),
        errorWithMessage('ERR foo'),
      )
      await assert.rejects(
        () => redisClient!.eval("return redis.error_reply('WRONGTYPE x')", 0),
        errorWithMessage('WRONGTYPE x'),
      )
    })

    test('coroutine library is available to scripts', async () => {
      assert.strictEqual(
        await redisClient?.eval('return type(coroutine)', 0),
        'table',
      )
    })

    test('accessing a nonexistent global errors like Redis', async () => {
      const script = "print('a')"
      const sha = createHash('sha1').update(script).digest('hex')
      await assert.rejects(
        () => redisClient!.eval(script, 0),
        errorWithMessage(
          `ERR user_script:1: Script attempted to access nonexistent global variable 'print' script: ${sha}, on @user_script:1.`,
        ),
      )
    })

    test('boolean redis.call argument is rejected like Redis', async () => {
      const script = "return redis.call('set', 'k', true)"
      const sha = createHash('sha1').update(script).digest('hex')
      await assert.rejects(
        () => redisClient!.eval(script, 0),
        errorWithMessage(
          `ERR Lua redis lib command arguments must be strings or integers script: ${sha}, on @user_script:1.`,
        ),
      )
    })
  })
})
