import { after, before, describe, test } from 'node:test'
import { Cluster } from 'ioredis'
import assert from 'node:assert'
import { createHash } from 'node:crypto'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`Redis scripts (ioredis) ${testRunner.getBackendName()}`, () => {
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

    test('redis.call and redis.pcall error replies match Redis', async () => {
      const tag = `{script-reply-errors:${randomKey()}}`
      const listKey = `${tag}:list`
      const directClient = await connectToSlotOwner(redisClient!, listKey)
      const unknownCallScript = "return redis.call('nosuchcommand')"
      const unknownCallSha = createHash('sha1')
        .update(unknownCallScript)
        .digest('hex')
      const wrongTypeCallScript = "return redis.call('get', KEYS[1])"
      const wrongTypeCallSha = createHash('sha1')
        .update(wrongTypeCallScript)
        .digest('hex')
      const noArgsCallScript = 'return redis.call()'
      const noArgsCallSha = createHash('sha1')
        .update(noArgsCallScript)
        .digest('hex')

      try {
        await directClient.lpush(listKey, 'value')

        await assert.rejects(
          () => directClient.eval(unknownCallScript, 0),
          errorWithMessage(
            `ERR Unknown Redis command called from script script: ${unknownCallSha}, on @user_script:1.`,
          ),
        )
        await assert.rejects(
          () => directClient.eval("return redis.pcall('nosuchcommand')", 0),
          errorWithMessage('ERR Unknown Redis command called from script'),
        )
        await assert.rejects(
          () => directClient.eval(wrongTypeCallScript, 1, listKey),
          errorWithMessage(
            `WRONGTYPE Operation against a key holding the wrong kind of value script: ${wrongTypeCallSha}, on @user_script:1.`,
          ),
        )
        await assert.rejects(
          () =>
            directClient.eval("return redis.pcall('get', KEYS[1])", 1, listKey),
          errorWithMessage(
            'WRONGTYPE Operation against a key holding the wrong kind of value',
          ),
        )
        await assert.rejects(
          () => directClient.eval(noArgsCallScript, 0),
          errorWithMessage(
            `ERR Please specify at least one argument for this redis lib call script: ${noArgsCallSha}, on @user_script:1.`,
          ),
        )
        await assert.rejects(
          () => directClient.eval('return redis.pcall()', 0),
          errorWithMessage(
            'ERR Please specify at least one argument for this redis lib call',
          ),
        )
      } finally {
        await directClient.del(listKey)
        directClient.disconnect()
      }
    })
  })

  test('SCRIPT LOAD, EXISTS, EVALSHA, and FLUSH use the node script cache', async () => {
    const directClient = await connectToSlotOwner(
      redisClient!,
      `{script:${randomKey()}}:probe`,
    )
    const script = 'return ARGV[1]'
    const sha = createHash('sha1').update(script).digest('hex')
    const missingSha = createHash('sha1')
      .update('return "missing"')
      .digest('hex')

    try {
      assert.strictEqual(await directClient.call('SCRIPT', 'LOAD', script), sha)
      assert.deepStrictEqual(
        await directClient.call('SCRIPT', 'EXISTS', sha, missingSha),
        [1, 0],
      )
      assert.strictEqual(await directClient.evalsha(sha, 0, 'cached'), 'cached')

      assert.strictEqual(
        await directClient.call('SCRIPT', 'FLUSH', 'SYNC'),
        'OK',
      )
      assert.deepStrictEqual(await directClient.call('SCRIPT', 'EXISTS', sha), [
        0,
      ])
      await assert.rejects(
        () => directClient.evalsha(sha, 0, 'cached'),
        errorWithMessage('NOSCRIPT No matching script. Please use EVAL.'),
      )
    } finally {
      directClient.disconnect()
    }
  })

  test('SCRIPT HELP, DEBUG, KILL, and syntax errors match Redis', async () => {
    const directClient = await connectToSlotOwner(
      redisClient!,
      `{script-errors:${randomKey()}}:probe`,
    )

    try {
      const help = (await directClient.call('SCRIPT', 'HELP')) as string[]
      assert.ok(help.some(line => line.includes('SCRIPT <subcommand>')))

      assert.strictEqual(
        await directClient.call('SCRIPT', 'DEBUG', 'SYNC'),
        'OK',
      )
      await assert.rejects(
        () => directClient.call('SCRIPT', 'KILL'),
        errorWithMessage('NOTBUSY No scripts in execution right now.'),
      )
      await assert.rejects(
        () => directClient.call('SCRIPT'),
        errorWithMessage("ERR wrong number of arguments for 'script' command"),
      )
      await assert.rejects(
        () => directClient.call('SCRIPT', 'missing'),
        errorWithMessage("ERR unknown subcommand 'missing'. Try SCRIPT HELP."),
      )
      await assert.rejects(
        () => directClient.call('SCRIPT', 'LOAD'),
        errorWithMessage(
          "ERR wrong number of arguments for 'script|load' command",
        ),
      )
      await assert.rejects(
        () => directClient.call('SCRIPT', 'DEBUG', 'invalid'),
        errorWithMessage('ERR Use SCRIPT DEBUG YES/SYNC/NO'),
      )
      await assert.rejects(
        () => directClient.call('SCRIPT', 'FLUSH', 'invalid'),
        errorWithMessage('ERR SCRIPT FLUSH only support SYNC|ASYNC option'),
      )
    } finally {
      directClient.disconnect()
    }
  })

  test('EVAL and EVALSHA arity errors match Redis', async () => {
    const directClient = await connectToSlotOwner(
      redisClient!,
      `{eval-errors:${randomKey()}}:probe`,
    )

    try {
      await assert.rejects(
        () => directClient.eval('return 1', 2, 'only-one-key'),
        errorWithMessage(
          "ERR Number of keys can't be greater than number of args",
        ),
      )
      await assert.rejects(
        () => directClient.evalsha('missing', 0),
        errorWithMessage('NOSCRIPT No matching script. Please use EVAL.'),
      )
    } finally {
      directClient.disconnect()
    }
  })
})
