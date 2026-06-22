import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe(`Set Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('SADD and SCARD commands', async () => {
    const add1 = await redisClient.sAdd('set1', 'member1')
    assert.strictEqual(add1, 1)

    const add2 = await redisClient.sAdd('set1', 'member1')
    assert.strictEqual(add2, 0)

    const add3 = await redisClient.sAdd('set1', [
      'member2',
      'member3',
      'member4',
    ])
    assert.strictEqual(add3, 3)

    const card = await redisClient.sCard('set1')
    assert.strictEqual(card, 4)
  })

  test('SMEMBERS command', async () => {
    await redisClient.sAdd('set2', ['a', 'b', 'c'])

    const members = await redisClient.sMembers('set2')
    assert.strictEqual(members.length, 3)
    assert.ok(members.includes('a'))
    assert.ok(members.includes('b'))
    assert.ok(members.includes('c'))
  })

  test('SISMEMBER command', async () => {
    await redisClient.sAdd('set3', ['member1', 'member2'])

    const is1 = await redisClient.sIsMember('set3', 'member1')
    assert.strictEqual(is1, 1)

    const is2 = await redisClient.sIsMember('set3', 'nonexistent')
    assert.strictEqual(is2, 0)
  })

  test('SMISMEMBER command matches Redis', async () => {
    const tag = `{smismember:${randomKey()}}`
    const setKey = `${tag}:set`
    const missingKey = `${tag}:missing`
    const stringKey = `${tag}:string`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, setKey)
      await directClient.sAdd(setKey, ['member1', 'member2'])

      const result = await directClient.sendCommand([
        'SMISMEMBER',
        setKey,
        'member1',
        'missing',
        'member2',
        'member1',
      ])
      assert.deepStrictEqual(result, [1, 0, 1, 1])

      const missing = await directClient.sendCommand([
        'SMISMEMBER',
        missingKey,
        'member1',
        'member2',
      ])
      assert.deepStrictEqual(missing, [0, 0])

      await directClient.set(stringKey, 'value')
      await assert.rejects(
        () => directClient!.sendCommand(['SMISMEMBER', stringKey, 'member1']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () => directClient!.sendCommand(['SMISMEMBER']),
        errorWithMessage(
          "ERR wrong number of arguments for 'smismember' command",
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SMISMEMBER', setKey]),
        errorWithMessage(
          "ERR wrong number of arguments for 'smismember' command",
        ),
      )
    } finally {
      await directClient?.del([setKey, missingKey, stringKey])
      directClient?.destroy()
    }
  })

  test('SREM command', async () => {
    await redisClient.sAdd('set4', ['a', 'b', 'c', 'd'])

    const rem1 = await redisClient.sRem('set4', 'a')
    assert.strictEqual(rem1, 1)

    const rem2 = await redisClient.sRem('set4', ['b', 'c'])
    assert.strictEqual(rem2, 2)

    const rem3 = await redisClient.sRem('set4', 'nonexistent')
    assert.strictEqual(rem3, 0)

    const remaining = await redisClient.sMembers('set4')
    assert.deepStrictEqual(remaining, ['d'])
  })

  test('SPOP command', async () => {
    await redisClient.sAdd('set5', ['a', 'b', 'c'])

    const popped = await redisClient.sPop('set5')
    assert.ok(['a', 'b', 'c'].includes(popped!))

    const card = await redisClient.sCard('set5')
    assert.strictEqual(card, 2)

    await redisClient.sPop('set5')
    await redisClient.sPop('set5')
    const empty = await redisClient.sPop('set5')
    assert.strictEqual(empty, null)
  })

  test('SPOP command with count', async () => {
    const key = `{spop-count:${randomKey()}}:set`
    const missingKey = `${key}:missing`

    try {
      await redisClient.sAdd(key, ['a', 'b', 'c', 'd'])

      const poppedOne = await redisClient.sPopCount(key, 1)
      assert.ok(Array.isArray(poppedOne))
      assert.strictEqual(poppedOne.length, 1)
      assert.ok(['a', 'b', 'c', 'd'].includes(poppedOne[0]))

      const cardAfterOne = await redisClient.sCard(key)
      assert.strictEqual(cardAfterOne, 3)

      const poppedRest = await redisClient.sPopCount(key, 10)
      assert.ok(Array.isArray(poppedRest))
      assert.strictEqual(poppedRest.length, 3)
      assert.deepStrictEqual([...poppedOne, ...poppedRest].sort(), [
        'a',
        'b',
        'c',
        'd',
      ])

      const missing = await redisClient.sPopCount(missingKey, 2)
      assert.deepStrictEqual(missing, [])

      await redisClient.sAdd(key, 'remaining')
      const zero = await redisClient.sPopCount(key, 0)
      assert.deepStrictEqual(zero, [])

      const cardAfterZero = await redisClient.sCard(key)
      assert.strictEqual(cardAfterZero, 1)

      await assert.rejects(
        () => redisClient.sendCommand(key, false, ['SPOP', key, '-1']),
        errorWithMessage('ERR value is out of range, must be positive'),
      )
    } finally {
      await redisClient.del([key, missingKey])
    }
  })

  test('SRANDMEMBER command', async () => {
    await redisClient.sAdd('set6', ['a', 'b', 'c'])

    const random = await redisClient.sRandMember('set6')
    assert.ok(['a', 'b', 'c'].includes(random!))

    const card = await redisClient.sCard('set6')
    assert.strictEqual(card, 3)

    const randoms = await redisClient.sRandMemberCount('set6', 2)
    assert.strictEqual(randoms.length, 2)
  })

  test('Set command errors match Redis', async () => {
    const tag = `{set-errors:${randomKey()}}`
    const setKey = `${tag}:set`
    const stringKey = `${tag}:string`

    try {
      await redisClient.sAdd(setKey, 'a')
      await redisClient.set(stringKey, 'value')

      await assert.rejects(
        () => redisClient.sAdd(stringKey, 'a'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(setKey, true, ['SRANDMEMBER', setKey, 'abc']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient.sMove(setKey, stringKey, 'a'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient.del([setKey, stringKey])
    }
  })

  test('SDIFF command', async () => {
    await redisClient.sAdd('{test}setA', ['a', 'b', 'c', 'd'])
    await redisClient.sAdd('{test}setB', ['b', 'd', 'e'])

    const diff = await redisClient.sDiff(['{test}setA', '{test}setB'])
    assert.strictEqual(diff.length, 2)
    assert.ok(diff.includes('a'))
    assert.ok(diff.includes('c'))
  })

  test('SINTER command', async () => {
    await redisClient.sAdd('{test}setX', ['a', 'b', 'c', 'd'])
    await redisClient.sAdd('{test}setY', ['b', 'c', 'e', 'f'])

    const inter = await redisClient.sInter(['{test}setX', '{test}setY'])
    assert.strictEqual(inter.length, 2)
    assert.ok(inter.includes('b'))
    assert.ok(inter.includes('c'))
  })

  test('SINTERCARD command matches Redis', async () => {
    const tag = `{sintercard:${randomKey()}}`
    const setA = `${tag}:a`
    const setB = `${tag}:b`
    const setC = `${tag}:c`
    const missing = `${tag}:missing`
    const stringKey = `${tag}:string`
    const crossSlotKey = `sintercard-cross:${randomKey()}`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, setA)

      await directClient.sAdd(setA, ['a', 'b', 'c', 'd'])
      await directClient.sAdd(setB, ['b', 'c', 'd', 'e'])
      await directClient.sAdd(setC, ['c', 'd', 'f'])

      const count = await directClient.sendCommand([
        'SINTERCARD',
        '3',
        setA,
        setB,
        setC,
      ])
      assert.strictEqual(count, 2)

      const limited = await directClient.sendCommand([
        'SINTERCARD',
        '3',
        setA,
        setB,
        setC,
        'LIMIT',
        '1',
      ])
      assert.strictEqual(limited, 1)

      const unlimited = await directClient.sendCommand([
        'SINTERCARD',
        '2',
        setA,
        setB,
        'LIMIT',
        '0',
      ])
      assert.strictEqual(unlimited, 3)

      const withMissing = await directClient.sendCommand([
        'SINTERCARD',
        '2',
        setA,
        missing,
      ])
      assert.strictEqual(withMissing, 0)

      await directClient.set(stringKey, 'value')
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', '2', setA, stringKey]),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () =>
          directClient!.sendCommand(['SINTERCARD', '2', setA, crossSlotKey]),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )

      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD']),
        errorWithMessage(
          "ERR wrong number of arguments for 'sintercard' command",
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', 'two', setA]),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', '0', setA]),
        errorWithMessage('ERR numkeys should be greater than 0'),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', '2', setA]),
        errorWithMessage(
          "ERR Number of keys can't be greater than number of args",
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', '1', setA, setB]),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['SINTERCARD', '1', setA, 'LIMIT']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand(['SINTERCARD', '1', setA, 'LIMIT', 'abc']),
        errorWithMessage("ERR LIMIT can't be negative"),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand(['SINTERCARD', '1', setA, 'LIMIT', '-1']),
        errorWithMessage("ERR LIMIT can't be negative"),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'SINTERCARD',
            '1',
            setA,
            'LIMIT',
            '1',
            'LIMIT',
          ]),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await directClient?.del([setA, setB, setC, missing, stringKey])
      directClient?.destroy()
    }
  })

  test('SUNION command', async () => {
    await redisClient.sAdd('{test}setP', ['a', 'b'])
    await redisClient.sAdd('{test}setQ', ['b', 'c', 'd'])

    const union = await redisClient.sUnion(['{test}setP', '{test}setQ'])
    assert.strictEqual(union.length, 4)
    assert.ok(union.includes('a'))
    assert.ok(union.includes('b'))
    assert.ok(union.includes('c'))
    assert.ok(union.includes('d'))
  })

  test('SMOVE command', async () => {
    await redisClient.sAdd('{test}source', ['a', 'b', 'c'])
    await redisClient.sAdd('{test}dest', ['x', 'y'])

    const move1 = await redisClient.sMove('{test}source', '{test}dest', 'a')
    assert.strictEqual(move1, 1)

    const sourceHas = await redisClient.sIsMember('{test}source', 'a')
    assert.strictEqual(sourceHas, 0)

    const destHas = await redisClient.sIsMember('{test}dest', 'a')
    assert.strictEqual(destHas, 1)

    const move2 = await redisClient.sMove(
      '{test}source',
      '{test}dest',
      'nonexistent',
    )
    assert.strictEqual(move2, 0)
  })

  test('Set commands workflow - User Tags System', async () => {
    const user1Tags = '{user}user:1001:tags'
    const user2Tags = '{user}user:1002:tags'

    await redisClient.sAdd(user1Tags, [
      'developer',
      'javascript',
      'nodejs',
      'redis',
    ])
    await redisClient.sAdd(user2Tags, [
      'developer',
      'python',
      'redis',
      'docker',
    ])

    const isJsDev = await redisClient.sIsMember(user1Tags, 'javascript')
    assert.strictEqual(isJsDev, 1)

    const user1AllTags = await redisClient.sMembers(user1Tags)
    assert.ok(user1AllTags.includes('developer'))
    assert.ok(user1AllTags.includes('javascript'))

    const commonTags = await redisClient.sInter([user1Tags, user2Tags])
    assert.ok(commonTags.includes('developer'))
    assert.ok(commonTags.includes('redis'))
    assert.strictEqual(commonTags.length, 2)

    const uniqueSkills = await redisClient.sDiff([user1Tags, user2Tags])
    assert.ok(uniqueSkills.includes('javascript'))
    assert.ok(uniqueSkills.includes('nodejs'))

    const allSkills = await redisClient.sUnion([user1Tags, user2Tags])
    assert.ok(allSkills.includes('javascript'))
    assert.ok(allSkills.includes('python'))
    assert.ok(allSkills.includes('docker'))

    await redisClient.sAdd(user1Tags, 'typescript')
    await redisClient.sRem(user1Tags, 'nodejs')

    const updatedTags = await redisClient.sMembers(user1Tags)
    assert.ok(updatedTags.includes('typescript'))
    assert.ok(!updatedTags.includes('nodejs'))
  })

  test('Set commands workflow - Online Users', async () => {
    const onlineUsers = '{users}online:users'
    const premiumUsers = '{users}premium:users'

    await redisClient.sAdd(onlineUsers, ['user1', 'user2', 'user3', 'user4'])
    await redisClient.sAdd(premiumUsers, ['user2', 'user4', 'user5'])

    const onlineCount = await redisClient.sCard(onlineUsers)
    assert.strictEqual(onlineCount, 4)

    const isUser1Online = await redisClient.sIsMember(onlineUsers, 'user1')
    assert.strictEqual(isUser1Online, 1)

    const onlinePremium = await redisClient.sInter([onlineUsers, premiumUsers])
    assert.deepStrictEqual(onlinePremium.sort(), ['user2', 'user4'])

    const randomUser = await redisClient.sRandMember(onlineUsers)
    assert.ok(['user1', 'user2', 'user3', 'user4'].includes(randomUser!))

    await redisClient.sRem(onlineUsers, 'user1')
    await redisClient.sAdd(onlineUsers, 'user6')

    const currentOnline = await redisClient.sMembers(onlineUsers)
    assert.ok(!currentOnline.includes('user1'))
    assert.ok(currentOnline.includes('user6'))

    const disconnectUser = await redisClient.sPop(onlineUsers)
    assert.ok(['user2', 'user3', 'user4', 'user6'].includes(disconnectUser!))

    const finalCount = await redisClient.sCard(onlineUsers)
    assert.strictEqual(finalCount, 3)
  })

  test('Set commands workflow - Content Categories', async () => {
    const techArticles = '{category}category:tech'
    const jsArticles = '{category}category:javascript'
    const tutorialArticles = '{category}category:tutorial'

    await redisClient.sAdd(techArticles, [
      'article1',
      'article2',
      'article3',
      'article4',
    ])
    await redisClient.sAdd(jsArticles, ['article2', 'article3', 'article5'])
    await redisClient.sAdd(tutorialArticles, [
      'article3',
      'article4',
      'article6',
    ])

    const techTutorials = await redisClient.sInter([
      techArticles,
      tutorialArticles,
    ])
    assert.deepStrictEqual(techTutorials.sort(), ['article3', 'article4'])

    const jsOrTutorial = await redisClient.sUnion([
      jsArticles,
      tutorialArticles,
    ])
    assert.ok(jsOrTutorial.includes('article2'))
    assert.ok(jsOrTutorial.includes('article5'))
    assert.ok(jsOrTutorial.includes('article6'))

    const nonJsTech = await redisClient.sDiff([techArticles, jsArticles])
    assert.deepStrictEqual(nonJsTech.sort(), ['article1', 'article4'])

    const moved = await redisClient.sMove(techArticles, jsArticles, 'article1')
    assert.strictEqual(moved, 1)

    const isTech = await redisClient.sIsMember(techArticles, 'article1')
    const isJs = await redisClient.sIsMember(jsArticles, 'article1')
    assert.strictEqual(isTech, 0)
    assert.strictEqual(isJs, 1)

    const techCount = await redisClient.sCard(techArticles)
    const jsCount = await redisClient.sCard(jsArticles)
    assert.strictEqual(techCount, 3) // Lost 1 article
    assert.strictEqual(jsCount, 4) // Gained 1 article

    const randomTechArticle = await redisClient.sRandMember(techArticles)
    assert.ok(['article2', 'article3', 'article4'].includes(randomTechArticle!))
  })
})
