import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { flushNodeRedisCluster } from '../../utils'

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
