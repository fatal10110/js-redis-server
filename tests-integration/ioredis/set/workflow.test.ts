import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'

const testRunner = new TestRunner()

describe(`Set Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster(
      'set-commands-integration',
    )
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('Set commands workflow - User Tags System', async () => {
    const user1Tags = '{user}user:1001:tags'
    const user2Tags = '{user}user:1002:tags'

    // Add tags for users
    await redisClient?.sadd(
      user1Tags,
      'developer',
      'javascript',
      'nodejs',
      'redis',
    )
    await redisClient?.sadd(user2Tags, 'developer', 'python', 'redis', 'docker')

    // Check if user has specific tag
    const isJsDev = await redisClient?.sismember(user1Tags, 'javascript')
    assert.strictEqual(isJsDev, 1)

    // Get all tags for user
    const user1AllTags = await redisClient?.smembers(user1Tags)
    assert.ok(user1AllTags?.includes('developer'))
    assert.ok(user1AllTags?.includes('javascript'))

    // Find common interests
    const commonTags = await redisClient?.sinter(user1Tags, user2Tags)
    assert.ok(commonTags?.includes('developer'))
    assert.ok(commonTags?.includes('redis'))
    assert.strictEqual(commonTags?.length, 2)

    // Find unique skills of user1
    const uniqueSkills = await redisClient?.sdiff(user1Tags, user2Tags)
    assert.ok(uniqueSkills?.includes('javascript'))
    assert.ok(uniqueSkills?.includes('nodejs'))

    // Get all skills from both users
    const allSkills = await redisClient?.sunion(user1Tags, user2Tags)
    assert.ok(allSkills?.includes('javascript'))
    assert.ok(allSkills?.includes('python'))
    assert.ok(allSkills?.includes('docker'))

    // User1 learns a new skill
    await redisClient?.sadd(user1Tags, 'typescript')

    // User1 stops using a technology
    await redisClient?.srem(user1Tags, 'nodejs')

    const updatedTags = await redisClient?.smembers(user1Tags)
    assert.ok(updatedTags?.includes('typescript'))
    assert.ok(!updatedTags?.includes('nodejs'))
  })

  test('Set commands workflow - Online Users', async () => {
    const onlineUsers = '{users}online:users'
    const premiumUsers = '{users}premium:users'

    // Users come online
    await redisClient?.sadd(onlineUsers, 'user1', 'user2', 'user3', 'user4')
    await redisClient?.sadd(premiumUsers, 'user2', 'user4', 'user5')

    // Check online count
    const onlineCount = await redisClient?.scard(onlineUsers)
    assert.strictEqual(onlineCount, 4)

    // Check if specific user is online
    const isUser1Online = await redisClient?.sismember(onlineUsers, 'user1')
    assert.strictEqual(isUser1Online, 1)

    // Find premium users who are online
    const onlinePremium = await redisClient?.sinter(onlineUsers, premiumUsers)
    assert.deepStrictEqual(onlinePremium?.sort(), ['user2', 'user4'])

    // Get random online user for feature testing
    const randomUser = await redisClient?.srandmember(onlineUsers)
    assert.ok(['user1', 'user2', 'user3', 'user4'].includes(randomUser!))

    // User goes offline
    await redisClient?.srem(onlineUsers, 'user1')

    // New user comes online
    await redisClient?.sadd(onlineUsers, 'user6')

    // Check updated online users
    const currentOnline = await redisClient?.smembers(onlineUsers)
    assert.ok(!currentOnline?.includes('user1'))
    assert.ok(currentOnline?.includes('user6'))

    // Pick random user to disconnect (maintenance)
    const disconnectUser = await redisClient?.spop(onlineUsers)
    assert.ok(['user2', 'user3', 'user4', 'user6'].includes(disconnectUser!))

    // Check final online count
    const finalCount = await redisClient?.scard(onlineUsers)
    assert.strictEqual(finalCount, 3)
  })

  test('Set commands workflow - Content Categories', async () => {
    const techArticles = '{category}category:tech'
    const jsArticles = '{category}category:javascript'
    const tutorialArticles = '{category}category:tutorial'

    // Categorize articles
    await redisClient?.sadd(
      techArticles,
      'article1',
      'article2',
      'article3',
      'article4',
    )
    await redisClient?.sadd(jsArticles, 'article2', 'article3', 'article5')
    await redisClient?.sadd(
      tutorialArticles,
      'article3',
      'article4',
      'article6',
    )

    // Find tech articles that are also tutorials
    const techTutorials = await redisClient?.sinter(
      techArticles,
      tutorialArticles,
    )
    assert.deepStrictEqual(techTutorials?.sort(), ['article3', 'article4'])

    // Find all JavaScript or tutorial articles
    const jsOrTutorial = await redisClient?.sunion(jsArticles, tutorialArticles)
    assert.ok(jsOrTutorial?.includes('article2'))
    assert.ok(jsOrTutorial?.includes('article5'))
    assert.ok(jsOrTutorial?.includes('article6'))

    // Find tech articles that are not JavaScript
    const nonJsTech = await redisClient?.sdiff(techArticles, jsArticles)
    assert.deepStrictEqual(nonJsTech?.sort(), ['article1', 'article4'])

    // Move article from one category to another
    const moved = await redisClient?.smove(techArticles, jsArticles, 'article1')
    assert.strictEqual(moved, 1)

    // Verify the move
    const isTech = await redisClient?.sismember(techArticles, 'article1')
    const isJs = await redisClient?.sismember(jsArticles, 'article1')
    assert.strictEqual(isTech, 0)
    assert.strictEqual(isJs, 1)

    // Check final counts
    const techCount = await redisClient?.scard(techArticles)
    const jsCount = await redisClient?.scard(jsArticles)
    assert.strictEqual(techCount, 3) // Lost 1 article
    assert.strictEqual(jsCount, 4) // Gained 1 article

    // Random article selection for feature highlighting
    const randomTechArticle = await redisClient?.srandmember(techArticles)
    assert.ok(['article2', 'article3', 'article4'].includes(randomTechArticle!))
  })
})
