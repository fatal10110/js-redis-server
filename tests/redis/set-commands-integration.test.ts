import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { ClusterNetwork } from '../../src/core/cluster/network'
import { Redis, Cluster } from 'ioredis'

describe('Set Commands Integration', () => {
  const redisCluster = new ClusterNetwork(console)
  let redisClient: Cluster | undefined

  before(async () => {
    await redisCluster.init({ masters: 3, slaves: 0 })
    redisClient = new Redis.Cluster(
      [
        {
          host: '127.0.0.1',
          port: Array.from(redisCluster.getAll())[0].port,
        },
      ],
      {
        slotsRefreshTimeout: 10000000,
        lazyConnect: true,
      },
    )
    await redisClient?.connect()
  })

  after(async () => {
    await redisClient?.quit()
    await redisCluster.shutdown()
  })

  test('SADD and SCARD commands', async () => {
    // SADD single member
    const add1 = await redisClient?.sadd('set1', 'member1')
    assert.strictEqual(add1, 1)

    // SADD duplicate member
    const add2 = await redisClient?.sadd('set1', 'member1')
    assert.strictEqual(add2, 0)

    // SADD multiple members
    const add3 = await redisClient?.sadd(
      'set1',
      'member2',
      'member3',
      'member4',
    )
    assert.strictEqual(add3, 3)

    // Check cardinality
    const card = await redisClient?.scard('set1')
    assert.strictEqual(card, 4)
  })

  test('SMEMBERS command', async () => {
    await redisClient?.sadd('set2', 'a', 'b', 'c')

    const members = await redisClient?.smembers('set2')
    assert.strictEqual(members?.length, 3)
    assert.ok(members?.includes('a'))
    assert.ok(members?.includes('b'))
    assert.ok(members?.includes('c'))
  })

  test('SISMEMBER command', async () => {
    await redisClient?.sadd('set3', 'member1', 'member2')

    const is1 = await redisClient?.sismember('set3', 'member1')
    assert.strictEqual(is1, 1)

    const is2 = await redisClient?.sismember('set3', 'nonexistent')
    assert.strictEqual(is2, 0)
  })

  test('SREM command', async () => {
    await redisClient?.sadd('set4', 'a', 'b', 'c', 'd')

    // Remove single member
    const rem1 = await redisClient?.srem('set4', 'a')
    assert.strictEqual(rem1, 1)

    // Remove multiple members
    const rem2 = await redisClient?.srem('set4', 'b', 'c')
    assert.strictEqual(rem2, 2)

    // Remove non-existent member
    const rem3 = await redisClient?.srem('set4', 'nonexistent')
    assert.strictEqual(rem3, 0)

    // Check remaining members
    const remaining = await redisClient?.smembers('set4')
    assert.deepStrictEqual(remaining, ['d'])
  })

  test('SPOP command', async () => {
    await redisClient?.sadd('set5', 'a', 'b', 'c')

    // Pop random member
    const popped = await redisClient?.spop('set5')
    assert.ok(['a', 'b', 'c'].includes(popped!))

    // Check set size decreased
    const card = await redisClient?.scard('set5')
    assert.strictEqual(card, 2)

    // Pop from empty set
    await redisClient?.spop('set5')
    await redisClient?.spop('set5')
    const empty = await redisClient?.spop('set5')
    assert.strictEqual(empty, null)
  })

  test('SRANDMEMBER command', async () => {
    await redisClient?.sadd('set6', 'a', 'b', 'c')

    // Get random member without removing
    const random = await redisClient?.srandmember('set6')
    assert.ok(['a', 'b', 'c'].includes(random!))

    // Check set size unchanged
    const card = await redisClient?.scard('set6')
    assert.strictEqual(card, 3)

    // Get multiple random members
    const randoms = await redisClient?.srandmember('set6', 2)
    assert.strictEqual(randoms?.length, 2)
  })

  test('SDIFF command', async () => {
    await redisClient?.sadd('setA', 'a', 'b', 'c', 'd')
    await redisClient?.sadd('setB', 'b', 'd', 'e')

    const diff = await redisClient?.sdiff('setA', 'setB')
    assert.strictEqual(diff?.length, 2)
    assert.ok(diff?.includes('a'))
    assert.ok(diff?.includes('c'))
  })

  test('SINTER command', async () => {
    await redisClient?.sadd('setX', 'a', 'b', 'c', 'd')
    await redisClient?.sadd('setY', 'b', 'c', 'e', 'f')

    const inter = await redisClient?.sinter('setX', 'setY')
    assert.strictEqual(inter?.length, 2)
    assert.ok(inter?.includes('b'))
    assert.ok(inter?.includes('c'))
  })

  test('SUNION command', async () => {
    await redisClient?.sadd('setP', 'a', 'b')
    await redisClient?.sadd('setQ', 'b', 'c', 'd')

    const union = await redisClient?.sunion('setP', 'setQ')
    assert.strictEqual(union?.length, 4)
    assert.ok(union?.includes('a'))
    assert.ok(union?.includes('b'))
    assert.ok(union?.includes('c'))
    assert.ok(union?.includes('d'))
  })

  test('SMOVE command', async () => {
    await redisClient?.sadd('source', 'a', 'b', 'c')
    await redisClient?.sadd('dest', 'x', 'y')

    // Move existing member
    const move1 = await redisClient?.smove('source', 'dest', 'a')
    assert.strictEqual(move1, 1)

    // Check source doesn't have member
    const sourceHas = await redisClient?.sismember('source', 'a')
    assert.strictEqual(sourceHas, 0)

    // Check dest has member
    const destHas = await redisClient?.sismember('dest', 'a')
    assert.strictEqual(destHas, 1)

    // Move non-existent member
    const move2 = await redisClient?.smove('source', 'dest', 'nonexistent')
    assert.strictEqual(move2, 0)
  })

  test('Set commands workflow - User Tags System', async () => {
    const user1Tags = 'user:1001:tags'
    const user2Tags = 'user:1002:tags'

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
    const onlineUsers = 'online:users'
    const premiumUsers = 'premium:users'

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
    const techArticles = 'category:tech'
    const jsArticles = 'category:javascript'
    const tutorialArticles = 'category:tutorial'

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

    // Get random article from JavaScript category
    const randomJs = await redisClient?.srandmember(jsArticles)
    const jsMembers = await redisClient?.smembers(jsArticles)
    assert.ok(jsMembers?.includes(randomJs!))
  })
})
