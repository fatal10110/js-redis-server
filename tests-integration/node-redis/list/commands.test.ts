import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

describe(`List Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('LPUSH and RPUSH commands', async () => {
    const lpush1 = await redisClient.lPush('list1', 'item1')
    assert.strictEqual(lpush1, 1)

    const lpush2 = await redisClient.lPush('list1', ['item2', 'item3'])
    assert.strictEqual(lpush2, 3)

    const rpush1 = await redisClient.rPush('list1', ['item4', 'item5'])
    assert.strictEqual(rpush1, 5)
  })

  test('LPOP and RPOP commands', async () => {
    await redisClient.lPush('list2', ['a', 'b', 'c'])

    const lpop = await redisClient.lPop('list2')
    assert.strictEqual(lpop, 'c') // Last pushed is first popped

    const rpop = await redisClient.rPop('list2')
    assert.strictEqual(rpop, 'a') // First pushed is last popped

    const remaining = await redisClient.lPop('list2')
    assert.strictEqual(remaining, 'b')

    const empty = await redisClient.lPop('list2')
    assert.strictEqual(empty, null)
  })

  test('LPOP and RPOP support count argument', async () => {
    const tag = `{list-pop-count:${randomKey()}}`
    const listKey = `${tag}:values`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, listKey)

    try {
      await directClient.del(listKey)
      await directClient.rPush(listKey, ['a', 'b', 'c', 'd'])

      const leftValues = await directClient.lPopCount(listKey, 2)
      assert.deepStrictEqual(leftValues, ['a', 'b'])

      const rightValue = await directClient.rPopCount(listKey, 1)
      assert.deepStrictEqual(rightValue, ['d'])

      const remaining = await directClient.lRange(listKey, 0, -1)
      assert.deepStrictEqual(remaining, ['c'])

      const drained = await directClient.rPopCount(listKey, 5)
      assert.deepStrictEqual(drained, ['c'])
      assert.strictEqual(await directClient.exists(listKey), 0)

      const empty = await directClient.lPopCount(listKey, 1)
      assert.strictEqual(empty, null)

      await directClient.rPush(listKey, ['x', 'y'])
      const zeroCount = await directClient.lPopCount(listKey, 0)
      assert.deepStrictEqual(zeroCount, [])
      assert.deepStrictEqual(await directClient.lRange(listKey, 0, -1), [
        'x',
        'y',
      ])

      await assert.rejects(
        () => directClient.lPopCount(listKey, -1),
        errorWithMessage('ERR value is out of range, must be positive'),
      )
    } finally {
      await directClient.del(listKey)
      directClient.destroy()
    }
  })

  test('LLEN command', async () => {
    const len1 = await redisClient.lLen('emptylist')
    assert.strictEqual(len1, 0)

    await redisClient.lPush('list3', ['a', 'b', 'c'])
    const len2 = await redisClient.lLen('list3')
    assert.strictEqual(len2, 3)
  })

  test('LINDEX command', async () => {
    await redisClient.lPush('list4', ['a', 'b', 'c']) // [c, b, a]

    assert.strictEqual(await redisClient.lIndex('list4', 0), 'c')
    assert.strictEqual(await redisClient.lIndex('list4', 1), 'b')
    assert.strictEqual(await redisClient.lIndex('list4', -1), 'a')
    assert.strictEqual(await redisClient.lIndex('list4', 10), null)
  })

  test('LRANGE command', async () => {
    await redisClient.lPush('list5', ['a', 'b', 'c', 'd', 'e']) // [e, d, c, b, a]

    const all = await redisClient.lRange('list5', 0, -1)
    assert.deepStrictEqual(all, ['e', 'd', 'c', 'b', 'a'])

    const subset = await redisClient.lRange('list5', 1, 3)
    assert.deepStrictEqual(subset, ['d', 'c', 'b'])

    const fromNeg = await redisClient.lRange('list5', -2, -1)
    assert.deepStrictEqual(fromNeg, ['b', 'a'])
  })

  test('LSET command', async () => {
    await redisClient.lPush('list6', ['a', 'b', 'c']) // [c, b, a]

    await redisClient.lSet('list6', 1, 'newb')

    assert.strictEqual(await redisClient.lIndex('list6', 1), 'newb')

    const all = await redisClient.lRange('list6', 0, -1)
    assert.deepStrictEqual(all, ['c', 'newb', 'a'])
  })

  test('List command errors match Redis', async () => {
    const tag = `{list-errors:${randomKey()}}`
    const listKey = `${tag}:list`
    const stringKey = `${tag}:string`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, listKey)

    try {
      await directClient.set(stringKey, 'value')

      await assert.rejects(
        () => directClient.lRange(stringKey, 0, -1),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient.lSet(`${tag}:missing`, 0, 'x'),
        errorWithMessage('ERR no such key'),
      )

      await directClient.rPush(listKey, 'a')
      await assert.rejects(
        () => directClient.lSet(listKey, 2, 'x'),
        errorWithMessage('ERR index out of range'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['LINDEX', listKey, 'abc']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['LRANGE', listKey, 'abc', '1']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    } finally {
      await directClient.del([listKey, stringKey, `${tag}:missing`])
      directClient.destroy()
    }
  })

  test('LREM command', async () => {
    await redisClient.rPush('list7', ['a', 'b', 'a', 'c', 'a']) // [a, b, a, c, a]

    const rem1 = await redisClient.lRem('list7', 2, 'a')
    assert.strictEqual(rem1, 2)

    const after1 = await redisClient.lRange('list7', 0, -1)
    assert.deepStrictEqual(after1, ['b', 'c', 'a'])

    const rem2 = await redisClient.lRem('list7', 0, 'a')
    assert.strictEqual(rem2, 1)

    const after2 = await redisClient.lRange('list7', 0, -1)
    assert.deepStrictEqual(after2, ['b', 'c'])
  })

  test('LTRIM command', async () => {
    await redisClient.rPush('list8', ['a', 'b', 'c', 'd', 'e']) // [a, b, c, d, e]

    await redisClient.lTrim('list8', 1, 3)

    const trimmed = await redisClient.lRange('list8', 0, -1)
    assert.deepStrictEqual(trimmed, ['b', 'c', 'd'])

    const len = await redisClient.lLen('list8')
    assert.strictEqual(len, 3)
  })

  test('List commands workflow - Task Queue', async () => {
    const queueKey = 'tasks:urgent'

    await redisClient.rPush(queueKey, ['task1', 'task2', 'task3'])

    const queueSize = await redisClient.lLen(queueKey)
    assert.strictEqual(queueSize, 3)

    const nextTask = await redisClient.lIndex(queueKey, 0)
    assert.strictEqual(nextTask, 'task1')

    const processed1 = await redisClient.lPop(queueKey)
    assert.strictEqual(processed1, 'task1')

    const processed2 = await redisClient.lPop(queueKey)
    assert.strictEqual(processed2, 'task2')

    await redisClient.lPush(queueKey, 'urgent_task')

    const currentQueue = await redisClient.lRange(queueKey, 0, -1)
    assert.deepStrictEqual(currentQueue, ['urgent_task', 'task3'])

    const urgentProcessed = await redisClient.lPop(queueKey)
    assert.strictEqual(urgentProcessed, 'urgent_task')

    const finalQueue = await redisClient.lRange(queueKey, 0, -1)
    assert.deepStrictEqual(finalQueue, ['task3'])
  })

  test('List commands workflow - Chat Messages', async () => {
    const chatKey = 'chat:room123'

    await redisClient.rPush(chatKey, [
      'Alice: Hello!',
      'Bob: Hi Alice!',
      'Charlie: Hey everyone!',
    ])

    const recentMessages = await redisClient.lRange(chatKey, -10, -1)
    assert.strictEqual(recentMessages.length, 3)
    assert.strictEqual(recentMessages[0], 'Alice: Hello!')

    await redisClient.rPush(chatKey, [
      'Alice: How is everyone?',
      'Bob: Good!',
      'Charlie: Great!',
    ])

    await redisClient.lTrim(chatKey, -5, -1)

    const trimmedMessages = await redisClient.lRange(chatKey, 0, -1)
    assert.strictEqual(trimmedMessages.length, 5)
    assert.strictEqual(trimmedMessages[0], 'Bob: Hi Alice!')

    await redisClient.lSet(chatKey, 0, 'Bob: Hi Alice! (edited)')

    const editedMessage = await redisClient.lIndex(chatKey, 0)
    assert.strictEqual(editedMessage, 'Bob: Hi Alice! (edited)')

    await redisClient.rPush(chatKey, ['Spam: Buy now!', 'Alice: Thanks Bob!'])

    const removed = await redisClient.lRem(chatKey, 0, 'Spam: Buy now!')
    assert.strictEqual(removed, 1)

    const finalChat = await redisClient.lRange(chatKey, 0, -1)
    assert.ok(!finalChat.includes('Spam: Buy now!'))
    assert.ok(finalChat.includes('Alice: Thanks Bob!'))
  })

  test('List commands workflow - Undo Stack', async () => {
    const undoKey = 'user:123:undo'

    await redisClient.lPush(undoKey, 'action:create_file')
    await redisClient.lPush(undoKey, 'action:edit_line_5')
    await redisClient.lPush(undoKey, 'action:delete_line_3')

    const stackSize = await redisClient.lLen(undoKey)
    assert.strictEqual(stackSize, 3)

    const lastAction = await redisClient.lIndex(undoKey, 0)
    assert.strictEqual(lastAction, 'action:delete_line_3')

    const undone1 = await redisClient.lPop(undoKey)
    assert.strictEqual(undone1, 'action:delete_line_3')

    const undone2 = await redisClient.lPop(undoKey)
    assert.strictEqual(undone2, 'action:edit_line_5')

    const remaining = await redisClient.lRange(undoKey, 0, -1)
    assert.deepStrictEqual(remaining, ['action:create_file'])

    await redisClient.lPush(undoKey, 'action:new_edit')

    const newStack = await redisClient.lRange(undoKey, 0, -1)
    assert.deepStrictEqual(newStack, ['action:new_edit', 'action:create_file'])
  })
})
