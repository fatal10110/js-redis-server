import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'

const testRunner = new TestRunner()

describe(`List Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('list-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('LPUSH and RPUSH commands', async () => {
    // LPUSH single item
    const lpush1 = await redisClient?.lpush('list1', 'item1')
    assert.strictEqual(lpush1, 1)

    // LPUSH multiple items
    const lpush2 = await redisClient?.lpush('list1', 'item2', 'item3')
    assert.strictEqual(lpush2, 3)

    // RPUSH items
    const rpush1 = await redisClient?.rpush('list1', 'item4', 'item5')
    assert.strictEqual(rpush1, 5)
  })

  test('LPOP and RPOP commands', async () => {
    await redisClient?.lpush('list2', 'a', 'b', 'c')

    // LPOP
    const lpop = await redisClient?.lpop('list2')
    assert.strictEqual(lpop, 'c') // Last pushed is first popped

    // RPOP
    const rpop = await redisClient?.rpop('list2')
    assert.strictEqual(rpop, 'a') // First pushed is last popped

    // Remaining item
    const remaining = await redisClient?.lpop('list2')
    assert.strictEqual(remaining, 'b')

    // Empty list
    const empty = await redisClient?.lpop('list2')
    assert.strictEqual(empty, null)
  })

  test('LLEN command', async () => {
    // Empty list
    const len1 = await redisClient?.llen('emptylist')
    assert.strictEqual(len1, 0)

    await redisClient?.lpush('list3', 'a', 'b', 'c')
    const len2 = await redisClient?.llen('list3')
    assert.strictEqual(len2, 3)
  })

  test('LINDEX command', async () => {
    await redisClient?.lpush('list4', 'a', 'b', 'c') // [c, b, a]

    const index0 = await redisClient?.lindex('list4', 0)
    assert.strictEqual(index0, 'c')

    const index1 = await redisClient?.lindex('list4', 1)
    assert.strictEqual(index1, 'b')

    const indexNeg1 = await redisClient?.lindex('list4', -1)
    assert.strictEqual(indexNeg1, 'a')

    const indexOut = await redisClient?.lindex('list4', 10)
    assert.strictEqual(indexOut, null)
  })

  test('LRANGE command', async () => {
    await redisClient?.lpush('list5', 'a', 'b', 'c', 'd', 'e') // [e, d, c, b, a]

    // Get all elements
    const all = await redisClient?.lrange('list5', 0, -1)
    assert.deepStrictEqual(all, ['e', 'd', 'c', 'b', 'a'])

    // Get subset
    const subset = await redisClient?.lrange('list5', 1, 3)
    assert.deepStrictEqual(subset, ['d', 'c', 'b'])

    // Get from negative index
    const fromNeg = await redisClient?.lrange('list5', -2, -1)
    assert.deepStrictEqual(fromNeg, ['b', 'a'])
  })

  test('LSET command', async () => {
    await redisClient?.lpush('list6', 'a', 'b', 'c') // [c, b, a]

    // Set element at index 1
    await redisClient?.lset('list6', 1, 'newb')

    const check = await redisClient?.lindex('list6', 1)
    assert.strictEqual(check, 'newb')

    // Verify full list
    const all = await redisClient?.lrange('list6', 0, -1)
    assert.deepStrictEqual(all, ['c', 'newb', 'a'])
  })

  test('LREM command', async () => {
    await redisClient?.rpush('list7', 'a', 'b', 'a', 'c', 'a') // [a, b, a, c, a]

    // Remove 2 occurrences of 'a' from left
    const rem1 = await redisClient?.lrem('list7', 2, 'a')
    assert.strictEqual(rem1, 2)

    const after1 = await redisClient?.lrange('list7', 0, -1)
    assert.deepStrictEqual(after1, ['b', 'c', 'a'])

    // Remove all occurrences of 'a'
    const rem2 = await redisClient?.lrem('list7', 0, 'a')
    assert.strictEqual(rem2, 1)

    const after2 = await redisClient?.lrange('list7', 0, -1)
    assert.deepStrictEqual(after2, ['b', 'c'])
  })

  test('LTRIM command', async () => {
    await redisClient?.rpush('list8', 'a', 'b', 'c', 'd', 'e') // [a, b, c, d, e]

    // Trim to keep only elements 1-3
    await redisClient?.ltrim('list8', 1, 3)

    const trimmed = await redisClient?.lrange('list8', 0, -1)
    assert.deepStrictEqual(trimmed, ['b', 'c', 'd'])

    const len = await redisClient?.llen('list8')
    assert.strictEqual(len, 3)
  })

  test('List commands workflow - Task Queue', async () => {
    const queueKey = 'tasks:urgent'

    // Add tasks to queue (FIFO - use RPUSH to add, LPOP to consume)
    await redisClient?.rpush(queueKey, 'task1', 'task2', 'task3')

    // Check queue size
    const queueSize = await redisClient?.llen(queueKey)
    assert.strictEqual(queueSize, 3)

    // Peek at next task without removing
    const nextTask = await redisClient?.lindex(queueKey, 0)
    assert.strictEqual(nextTask, 'task1')

    // Process tasks one by one
    const processed1 = await redisClient?.lpop(queueKey)
    assert.strictEqual(processed1, 'task1')

    const processed2 = await redisClient?.lpop(queueKey)
    assert.strictEqual(processed2, 'task2')

    // Add urgent task to front
    await redisClient?.lpush(queueKey, 'urgent_task')

    // Check queue state
    const currentQueue = await redisClient?.lrange(queueKey, 0, -1)
    assert.deepStrictEqual(currentQueue, ['urgent_task', 'task3'])

    // Process urgent task
    const urgentProcessed = await redisClient?.lpop(queueKey)
    assert.strictEqual(urgentProcessed, 'urgent_task')

    // Check final queue
    const finalQueue = await redisClient?.lrange(queueKey, 0, -1)
    assert.deepStrictEqual(finalQueue, ['task3'])
  })

  test('List commands workflow - Chat Messages', async () => {
    const chatKey = 'chat:room123'

    // Add messages
    await redisClient?.rpush(
      chatKey,
      'Alice: Hello!',
      'Bob: Hi Alice!',
      'Charlie: Hey everyone!',
    )

    // Get recent messages (last 10)
    const recentMessages = await redisClient?.lrange(chatKey, -10, -1)
    assert.strictEqual(recentMessages?.length, 3)
    assert.strictEqual(recentMessages?.[0], 'Alice: Hello!')

    // Add more messages
    await redisClient?.rpush(
      chatKey,
      'Alice: How is everyone?',
      'Bob: Good!',
      'Charlie: Great!',
    )

    // Trim to keep only last 5 messages
    await redisClient?.ltrim(chatKey, -5, -1)

    const trimmedMessages = await redisClient?.lrange(chatKey, 0, -1)
    assert.strictEqual(trimmedMessages?.length, 5)
    assert.strictEqual(trimmedMessages?.[0], 'Bob: Hi Alice!')

    // Edit a message (replace at specific index)
    await redisClient?.lset(chatKey, 0, 'Bob: Hi Alice! (edited)')

    const editedMessage = await redisClient?.lindex(chatKey, 0)
    assert.strictEqual(editedMessage, 'Bob: Hi Alice! (edited)')

    // Remove inappropriate messages
    await redisClient?.rpush(chatKey, 'Spam: Buy now!', 'Alice: Thanks Bob!')

    // Remove spam messages
    const removed = await redisClient?.lrem(chatKey, 0, 'Spam: Buy now!')
    assert.strictEqual(removed, 1)

    // Check final chat
    const finalChat = await redisClient?.lrange(chatKey, 0, -1)
    assert.ok(!finalChat?.includes('Spam: Buy now!'))
    assert.ok(finalChat?.includes('Alice: Thanks Bob!'))
  })

  test('List commands workflow - Undo Stack', async () => {
    const undoKey = 'user:123:undo'

    // Simulate user actions (LIFO - use LPUSH to add, LPOP to undo)
    await redisClient?.lpush(undoKey, 'action:create_file')
    await redisClient?.lpush(undoKey, 'action:edit_line_5')
    await redisClient?.lpush(undoKey, 'action:delete_line_3')

    // Check undo stack
    const stackSize = await redisClient?.llen(undoKey)
    assert.strictEqual(stackSize, 3)

    // Peek at last action
    const lastAction = await redisClient?.lindex(undoKey, 0)
    assert.strictEqual(lastAction, 'action:delete_line_3')

    // Undo last action
    const undone1 = await redisClient?.lpop(undoKey)
    assert.strictEqual(undone1, 'action:delete_line_3')

    // Undo another action
    const undone2 = await redisClient?.lpop(undoKey)
    assert.strictEqual(undone2, 'action:edit_line_5')

    // Check remaining stack
    const remaining = await redisClient?.lrange(undoKey, 0, -1)
    assert.deepStrictEqual(remaining, ['action:create_file'])

    // Add new action after undo
    await redisClient?.lpush(undoKey, 'action:new_edit')

    const newStack = await redisClient?.lrange(undoKey, 0, -1)
    assert.deepStrictEqual(newStack, ['action:new_edit', 'action:create_file'])
  })
})
