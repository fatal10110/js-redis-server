import { test, describe, beforeEach } from 'node:test'
import assert from 'node:assert'
import { DB, StoreEvent } from '../src/commanders/custom/db'
import { StringDataType } from '../src/commanders/custom/data-structures/string'
import { ListDataType } from '../src/commanders/custom/data-structures/list'
import {
  RedisKernel,
  CommandJob,
  JobHandlerResult,
} from '../src/commanders/custom/redis-kernel'

describe('Reactive Store', () => {
  let db: DB

  beforeEach(() => {
    db = new DB()
  })

  describe('change event emission', () => {
    test('emits set event on db.set()', async () => {
      const events: StoreEvent[] = []
      db.on('change', event => events.push(event))

      const key = Buffer.from('mykey')
      const value = new StringDataType(Buffer.from('myvalue'))
      db.set(key, value)

      assert.strictEqual(events.length, 1)
      assert.strictEqual(events[0].type, 'set')
      if (events[0].type === 'set') {
        assert.strictEqual(events[0].key.toString(), 'mykey')
        assert.strictEqual(events[0].value, value)
      }
    })

    test('emits del event on db.del()', async () => {
      const key = Buffer.from('mykey')
      db.set(key, new StringDataType(Buffer.from('value')))

      const events: StoreEvent[] = []
      db.on('change', event => events.push(event))

      db.del(key)

      assert.strictEqual(events.length, 1)
      assert.strictEqual(events[0].type, 'del')
      if (events[0].type === 'del') {
        assert.strictEqual(events[0].key.toString(), 'mykey')
      }
    })

    test('emits expire event on setExpiration()', async () => {
      const key = Buffer.from('mykey')
      db.set(key, new StringDataType(Buffer.from('value')))

      const events: StoreEvent[] = []
      db.on('change', event => events.push(event))

      const expiration = Date.now() + 10000
      db.setExpiration(key, expiration)

      assert.strictEqual(events.length, 1)
      assert.strictEqual(events[0].type, 'expire')
      if (events[0].type === 'expire') {
        assert.strictEqual(events[0].key.toString(), 'mykey')
        assert.strictEqual(events[0].expiration, expiration)
      }
    })

    test('emits flush event on flushdb()', async () => {
      db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value1')))
      db.set(Buffer.from('key2'), new StringDataType(Buffer.from('value2')))

      const events: StoreEvent[] = []
      db.on('change', event => events.push(event))

      db.flushdb()

      assert.strictEqual(events.length, 1)
      assert.strictEqual(events[0].type, 'flush')
    })

    test('emits evict event when key expires on access', async () => {
      const key = Buffer.from('expiring')
      const expiration = Date.now() - 100 // Already expired
      db.set(key, new StringDataType(Buffer.from('value')), expiration)

      const events: StoreEvent[] = []
      db.on('change', event => events.push(event))

      // Access will trigger eviction
      db.get(key)

      assert.strictEqual(events.length, 1)
      assert.strictEqual(events[0].type, 'evict')
      if (events[0].type === 'evict') {
        assert.strictEqual(events[0].key.toString(), 'expiring')
      }
    })
  })

  describe('key-specific event emission', () => {
    test('emits key-specific event on set', async () => {
      const events: StoreEvent[] = []
      const key = Buffer.from('mykey')
      db.on(`key:${key.toString('hex')}`, event => events.push(event))

      db.set(key, new StringDataType(Buffer.from('value')))
      db.set(Buffer.from('otherkey'), new StringDataType(Buffer.from('other')))

      assert.strictEqual(events.length, 1)
      assert.strictEqual(events[0].type, 'set')
    })

    test('emits key-specific event on del', async () => {
      const key = Buffer.from('watchedkey')
      db.set(key, new StringDataType(Buffer.from('value')))

      const events: StoreEvent[] = []
      db.on(`key:${key.toString('hex')}`, event => events.push(event))

      db.del(key)

      assert.strictEqual(events.length, 1)
      assert.strictEqual(events[0].type, 'del')
    })

    test('once() handler fires only once (WATCH pattern)', async () => {
      const key = Buffer.from('watched')
      db.set(key, new StringDataType(Buffer.from('initial')))

      let watchTriggered = false
      db.once(`key:${key.toString('hex')}`, () => {
        watchTriggered = true
      })

      // First change should trigger
      db.set(key, new StringDataType(Buffer.from('changed1')))
      assert.strictEqual(watchTriggered, true)

      // Reset and verify second change doesn't trigger
      watchTriggered = false
      db.set(key, new StringDataType(Buffer.from('changed2')))
      assert.strictEqual(watchTriggered, false)
    })
  })

  describe('WATCH implementation pattern', () => {
    test('can implement WATCH-like behavior using once()', async () => {
      const key = Buffer.from('transactionKey')
      db.set(key, new StringDataType(Buffer.from('original')))

      // Simulate WATCH - transaction context tracks if key changed
      let transactionAborted = false
      const watchKey = (keyToWatch: Buffer) => {
        db.once(`key:${keyToWatch.toString('hex')}`, () => {
          transactionAborted = true
        })
      }

      watchKey(key)

      // Simulate another client modifying the key
      db.set(key, new StringDataType(Buffer.from('modified')))

      // Transaction should be aborted
      assert.strictEqual(transactionAborted, true)
    })

    test('WATCH on non-modified key allows transaction', async () => {
      const key1 = Buffer.from('key1')
      const key2 = Buffer.from('key2')
      db.set(key1, new StringDataType(Buffer.from('value1')))
      db.set(key2, new StringDataType(Buffer.from('value2')))

      let transactionAborted = false
      db.once(`key:${key1.toString('hex')}`, () => {
        transactionAborted = true
      })

      // Modify a different key
      db.set(key2, new StringDataType(Buffer.from('newvalue2')))

      // Transaction should NOT be aborted
      assert.strictEqual(transactionAborted, false)
    })
  })

  describe('multiple listeners', () => {
    test('multiple change listeners receive events', async () => {
      let listener1Count = 0
      let listener2Count = 0

      db.on('change', () => listener1Count++)
      db.on('change', () => listener2Count++)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))

      assert.strictEqual(listener1Count, 1)
      assert.strictEqual(listener2Count, 1)
    })

    test('removeListener stops event delivery', async () => {
      let eventCount = 0
      const listener = () => eventCount++

      db.on('change', listener)
      db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value1')))
      assert.strictEqual(eventCount, 1)

      db.removeListener('change', listener)
      db.set(Buffer.from('key2'), new StringDataType(Buffer.from('value2')))
      assert.strictEqual(eventCount, 1) // Should not increment
    })
  })
})

describe('Kernel Suspended Jobs', () => {
  function createMockJob(id: string): CommandJob {
    return {
      id,
      connectionId: 'conn-1',
      request: {
        command: Buffer.from('TEST'),
        args: [],
        transport: { write: () => {} },
        signal: new AbortController().signal,
      },
      resolve: () => {},
      reject: () => {},
    }
  }

  test('kernel processes normal jobs sequentially', async () => {
    const executionOrder: string[] = []

    const kernel = new RedisKernel(async job => {
      executionOrder.push(`start:${job.id}`)
      await new Promise(r => setImmediate(r))
      executionOrder.push(`end:${job.id}`)
    })

    const job1 = createMockJob('job1')
    const job2 = createMockJob('job2')

    const p1 = new Promise<void>((resolve, reject) => {
      job1.resolve = resolve
      job1.reject = reject
    })
    const p2 = new Promise<void>((resolve, reject) => {
      job2.resolve = resolve
      job2.reject = reject
    })

    kernel.submit(job1)
    kernel.submit(job2)

    await Promise.all([p1, p2])

    assert.deepStrictEqual(executionOrder, [
      'start:job1',
      'end:job1',
      'start:job2',
      'end:job2',
    ])
  })

  test('suspended job allows other jobs to process', async () => {
    const executionOrder: string[] = []
    let resolveSuspended: () => void

    const kernel = new RedisKernel(async (job): Promise<JobHandlerResult> => {
      executionOrder.push(`start:${job.id}`)

      if (job.id === 'blocking') {
        // Return suspended - this job will wait
        const suspendedPromise = new Promise<void>(resolve => {
          resolveSuspended = resolve
        })
        executionOrder.push(`suspend:${job.id}`)
        return { suspended: suspendedPromise }
      }

      await new Promise(r => setImmediate(r))
      executionOrder.push(`end:${job.id}`)
    })

    const blockingJob = createMockJob('blocking')
    const normalJob = createMockJob('normal')

    const pBlocking = new Promise<void>((resolve, reject) => {
      blockingJob.resolve = resolve
      blockingJob.reject = reject
    })
    const pNormal = new Promise<void>((resolve, reject) => {
      normalJob.resolve = resolve
      normalJob.reject = reject
    })

    kernel.submit(blockingJob)
    kernel.submit(normalJob)

    // Wait for normal job to complete
    await pNormal

    // Normal job should complete while blocking is still suspended
    assert.deepStrictEqual(executionOrder, [
      'start:blocking',
      'suspend:blocking',
      'start:normal',
      'end:normal',
    ])

    assert.strictEqual(kernel.getSuspendedCount(), 1)

    // Now resolve the suspended job
    resolveSuspended!()
    await pBlocking

    assert.strictEqual(kernel.getSuspendedCount(), 0)
  })

  test('BLPOP-like pattern with reactive store', async () => {
    const db = new DB()
    const executionOrder: string[] = []

    const kernel = new RedisKernel(async (job): Promise<JobHandlerResult> => {
      const cmd = job.request.command.toString()
      executionOrder.push(`start:${cmd}`)

      if (cmd === 'BLPOP') {
        const key = job.request.args[0]
        const list = db.get(key)

        // If list has items, pop and return immediately
        if (list instanceof ListDataType && list.llen() > 0) {
          const value = list.lpop()
          executionOrder.push(`blpop:immediate:${value?.toString()}`)
          return
        }

        // Otherwise, suspend and wait for a push
        const suspendedPromise = new Promise<void>(resolve => {
          db.once(`key:${key.toString('hex')}`, event => {
            if (event.type === 'set') {
              const updatedList = db.get(key)
              if (updatedList instanceof ListDataType) {
                const value = updatedList.lpop()
                executionOrder.push(`blpop:wakeup:${value?.toString()}`)
              }
            }
            resolve()
          })
        })

        executionOrder.push(`blpop:suspend`)
        return { suspended: suspendedPromise }
      }

      if (cmd === 'LPUSH') {
        const key = job.request.args[0]
        const value = job.request.args[1]
        let list = db.get(key)
        if (!(list instanceof ListDataType)) {
          list = new ListDataType()
        }
        list.lpush(value)
        db.set(key, list)
        executionOrder.push(`lpush:${value.toString()}`)
        return
      }
    })

    // Submit BLPOP on empty list - should suspend
    const blpopJob = createMockJob('blpop')
    blpopJob.request.command = Buffer.from('BLPOP')
    blpopJob.request.args = [Buffer.from('mylist')]

    const pBlpop = new Promise<void>((resolve, reject) => {
      blpopJob.resolve = resolve
      blpopJob.reject = reject
    })

    kernel.submit(blpopJob)

    // Wait a tick for BLPOP to suspend
    await new Promise(r => setImmediate(r))
    await new Promise(r => setImmediate(r))

    assert.strictEqual(kernel.getSuspendedCount(), 1)

    // Submit LPUSH - should wake up BLPOP
    const lpushJob = createMockJob('lpush')
    lpushJob.request.command = Buffer.from('LPUSH')
    lpushJob.request.args = [Buffer.from('mylist'), Buffer.from('hello')]

    const pLpush = new Promise<void>((resolve, reject) => {
      lpushJob.resolve = resolve
      lpushJob.reject = reject
    })

    kernel.submit(lpushJob)

    await Promise.all([pBlpop, pLpush])

    // Note: wakeup happens synchronously during db.set() before lpush completes
    assert.deepStrictEqual(executionOrder, [
      'start:BLPOP',
      'blpop:suspend',
      'start:LPUSH',
      'blpop:wakeup:hello', // Event fires synchronously during db.set
      'lpush:hello',
    ])

    assert.strictEqual(kernel.getSuspendedCount(), 0)
  })
})
