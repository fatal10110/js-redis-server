import { test, describe } from 'node:test'
import assert from 'node:assert'
import { RedisServerState } from '../src/state'

async function waitUntil(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 2000
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error('condition still false after 2000ms')
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

describe('RedisServerState.reopen', () => {
  test('keeps the data and re-arms active expiry', async () => {
    const server = new RedisServerState({ activeExpiryIntervalMs: 5 })
    try {
      const db = server.getDatabase(0)
      db.setString(Buffer.from('kept'), Buffer.from('v'))
      server.close()
      server.reopen()

      assert.strictEqual(db.size(), 1)
      let evicted = false
      db.subscribe(event => {
        if (event.type === 'evict') {
          evicted = true
        }
      })
      db.setString(Buffer.from('expiring'), Buffer.from('v'), {
        expiresAt: Date.now() + 10,
      })
      // Swept without a read: the expiry loop runs again.
      await waitUntil(() => evicted)
      assert.strictEqual(db.size(), 1)
    } finally {
      server.close()
    }
  })

  test('is a no-op on an open state', () => {
    const server = new RedisServerState({ activeExpiryIntervalMs: false })
    server.reopen()
    server.close()
  })
})
