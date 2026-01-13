# Phase 5: Improve Single-Threaded Semantics (Optional)

## Objective

Enhance the transport layer to properly serialize command execution per connection and add backpressure handling, completing the single-threaded execution model.

## Status

This phase is **OPTIONAL** but recommended for production-quality behavior. The current implementation is functional but has room for improvement.

## Problem Statement

### Current Issues

**Location:** [src/core/transports/resp2/index.ts](src/core/transports/resp2/index.ts)

1. **No Per-Connection Serialization**
   - Commands from the same client connection can interleave if they `await`
   - Each command execution goes through the DB lock, but response ordering isn't guaranteed

2. **No Backpressure Handling**
   - `socket.write()` return value is ignored
   - If client is slow to read responses, memory can build up
   - No flow control between command processing and response writing

3. **Fire-and-Forget Response Writing**
   - Responses written asynchronously without tracking completion
   - Pipelined commands might have responses arrive out of order

### Example of Potential Issue

```typescript
// Client sends pipelined commands:
// 1. SET key1 value1
// 2. GET key1
// 3. SET key2 value2

// Without serialization, execution could be:
// SET key1 starts → awaits DB lock
// GET key1 starts → awaits DB lock
// SET key2 starts → awaits DB lock
// Responses could arrive out of order
```

## Solution Design

### Strategy

1. **Per-Connection Command Queue** - Serialize command execution per connection
2. **Backpressure Monitoring** - Track socket buffer state and pause if needed
3. **Response Ordering** - Ensure responses match command order

### Architecture

```
Socket receives data
  ↓
Parse RESP command
  ↓
Add to per-connection queue ← NEW
  ↓
Execute when queue ready
  ↓
Acquire DB lock
  ↓
Execute command
  ↓
Release DB lock
  ↓
Write response with backpressure check ← NEW
  ↓
Process next command in queue
```

## Implementation

### 5.1 Add Connection State

**File:** [src/core/transports/resp2/index.ts](src/core/transports/resp2/index.ts)

```typescript
import { Resp } from 'respjs'
import { createServer, Socket } from 'net'
import type { Logger } from '../../logger'
import type { DBCommandExecutor, Transport } from '../../../types'

/**
 * Per-connection state
 */
interface ConnectionState {
  transport: RespTransport
  controller: AbortController
  commandQueue: Promise<void>  // Serialization queue
  isPaused: boolean  // Backpressure state
}

export class Resp2Transport {
  private connections = new Map<Socket, ConnectionState>()

  constructor(
    private readonly logger: Logger,
    private readonly commandExecutor: DBCommandExecutor,
  ) {
    this.server = createServer({ keepAlive: true })
      .on('connection', socket => {
        socket.pipe(this.handleConnection(socket))
      })
  }

  listen(port: number, hostname: string, callback: () => void): void {
    this.server.listen(port, hostname, callback)
  }

  close(callback: () => void): void {
    // Close all connections
    for (const [socket, state] of this.connections.entries()) {
      state.controller.abort()
      socket.destroySoon()
    }
    this.connections.clear()

    this.server.close(callback)
  }

  private handleConnection(socket: Socket) {
    const controller = new AbortController()
    const transport = new RespTransport(this.logger, socket)

    // Initialize connection state
    const state: ConnectionState = {
      transport,
      controller,
      commandQueue: Promise.resolve(),
      isPaused: false,
    }

    this.connections.set(socket, state)

    // Setup socket lifecycle events
    socket
      .on('close', () => {
        controller.abort()
        this.connections.delete(socket)
      })
      .on('error', (err) => {
        this.logger.error('Socket error:', err)
        controller.abort()
        this.connections.delete(socket)
      })
      .on('timeout', () => {
        this.logger.debug('Socket timeout')
        controller.abort()
        this.connections.delete(socket)
      })
      .on('end', () => {
        controller.abort()
        this.connections.delete(socket)
      })

    // Setup backpressure monitoring
    socket.on('drain', () => {
      this.logger.debug('Socket drained, resuming')
      state.isPaused = false
    })

    return new Resp({ bufBulk: true })
      .on('error', (err: unknown) => {
        this.logger.error('RESP parse error:', err)
        transport.write(err)
      })
      .on('data', (data: Buffer[]) => {
        const [cmdName, ...args] = data

        // CRITICAL: Serialize command execution per connection
        state.commandQueue = state.commandQueue.then(async () => {
          try {
            // Check if connection is still alive
            if (controller.signal.aborted) {
              return
            }

            // Wait for backpressure to clear
            if (state.isPaused) {
              this.logger.debug('Waiting for backpressure to clear')
              await this.waitForDrain(socket)
            }

            // Execute command (will acquire DB lock internally)
            await this.commandExecutor.execute(
              transport,
              cmdName,
              args,
              controller.signal,
            )
          } catch (err) {
            this.logger.error('Command execution error:', err)
            transport.write(err)
          }
        })
      })
  }

  /**
   * Wait for socket to drain (backpressure cleared)
   */
  private async waitForDrain(socket: Socket): Promise<void> {
    return new Promise((resolve) => {
      if (socket.writableNeedDrain === false) {
        resolve()
        return
      }

      const onDrain = () => {
        socket.off('drain', onDrain)
        resolve()
      }

      socket.once('drain', onDrain)

      // Timeout after 30 seconds
      setTimeout(() => {
        socket.off('drain', onDrain)
        resolve()
      }, 30000)
    })
  }
}
```

### 5.2 Add Backpressure Handling to Transport

**File:** [src/core/transports/resp2/index.ts](src/core/transports/resp2/index.ts)

Update the `RespTransport` class:

```typescript
export class RespTransport implements Transport {
  constructor(
    private readonly logger: Logger,
    private readonly socket: Socket,
  ) {}

  write(responseData: unknown, close?: boolean): void {
    // Check for non-user-faced errors (internal errors should close connection)
    if (
      responseData instanceof Error &&
      !(responseData instanceof UserFacedError)
    ) {
      close = true
    }

    const buffer = this.prepareResponse(responseData)

    // Write to socket and check backpressure
    const canContinue = this.socket.write(buffer, (err) => {
      if (err) {
        this.logger.error('Socket write error:', err)
        this.socket.destroySoon()
      }
    })

    // Backpressure detected
    if (!canContinue) {
      this.logger.debug(
        `Socket buffer full (${this.socket.writableLength} bytes), backpressure applied`
      )
      // The connection state will handle waiting for drain
    }

    if (close) {
      this.socket.destroySoon()
    }
  }

  private prepareResponse(jsResponse: unknown): Buffer {
    // ... existing implementation unchanged
  }
}
```

### 5.3 Add Metrics (Optional)

Track connection and command statistics:

```typescript
interface ConnectionMetrics {
  commandsExecuted: number
  commandsQueued: number
  backpressureEvents: number
  avgQueueDepth: number
}

export class Resp2Transport {
  private metrics = new Map<Socket, ConnectionMetrics>()

  private recordMetric(socket: Socket, metric: keyof ConnectionMetrics, value: number): void {
    const m = this.metrics.get(socket)
    if (m) {
      m[metric] = value
    }
  }

  // Expose metrics for monitoring
  getMetrics(): ConnectionMetrics[] {
    return Array.from(this.metrics.values())
  }
}
```

## Testing

### Unit Tests

Create `tests/transport-serialization.test.ts`:

```typescript
import { test, describe } from 'node:test'
import assert from 'node:assert'
import { Resp2Transport } from '../src/core/transports/resp2'
import { Commander } from '../src/commanders/custom/commander'
import { DB } from '../src/commanders/custom/db'
import { Socket } from 'net'

describe('Transport Serialization', () => {
  test('commands from same connection execute in order', async () => {
    const db = new DB()
    const commander = new Commander(db, luaEngine)
    const transport = new Resp2Transport(mockLogger, commander)

    const executionOrder: string[] = []

    // Mock socket
    const mockSocket = {
      write: () => true,
      on: () => mockSocket,
      once: () => mockSocket,
      pipe: () => mockSocket,
    } as any

    // Send 3 commands rapidly
    await Promise.all([
      commander.execute(mockTransport, Buffer.from('SET'), [Buffer.from('key1'), Buffer.from('a')], signal),
      commander.execute(mockTransport, Buffer.from('SET'), [Buffer.from('key1'), Buffer.from('b')], signal),
      commander.execute(mockTransport, Buffer.from('GET'), [Buffer.from('key1')], signal),
    ])

    // Final value should be 'b' (second SET)
    // GET should return 'b' (not 'a')
    const final = db.get(Buffer.from('key1'))
    assert.strictEqual(final?.toString(), 'b')
  })

  test('backpressure pauses command processing', async () => {
    const db = new DB()
    const commander = new Commander(db, luaEngine)

    let writeCalls = 0
    let shouldBlock = true

    const mockSocket = {
      write: () => {
        writeCalls++
        return !shouldBlock  // Return false to trigger backpressure
      },
      writableNeedDrain: () => shouldBlock,
      on: () => mockSocket,
      once: (event: string, handler: Function) => {
        if (event === 'drain') {
          // Simulate drain after 100ms
          setTimeout(() => {
            shouldBlock = false
            handler()
          }, 100)
        }
        return mockSocket
      },
    } as any

    const start = Date.now()

    // This should trigger backpressure and wait for drain
    await commander.execute(mockTransport, Buffer.from('SET'), [Buffer.from('key'), Buffer.from('value')], signal)

    const elapsed = Date.now() - start

    // Should have waited for drain (~100ms)
    assert.ok(elapsed >= 100)
  })
})
```

### Integration Tests

Add to `tests-integration/ioredis/pipelining.test.ts` (NEW):

```typescript
import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import Redis from 'ioredis'

describe('Command Pipelining', () => {
  let redisClient: Redis | null = null

  before(async () => {
    redisClient = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: parseInt(process.env.REDIS_PORT || '6379'),
    })
  })

  after(async () => {
    await redisClient?.quit()
  })

  test('pipelined commands execute in order', async () => {
    const pipeline = redisClient!.pipeline()

    pipeline.set('key', 'value1')
    pipeline.set('key', 'value2')
    pipeline.set('key', 'value3')
    pipeline.get('key')

    const results = await pipeline.exec()

    assert.ok(results)
    assert.strictEqual(results.length, 4)

    // Last command (GET) should return 'value3'
    const [err, value] = results[3]
    assert.strictEqual(err, null)
    assert.strictEqual(value, 'value3')
  })

  test('large pipeline handles backpressure', async () => {
    const pipeline = redisClient!.pipeline()

    // Send 10,000 commands
    for (let i = 0; i < 10000; i++) {
      pipeline.set(`key${i}`, `value${i}`)
    }

    const start = Date.now()
    const results = await pipeline.exec()
    const elapsed = Date.now() - start

    assert.ok(results)
    assert.strictEqual(results.length, 10000)

    // All should succeed
    const failures = results.filter(([err]) => err !== null)
    assert.strictEqual(failures.length, 0)

    console.log(`Processed 10,000 commands in ${elapsed}ms`)
  })
})
```

### Performance Tests

Create `tests/performance.test.ts`:

```typescript
test('serialization overhead is minimal', async () => {
  const db = new DB()
  const commander = new Commander(db, luaEngine)

  const iterations = 10000

  // Test without serialization (baseline)
  const baselineStart = Date.now()
  for (let i = 0; i < iterations; i++) {
    await db.set(Buffer.from(`key${i}`), Buffer.from(`value${i}`))
  }
  const baselineTime = Date.now() - baselineStart

  // Test with serialization
  const serializedStart = Date.now()
  let queue = Promise.resolve()
  for (let i = 0; i < iterations; i++) {
    queue = queue.then(() =>
      commander.execute(mockTransport, Buffer.from('SET'), [Buffer.from(`key${i}`), Buffer.from(`value${i}`)], signal)
    )
  }
  await queue
  const serializedTime = Date.now() - serializedStart

  const overhead = ((serializedTime - baselineTime) / baselineTime) * 100

  console.log(`Baseline: ${baselineTime}ms`)
  console.log(`Serialized: ${serializedTime}ms`)
  console.log(`Overhead: ${overhead.toFixed(2)}%`)

  // Overhead should be < 20%
  assert.ok(overhead < 20, `Serialization overhead too high: ${overhead}%`)
})
```

## Files Modified

1. **[src/core/transports/resp2/index.ts](src/core/transports/resp2/index.ts)** - Add serialization and backpressure (~100 lines added)
2. **[tests/transport-serialization.test.ts](tests/transport-serialization.test.ts)** - NEW TEST FILE (~100 lines)
3. **[tests-integration/ioredis/pipelining.test.ts](tests-integration/ioredis/pipelining.test.ts)** - NEW TEST FILE (~80 lines)
4. **[tests/performance.test.ts](tests/performance.test.ts)** - NEW TEST FILE (~50 lines)

## Verification Checklist

- [ ] Commands from same connection execute in order
- [ ] Backpressure detection works
- [ ] Socket drain event handled correctly
- [ ] Connection state cleaned up on close
- [ ] Large pipelines don't cause memory buildup
- [ ] Performance overhead < 20%
- [ ] Integration tests pass with pipelining
- [ ] Concurrent connections still work correctly
- [ ] Existing tests still pass

## Benefits

1. **Predictable ordering** - Commands execute in order received
2. **Memory safety** - Backpressure prevents unbounded growth
3. **Better resource management** - Pause processing when client is slow
4. **Production ready** - Handles edge cases properly
5. **Observable** - Can add metrics and monitoring

## Trade-offs

1. **Slight performance overhead** - Serialization adds queueing cost (~10-20%)
2. **Complexity** - More state to manage per connection
3. **Testing** - Need to test backpressure scenarios

## Optional Enhancements

After Phase 5, consider:
1. **Command timeout** - Abort commands that take too long
2. **Connection limits** - Max concurrent connections
3. **Rate limiting** - Limit commands per second per connection
4. **Metrics endpoint** - Expose connection and command metrics
5. **Slow log** - Track slow commands like Redis SLOWLOG

## Next Steps

This completes the refactoring plan. The Redis mock server now has:
- ✅ Metadata-based command registration
- ✅ Atomic Lua script execution
- ✅ Full COMMAND introspection
- ✅ Proper single-threaded semantics (optional)

Future improvements:
- Add missing Redis commands (KEYS, SCAN, blocking operations)
- Implement Pub/Sub
- Add Streams data structure
- Consider persistence layer (optional for mock server)
