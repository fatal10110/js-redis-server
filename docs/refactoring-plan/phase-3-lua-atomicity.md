# Phase 3: Fix Lua Script Atomicity

## Objective

Fix the critical race condition in Lua script execution where `redis.call()` acquires and releases the mutex separately, allowing other clients to execute commands between Lua calls. Ensure Lua scripts execute atomically as a single operation, matching Redis behavior.

## Problem Statement

### Current Behavior (BROKEN)

**Location:** [src/commanders/custom/commands/redis/eval.ts:67](src/commanders/custom/commands/redis/eval.ts#L67)

```typescript
// Current implementation (simplified)
async run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
  // No lock acquired here

  this.lua.global.set('redisCall', async (cmdName: string, args: string[]) => {
    const cmd = this.commands[cmdName]
    const { response } = await cmd.run(rawCmd, argsBuffer, signal)
    // ^^^^^ Each call to cmd.run() goes through ExecutionContext which acquires/releases lock
    return convertResponse(response)
  })

  const result = await this.lua.doString(`return (${script})(...)`)
  return { response: convertResult(result) }
}
```

**The Race Condition:**

```lua
-- Lua script
local val = redis.call('GET', KEYS[1])  -- Lock acquired → released
redis.call('SET', KEYS[1], val + 1)      -- Lock acquired → released
-- ↑ Another client can execute commands between these calls!
```

**Timeline of the bug:**
```
Time | Client A (Lua script)              | Client B
-----|------------------------------------|-----------------
t1   | Acquire lock                       |
t2   | GET mykey → returns 10             |
t3   | Release lock                       |
t4   |                                    | Acquire lock
t5   |                                    | SET mykey 999
t6   |                                    | Release lock
t7   | Acquire lock                       |
t8   | SET mykey 11 (should be 11 but overwrites 999!) |
t9   | Release lock                       |
```

**Expected Redis Behavior:**

Lua scripts execute atomically - the entire script holds the lock from start to finish. No other client can execute commands during script execution.

### Root Cause

Commands called from Lua go through `CommandExecutionContext.execute()`, which always acquires the DB lock:

```typescript
// src/commanders/custom/execution-context.ts:51
async execute(...): Promise<ExecutionContext> {
  const release = await this.db.lock.acquire()  // Always acquires
  try {
    const res = await cmd.run(rawCmd, args, signal)
    transport.write(res.response, res.close)
  } finally {
    release()  // Always releases
  }
}
```

## Solution Design

### Strategy

1. **Hold lock for entire script** - EVAL/EVALSHA acquire lock once at script start
2. **Pass lock context to nested commands** - Commands called from Lua don't re-acquire
3. **Synchronous transport for Lua** - Results returned directly, not written to socket
4. **Maintain backward compatibility** - Normal commands still acquire lock as before

### Architecture Changes

#### 1. Add Lock Context to Execution

Modify execution path to track whether lock is already held.

**Option A: Add parameter to execute()** (Chosen approach)

```typescript
interface ExecutionContext {
  execute(
    transport: Transport,
    cmdName: string,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
    lockContext?: LockContext  // NEW: Track lock state
  ): Promise<ExecutionContext>
}

interface LockContext {
  lockHeld: boolean  // Is lock currently held?
}
```

**Option B: Separate execution method** (More explicit but verbose)

```typescript
interface ExecutionContext {
  execute(...): Promise<ExecutionContext>
  executeWithLockHeld(...): Promise<ExecutionContext>  // New method
}
```

We'll use **Option A** for minimal changes.

#### 2. Special Transport for Lua

Lua scripts need command results directly, not written to socket.

Create `src/commanders/custom/lua-transport.ts`:

```typescript
import type { Transport } from '../../types'

/**
 * Transport for Lua scripts - captures responses instead of writing to socket
 */
export class LuaTransport implements Transport {
  public lastResponse: unknown = null

  write(responseData: unknown, close?: boolean): void {
    this.lastResponse = responseData
    // Don't write to socket - just capture response
  }

  getResponse(): unknown {
    return this.lastResponse
  }

  reset(): void {
    this.lastResponse = null
  }
}
```

## Implementation

### 3.1 Update Execution Context

**File:** [src/commanders/custom/execution-context.ts](src/commanders/custom/execution-context.ts)

```typescript
export interface LockContext {
  lockHeld: boolean
}

export interface ExecutionContext {
  execute(
    transport: Transport,
    cmdName: string,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
    lockContext?: LockContext  // NEW parameter
  ): Promise<ExecutionContext>
}

export class CommandExecutionContext implements ExecutionContext {
  constructor(
    private readonly commands: Record<string, Command>,
    private readonly db: DB,
    private readonly validator?: Validator<Buffer>,
  ) {}

  async execute(
    transport: Transport,
    cmdName: string,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
    lockContext?: LockContext,  // NEW parameter
  ): Promise<ExecutionContext> {
    const cmd = this.commands[cmdName]
    if (!cmd) {
      throw new UnknownCommand(cmdName, args)
    }

    // Handle MULTI transition
    if (cmdName === 'multi') {
      return new TransactionExecutionContext(
        this.commands,
        this.db,
        this.validator,
      )
    }

    // Check if lock is already held (e.g., from Lua script)
    const lockAlreadyHeld = lockContext?.lockHeld ?? false

    if (lockAlreadyHeld) {
      // Lock already held - execute without re-acquiring
      const res = await cmd.run(rawCmd, args, signal)
      transport.write(res.response, res.close)
      return this
    }

    // Normal execution - acquire lock
    const release = await this.db.lock.acquire()
    try {
      // Validate if needed
      if (this.validator) {
        const keys = cmd.getKeys(rawCmd, args)
        await this.validator.validate(keys)
      }

      const res = await cmd.run(rawCmd, args, signal)
      transport.write(res.response, res.close)
      return this
    } finally {
      release()
    }
  }
}
```

### 3.2 Update Transaction Context

**File:** [src/commanders/custom/transaction-execution-context.ts](src/commanders/custom/transaction-execution-context.ts)

```typescript
import type { LockContext } from './execution-context'

export class TransactionExecutionContext implements ExecutionContext {
  // ... existing fields

  async execute(
    transport: Transport,
    cmdName: string,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
    lockContext?: LockContext,  // NEW parameter (ignored in transaction mode)
  ): Promise<ExecutionContext> {
    // ... existing implementation
    // Transaction context manages its own locking during EXEC
  }
}
```

### 3.3 Create Lua Transport

**File:** [src/commanders/custom/lua-transport.ts](src/commanders/custom/lua-transport.ts) (NEW)

```typescript
import type { Transport } from '../../types'

/**
 * Transport for capturing command responses during Lua script execution
 * instead of writing to socket
 */
export class LuaTransport implements Transport {
  private lastResponse: unknown = null

  write(responseData: unknown, close?: boolean): void {
    // Capture response for Lua script
    this.lastResponse = responseData

    // Ignore close flag - Lua scripts don't close connections
  }

  /**
   * Get the captured response
   */
  getResponse(): unknown {
    return this.lastResponse
  }

  /**
   * Reset for next command
   */
  reset(): void {
    this.lastResponse = null
  }
}
```

### 3.4 Fix EVAL Command

**File:** [src/commanders/custom/commands/redis/eval.ts](src/commanders/custom/commands/redis/eval.ts)

```typescript
import { LuaTransport } from '../../lua-transport'
import type { LockContext } from '../../execution-context'

export class Eval implements Command {
  constructor(
    private readonly db: DB,
    private readonly lua: LuaEngine,
    private readonly commands: Record<string, Command>,
    private readonly executionContext: ExecutionContext,  // Need context reference
  ) {}

  async run(rawCmd: Buffer, args: Buffer[], signal: AbortSignal): Promise<CommandResult> {
    const script = args[0].toString()
    const numKeys = parseInt(args[1].toString(), 10)

    if (isNaN(numKeys) || numKeys < 0) {
      throw new Error('Number of keys must be a non-negative integer')
    }

    const keys = args.slice(2, 2 + numKeys).map(k => k.toString())
    const argv = args.slice(2 + numKeys).map(a => a.toString())

    // CRITICAL: Acquire lock ONCE for entire script
    const release = await this.db.lock.acquire()

    try {
      // Create special transport for Lua
      const luaTransport = new LuaTransport()

      // Set up redis.call to execute without re-acquiring lock
      this.lua.global.set('redisCall', async (cmdName: string, luaArgs: string[]) => {
        const cmd = this.commands[cmdName.toLowerCase()]
        if (!cmd) {
          throw new Error(`Unknown Redis command called from Lua: ${cmdName}`)
        }

        const argsBuffer = luaArgs.map(arg => Buffer.from(arg))

        // Reset transport for this command
        luaTransport.reset()

        // CRITICAL: Pass lockContext to indicate lock is already held
        const lockContext: LockContext = { lockHeld: true }

        await this.executionContext.execute(
          luaTransport,
          cmdName.toLowerCase(),
          Buffer.from(cmdName),
          argsBuffer,
          signal,
          lockContext,  // Lock already held!
        )

        // Get response from transport
        const response = luaTransport.getResponse()

        // Convert Redis response to Lua format
        return this.convertResponseToLua(response)
      })

      // Execute script
      const luaScript = `
        local KEYS = {${keys.map(k => `"${k}"`).join(', ')}}
        local ARGV = {${argv.map(a => `"${a}"`).join(', ')}}
        return (${script})(KEYS, ARGV)
      `

      const result = await this.lua.doString(luaScript)

      // Convert Lua result to Redis response
      return { response: this.convertLuaToResponse(result) }
    } finally {
      // RELEASE lock ONCE after script completes
      release()
    }
  }

  private convertResponseToLua(response: unknown): any {
    // Convert Redis RESP response to Lua format
    if (response === null) {
      return null
    } else if (Buffer.isBuffer(response)) {
      return response.toString()
    } else if (typeof response === 'number') {
      return response
    } else if (typeof response === 'string') {
      return response
    } else if (Array.isArray(response)) {
      return response.map(r => this.convertResponseToLua(r))
    } else if (response instanceof Error) {
      throw response
    }
    return response
  }

  private convertLuaToResponse(result: any): unknown {
    // Convert Lua result to Redis RESP format
    if (result === null || result === undefined) {
      return null
    } else if (typeof result === 'number') {
      return result
    } else if (typeof result === 'string') {
      return Buffer.from(result)
    } else if (typeof result === 'boolean') {
      return result ? 1 : 0
    } else if (Array.isArray(result)) {
      return result.map(r => this.convertLuaToResponse(r))
    } else if (typeof result === 'object') {
      // Convert Lua table to array
      const arr: unknown[] = []
      for (const key in result) {
        arr.push(this.convertLuaToResponse(result[key]))
      }
      return arr
    }
    return result
  }
}
```

### 3.5 Fix EVALSHA Command

**File:** [src/commanders/custom/commands/redis/evalsha.ts](src/commanders/custom/commands/redis/evalsha.ts)

Same changes as EVAL - acquire lock once, pass lock context to nested commands.

```typescript
async run(rawCmd: Buffer, args: Buffer[], signal: AbortSignal): Promise<CommandResult> {
  const sha = args[0].toString()
  const script = this.db.scriptsStore.get(sha)

  if (!script) {
    throw new Error(`NOSCRIPT No matching script. Please use EVAL.`)
  }

  // Replace first arg with script content
  const evalArgs = [script, ...args.slice(1)]

  // Delegate to EVAL logic with lock context
  const evalCmd = this.commands['eval']
  return evalCmd.run(rawCmd, evalArgs, signal)
}
```

### 3.6 Update Command Factories

**File:** [src/commanders/custom/commands/redis/index.ts](src/commanders/custom/commands/redis/index.ts)

EVAL and EVALSHA need reference to execution context:

```typescript
export function createCommands(
  luaEngine: LuaEngine,
  db: DB,
  executionContext?: ExecutionContext  // NEW parameter
): Record<string, Command> {
  const commands: Record<string, Command> = {
    // ... other commands
  }

  // EVAL and EVALSHA need execution context for nested commands
  if (executionContext) {
    commands.eval = new Eval(db, luaEngine, commands, executionContext)
    commands.evalsha = new Evalsha(db, luaEngine, commands, executionContext)
  }

  return commands
}
```

### 3.7 Update Commander

**File:** [src/commanders/custom/commander.ts](src/commanders/custom/commander.ts)

Pass execution context when creating commands:

```typescript
export class Commander implements DBCommandExecutor {
  private context: ExecutionContext

  constructor(private readonly db: DB, luaEngine: LuaEngine) {
    // Create initial execution context
    this.context = new CommandExecutionContext(
      {},  // Placeholder, will be updated
      db,
    )

    // Create commands with reference to context
    const commands = createCommands(luaEngine, db, this.context)

    // Update context with actual commands
    this.context = new CommandExecutionContext(commands, db)
  }

  async execute(
    transport: Transport,
    cmdName: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    this.context = await this.context.execute(
      transport,
      cmdName.toString().toLowerCase(),
      cmdName,
      args,
      signal,
    )
  }
}
```

## Testing

### Unit Tests

Create `tests/lua-atomicity.test.ts`:

```typescript
import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../src/commanders/custom/db'
import { Commander } from '../src/commanders/custom/commander'
import { LuaEngine } from '../src/lua-engine'

describe('Lua Script Atomicity', () => {
  test('script holds lock for entire execution', async () => {
    const db = new DB()
    const lua = new LuaEngine()
    const commander = new Commander(db, lua)

    // Set initial value
    db.set(Buffer.from('mykey'), Buffer.from('10'))

    // Track lock acquisitions
    const lockAcquisitions: number[] = []
    const originalAcquire = db.lock.acquire.bind(db.lock)
    db.lock.acquire = async () => {
      lockAcquisitions.push(Date.now())
      return originalAcquire()
    }

    // Execute Lua script that calls redis.call twice
    const script = `
      local val = redis.call('GET', KEYS[1])
      redis.call('SET', KEYS[1], tonumber(val) + 1)
      return redis.call('GET', KEYS[1])
    `

    await commander.execute(
      mockTransport,
      Buffer.from('EVAL'),
      [Buffer.from(script), Buffer.from('1'), Buffer.from('mykey')],
      new AbortController().signal,
    )

    // Should only acquire lock ONCE for entire script
    assert.strictEqual(lockAcquisitions.length, 1)
  })

  test('concurrent client cannot interfere with Lua script', async (t) => {
    const db = new DB()
    const lua = new LuaEngine()
    const commander1 = new Commander(db, lua)
    const commander2 = new Commander(db, lua)

    db.set(Buffer.from('counter'), Buffer.from('0'))

    const script = `
      local val = redis.call('GET', KEYS[1])
      -- Simulate slow operation
      for i = 1, 1000000 do end
      redis.call('SET', KEYS[1], tonumber(val) + 1)
      return redis.call('GET', KEYS[1])
    `

    // Start Lua script on commander1
    const luaPromise = commander1.execute(
      mockTransport,
      Buffer.from('EVAL'),
      [Buffer.from(script), Buffer.from('1'), Buffer.from('counter')],
      new AbortController().signal,
    )

    // Try to execute SET on commander2 (should wait for Lua to finish)
    const setPromise = commander2.execute(
      mockTransport,
      Buffer.from('SET'),
      [Buffer.from('counter'), Buffer.from('999')],
      new AbortController().signal,
    )

    await Promise.all([luaPromise, setPromise])

    const final = db.get(Buffer.from('counter'))
    // Final value should be 999 (SET happened after Lua)
    // If race condition exists, result would be 1 (Lua overwrote SET)
    assert.strictEqual(final?.toString(), '999')
  })
})
```

### Integration Tests

Add to `tests-integration/ioredis/lua.test.ts`:

```typescript
test('Lua scripts execute atomically', async () => {
  const script = `
    local val = redis.call('GET', KEYS[1])
    redis.call('SET', KEYS[1], tonumber(val) + 1)
    return redis.call('GET', KEYS[1])
  `

  await redisClient.set('counter', '0')

  // Execute Lua script and SET concurrently
  const promises = [
    redisClient.eval(script, 1, 'counter'),
    redisClient.set('counter', '999'),
  ]

  await Promise.all(promises)

  const final = await redisClient.get('counter')
  // Final value should be either 1 or 999 (atomicity preserved)
  // NOT an intermediate value
  assert.ok(final === '1' || final === '999')
})
```

## Files Modified

1. **[src/commanders/custom/execution-context.ts](src/commanders/custom/execution-context.ts)** - Add LockContext parameter
2. **[src/commanders/custom/transaction-execution-context.ts](src/commanders/custom/transaction-execution-context.ts)** - Update signature
3. **[src/commanders/custom/lua-transport.ts](src/commanders/custom/lua-transport.ts)** - NEW FILE
4. **[src/commanders/custom/commands/redis/eval.ts](src/commanders/custom/commands/redis/eval.ts)** - Acquire lock once
5. **[src/commanders/custom/commands/redis/evalsha.ts](src/commanders/custom/commands/redis/evalsha.ts)** - Acquire lock once
6. **[src/commanders/custom/commands/redis/index.ts](src/commanders/custom/commands/redis/index.ts)** - Pass execution context
7. **[src/commanders/custom/commander.ts](src/commanders/custom/commander.ts)** - Create context reference
8. **[src/commanders/custom/clusterCommander.ts](src/commanders/custom/clusterCommander.ts)** - Update for lock context
9. **[tests/lua-atomicity.test.ts](tests/lua-atomicity.test.ts)** - NEW TEST FILE

## Verification Checklist

- [ ] LockContext interface added to execution-context.ts
- [ ] LuaTransport class created
- [ ] EVAL acquires lock once for entire script
- [ ] EVALSHA acquires lock once for entire script
- [ ] Nested redis.call() doesn't re-acquire lock
- [ ] Unit test: Single lock acquisition per script
- [ ] Unit test: Concurrent client blocked during Lua
- [ ] Integration test: Atomicity preserved under load
- [ ] Existing Lua tests still pass
- [ ] No performance regression

## Benefits

1. **Correct atomicity** - Matches Redis behavior exactly
2. **No race conditions** - Other clients blocked during script execution
3. **Consistent state** - Scripts see consistent data throughout execution
4. **Predictable behavior** - Scripts behave identically to real Redis
5. **Test compatibility** - Existing Redis tests now pass

## Next Steps

After Phase 3 is complete:
- **Phase 4**: Implement COMMAND command using metadata registry
- **Phase 5**: Improve transport layer with command queuing
