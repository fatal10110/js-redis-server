# Phase 5: Transactions & Atomicity

## Objective

Implement `MULTI`, `EXEC`, `DISCARD`, and `WATCH` using the Kernel's sequential execution model. This replaces the complex `Mutex` locking mechanism with a design that guarantees atomicity by virtue of the Kernel's single-threaded nature.

## The Problem

In the current architecture, `EXEC` acquires a lock and executes commands. This is prone to race conditions if `await` is used improperly. Additionally, the transport layer must ensure that no "unrelated" commands slip through during a transaction.

## The Solution: Session State Pattern

We move the transaction state management to the **Transport/Adapter** layer (specifically, a `Session` object). The Kernel remains a dumb executor of jobs.

### 5.1 The Session State Machine

Each connection has a `Session` that can be in one of two states: `Normal` or `Transaction`.

```typescript
interface SessionState {
  handle(command: string, args: unknown[]): Promise<any>
}

class NormalState implements SessionState {
  constructor(private session: Session) {}

  async handle(command: string, args: unknown[]) {
    if (command === 'MULTI') {
      this.session.setState(new TransactionState(this.session))
      return 'OK'
    }
    // Pass through to Kernel immediately
    return this.session.kernel.execute({ command, args })
  }
}

class TransactionState implements SessionState {
  private buffer: CommandRequest[] = []

  constructor(private session: Session) {}

  async handle(command: string, args: unknown[]) {
    if (command === 'EXEC') {
      // Submit the entire buffer as a SINGLE atomic job
      const result = await this.session.kernel.executeTransaction(this.buffer)
      this.session.setState(new NormalState(this.session))
      return result
    }

    if (command === 'DISCARD') {
      this.session.setState(new NormalState(this.session))
      return 'OK'
    }

    if (command === 'MULTI') {
      throw new Error('ERR MULTI calls can not be nested')
    }

    // 1. Validate Command (Arity, Schema)
    const schema = this.session.registry.get(command)
    if (!schema) throw new Error('ERR unknown command')

    // 2. "Remember" (Buffer)
    this.buffer.push({ command, args })

    // 3. Do NOT execute
    return 'QUEUED'
  }
}
```

### 5.2 Atomic Execution in Kernel

The `RedisKernel` needs a method to execute a batch of commands without yielding to other jobs.

```typescript
class RedisKernel {
  // ... existing code ...

  async executeTransaction(commands: CommandRequest[]): Promise<any[]> {
    // This runs inside the single-threaded processLoop
    // No other jobs can interleave here because we don't yield back to the queue
    const results = []
    for (const req of commands) {
      const handler = this.registry.getHandler(req.command)
      results.push(await handler.execute(req, this.store))
    }
    return results
  }
}
```

### 5.3 Handling WATCH

`WATCH` is handled by the `NormalState`. It registers the session as an observer on specific keys in the `ReactiveStore`.

If a watched key is modified (event emitted), the Session is marked as `dirty`.

When `TransactionState` handles `EXEC`:

1. Checks if Session is `dirty`.
2. If dirty, returns `(nil)` (transaction aborted).
3. If clean, proceeds with `kernel.executeTransaction`.
