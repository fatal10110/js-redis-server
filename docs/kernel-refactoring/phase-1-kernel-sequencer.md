# Phase 1: The Kernel & Sequencer

## Objective

Replace the current direct-execution model with a queued execution model. This eliminates the need for locks (Mutex) inside commands and guarantees that Lua scripts or multi-step operations cannot be interleaved by other clients.

## The Problem

Currently, `await` inside a command (e.g., in `EVAL` or `MULTI`) yields the event loop, allowing other connections to execute commands. We try to patch this with `Mutex`, but it is error-prone.

## The Solution: Global Sequencer

The `RedisKernel` maintains a FIFO queue of jobs. It pulls one job, executes it to completion, and then pulls the next.

### 1.1 Core Interfaces

```typescript
export interface CommandJob {
  /** Unique ID for tracing */
  id: string
  /** The connection that sent this command */
  connectionId: string
  /** The abstract command request */
  request: CommandRequest
  /** Promise resolvers for the transport layer */
  resolve: (result: any) => void
  reject: (error: Error) => void
}

export interface CommandRequest {
  command: string
  args: unknown[] // Abstract arguments, not necessarily Buffers yet
}
```

### 1.2 The RedisKernel Class

```typescript
export class RedisKernel {
  private queue: CommandJob[] = []
  private isProcessing = false
  private registry: CommandRegistry
  private store: DataStore

  submit(job: CommandJob) {
    this.queue.push(job)
    // Schedule processing on next tick to allow batching/IO
    if (!this.isProcessing) {
      setImmediate(() => this.processLoop())
    }
  }

  private async processLoop() {
    if (this.isProcessing) return
    this.isProcessing = true

    while (this.queue.length > 0) {
      const job = this.queue.shift()
      try {
        const handler = this.registry.getHandler(job.request.command)
        const result = await handler.execute(job.request, this.store)
        job.resolve(result)
      } catch (err) {
        job.reject(err)
      }
    }

    this.isProcessing = false
  }
}
```
