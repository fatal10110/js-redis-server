# Phase 4: Transport Adapters

## Objective

Implement the Transport Layer as a set of adapters that translate wire protocols into `CommandJobs`.

## The Architecture

```
[ Client ] -> [ TCP Socket ] -> [ RESP Adapter ] -> [ Kernel ]
```

### 4.1 The Transport Interface

The Transport layer is responsible for:

1.  Managing Connection Lifecycle (Connect/Disconnect).
2.  Parsing incoming bytes.
3.  Formatting outgoing results.

It does **not** execute commands.

### 4.2 RESP Adapter Implementation

This adapter uses the existing `respjs` parser but changes the output target.

```typescript
class RespAdapter {
  constructor(
    private kernel: RedisKernel,
    socket: Socket,
  ) {
    const parser = new Resp()

    parser.on('data', (args: Buffer[]) => {
      // 1. Create Job
      const job: CommandJob = {
        request: { command: args[0].toString(), args: args.slice(1) },
        resolve: res => this.sendResp(res),
        reject: err => this.sendError(err),
      }

      // 2. Submit to Kernel
      this.kernel.submit(job)
    })
  }
}
```

This phase effectively makes the server "Headless". We could easily add a WebSocket adapter for a browser-based Redis playground.
