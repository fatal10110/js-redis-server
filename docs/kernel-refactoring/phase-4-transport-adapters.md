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
  private session: Session

  constructor(
    private kernel: RedisKernel,
    socket: Socket,
  ) {
    this.session = new Session(kernel) // Starts in NormalState
    const parser = new Resp()

    parser.on('data', async (args: Buffer[]) => {
      const command = args[0].toString()
      const cmdArgs = args.slice(1)

      try {
        // Delegate to Session (handles MULTI/EXEC buffering internally)
        const result = await this.session.handle(command, cmdArgs)
        this.sendResp(result)
      } catch (err) {
        this.sendError(err)
      }
    })
  }
}
```

This phase effectively makes the server "Headless". We could easily add a WebSocket adapter for a browser-based Redis playground.
