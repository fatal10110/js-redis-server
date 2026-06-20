# Architecture Refactor Plan

> **Status (2026-06-20): the hard cutover is complete.** All five migration
> phases below have landed — including Phase 4's Pub/Sub, blocking, and
> streaming commands, which were the last to arrive. This document is retained
> as the design record for the new core; current per-command coverage and the
> remaining compatibility gaps are tracked in
> [COMMANDS.md](./COMMANDS.md) and the acceptance checklist in
> [ARCHITECTURE-REVIEW.md](./ARCHITECTURE-REVIEW.md). The runtime architecture
> itself is documented in [ARCHITECTURE.md](./ARCHITECTURE.md).

## Purpose

This project is primarily a Redis-compatible mock server, not a high-performance
database. The refactor should optimize for extensibility, correctness of Redis
semantics that clients depend on, and low friction when adding commands.

The current design makes commands responsible for too many concerns: parsing,
key extraction, execution, transport writes, transaction behavior, Lua behavior,
and cluster behavior. The target design is a small Redis interpreter core with
RESP, cluster, Lua, and transactions implemented as adapters or execution
policies around it.

## Design Goals

- Adding a normal Redis command should only require a command definition,
  registration, and focused tests.
- Commands should return Redis results, not write to sockets.
- Parsing, key extraction, arity validation, and command metadata should have a
  single source of truth.
- Standalone, cluster, transaction, and Lua execution should use the same
  executor pipeline.
- Every key mutation should pass through one observable database API so WATCH,
  blocking commands, debugging hooks, and future key notifications stay correct.
- Redis keys, values, fields, and members should remain byte-safe Buffers unless
  a command explicitly parses them as numbers or keywords.
- The command contract must support async execution, blocking/parking, and
  streaming/push responses as first-class concerns, not retrofits. Half of
  Redis (BLPOP, XREAD BLOCK, WAIT, MONITOR, SUBSCRIBE, keyspace push, client
  tracking) cannot be modeled otherwise.
- The result/value model must be RESP3-ready: Map, Set, Double, Bool,
  BigNumber, VerbatimString, and Push must be representable without command
  rewrites later.
- The transport layer must be pluggable. A `DBCommandExecutor` should accept
  any framed duplex source, not a `net.Socket`. In-process, programmatic
  test, Unix socket, and future RESP3 / TLS / WebSocket transports must fit
  the same interface.
- Command registration must be open. External callers (tests, mocks, plugins)
  should be able to add or override commands without editing the core
  registry switchboard.
- Cross-cutting behavior (logging, ACL, slow-log, telemetry, audit) belongs in
  composable policies, not in command bodies.
- Performance may be traded for simpler and more reliable behavior.

## Target Architecture

```text
RESP parser
  -> ClientSession
  -> CommandExecutor
      -> CommandRegistry lookup
      -> Schema parse
      -> Key extraction
      -> Execution policies
      -> Command implementation
  -> RedisResult
  -> RESP writer
```

Execution policies should be explicit and composable:

```text
SyntaxPolicy
ClusterPolicy
ReadonlyPolicy
LuaPolicy
TransactionPolicy
WatchPolicy
ObservabilityPolicy   // logging, slow-log, telemetry, audit
AclPolicy             // future
```

Standalone execution uses the base policies. Cluster execution adds slot
validation and MOVED/CROSSSLOT behavior. Lua execution adds script command
restrictions. Transactions queue parsed command plans and later execute them
through the same command executor.

Policies are the only place cross-cutting behavior lives. Commands stay
focused on Redis semantics; observability, authorization, and protocol
constraints are layered around them.

## Proposed Module Layout

```text
src/core/redis-value.ts
src/core/redis-result.ts
src/core/redis-context.ts
src/core/command-definition.ts
src/core/command-executor.ts
src/core/execution-policies/
src/state/server-state.ts
src/state/database.ts
src/state/keyspace.ts
src/state/script-cache.ts
src/commands/
```

The exact paths can change during implementation, but the boundaries should stay
clear:

- `core/` owns command execution concepts.
- `state/` owns Redis server and database state.
- `commands/` owns Redis command definitions.
- `core/transports/` owns RESP/network concerns.

## New Command Contract

Commands should become plain definitions:

```ts
export const getCommand = defineCommand({
  name: 'get',
  schema: t.tuple({
    key: t.key(),
  }),
  flags: ['readonly', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    return ctx.db.getString(args.key)
  },
})
```

The command should not know whether it is running under RESP, Lua, cluster, or
MULTI/EXEC unless the Redis command itself has special behavior.

`execute` may return `RedisResult` synchronously, `Promise<RedisResult>`, or a
`ResponseStream` for push/streaming commands (see below). All three paths flow
through the same executor and result encoder. Most commands stay sync.

Commands declare optional capabilities for the framework to interpret:

```ts
{
  blocking: true,      // may await db.wait(...) or ctx.park(...)
  pushOnly: true,      // pubsub-style; uses ResponseStream
  movableKeys: true,   // override default positional key extraction
  scriptKeys: true,    // routing must inspect runtime args (EVAL)
}
```

This keeps positional-key commands declarative while making escape hatches
explicit instead of accidental.

### Command Registration

Registration is open. The registry exposes `register(definition)` and
`registerAll(definitions)` so external packages, tests, and plugins can add
or override commands without editing the core switchboard. Built-in command
families register themselves on import; the central `index.ts` aggregates by
re-export, not by manually listing every constructor.

## Result And Value Model

`RedisValue` is a discriminated union covering all RESP2 and RESP3 reply
shapes:

```text
RedisValue =
  | { kind: 'simple-string', value: string }
  | { kind: 'bulk-string', value: Buffer | null }
  | { kind: 'integer', value: number | bigint }
  | { kind: 'double', value: number }
  | { kind: 'boolean', value: boolean }
  | { kind: 'big-number', value: bigint }
  | { kind: 'verbatim', format: string, value: Buffer }
  | { kind: 'array', items: RedisValue[] }
  | { kind: 'set', items: RedisValue[] }
  | { kind: 'map', entries: [RedisValue, RedisValue][] }
  | { kind: 'push', name: string, items: RedisValue[] }
  | { kind: 'null' }
  | { kind: 'error', message: string, code?: string }
```

`RedisResult` wraps `RedisValue` plus optional connection-control flags
(`close: true` for QUIT, `disconnect: true` for fatal). Commands never write
to a socket; the RESP encoder is the only place that knows wire format. A
RESP2 encoder downgrades RESP3-only shapes deterministically.

`RedisResult` is for single-frame replies. Streaming/push commands use
`ResponseStream` instead (see next section).

## Async, Blocking, And Streaming

Three response shapes, one executor:

1. `RedisResult` — single frame. The default.
2. `Promise<RedisResult>` — async-but-single. Lua, async I/O, scripted
   computations.
3. `ResponseStream` — push channel. The command holds a write-end; the
   transport reads frames as they arrive. Used for MONITOR, SUBSCRIBE,
   PSUBSCRIBE, keyspace notifications, CLIENT TRACKING invalidations, future
   server-side push.

Blocking commands (BLPOP, BRPOP, BRPOPLPUSH, XREAD BLOCK, WAIT, etc.) park
through the executor:

```ts
execute: async (args, ctx) => {
  const value = await ctx.park({
    waitFor: ctx.db.events.list(args.key, 'push'),
    timeoutMs: args.timeout * 1000,
    signal: ctx.signal,
  })
  return value ?? RedisResult.nil()
}
```

`ctx.park` is the single supported parking primitive. It releases the
serialization turn (today's `RedisKernel`), waits on a keyspace event or
timeout, then reacquires the turn before resuming. Commands never touch the
kernel directly. Cancellation flows through `ctx.signal`, which is the
session's signal, propagated everywhere — no fresh `AbortController()`
allocations.

## Transport Abstraction

`DBCommandExecutor.createAdapter` must accept a transport-neutral handle, not
`net.Socket`. Define:

```ts
interface ConnectionTransport {
  readonly id: string
  readonly signal: AbortSignal
  read(): AsyncIterable<Buffer>
  write(chunk: Buffer): void | Promise<void>
  close(reason?: string): void
  on(event: 'close' | 'drain' | 'error', cb: (...) => void): void
}
```

`Resp2Transport` (TCP) wraps a `net.Socket` into a `ConnectionTransport`. A
RESP3 transport, an in-process programmatic transport (for unit tests and
embedded use), and a WebSocket transport all implement the same interface.
The executor never imports `net`.

## Pub/Sub Model

A `PubSubBroker` lives in `RedisServerState`. It owns subscriptions per
channel and pattern, supports keyspace/keyevent notifications, and exposes:

```ts
broker.subscribe(client, channel) -> ResponseStream
broker.psubscribe(client, pattern) -> ResponseStream
broker.publish(channel, message) -> number
broker.notify(event) -> void   // for keyspace notifications
```

SUBSCRIBE/PSUBSCRIBE return a `ResponseStream`. The client session tracks
active streams so DISCONNECT/RESET/UNSUBSCRIBE can close them. The session
also enters a restricted "subscribed" state where only pubsub commands are
accepted, modeled as another `SessionState`, not as a flag scattered through
the executor.

## State Model

Split server-wide state from database state:

```text
RedisServerState
  scriptCache
  pubsubBroker
  clusterTopology       // structured data, not parsed node-id strings
  databases
  clients

RedisDatabase
  keyspace
  expirations
  notifications
```

`clusterTopology` exposes master/replica relationships and slot ownership as
data. Replica identity is a property of a node, not encoded in its id
string. `DiscoveryService` returns structured topology; routing and replica
behavior derive from it.

Commands should mutate data through central APIs:

```ts
db.updateList(key, list => list.lpush(value))
db.updateHash(key, hash => hash.hset(field, value))
db.updateSet(key, set => set.sadd(member))
db.delete(key)
db.expire(key, timestamp)
```

These APIs should emit mutation events consistently. Commands should not mutate
stored data structures in place without going through the database/keyspace
layer.

## Execution Pipeline

The new executor should produce a command plan before running command behavior:

```ts
type CommandPlan<TArgs = unknown> = {
  definition: CommandDefinition<TArgs>
  args: TArgs
  keys: Buffer[]
  flags: CommandFlags
}
```

Pipeline:

1. Normalize command name.
2. Look up command definition.
3. Parse raw args using the command schema.
4. Extract keys from parsed args.
5. Run policy checks.
6. Acquire a serialization turn (executor-owned, not exposed to commands).
7. Execute the command against `RedisExecutionContext`.
8. Resolve the result (`RedisResult`, `Promise<RedisResult>`, or
   `ResponseStream`).
9. Release the turn; pass the result to the transport.

Transport code writes the returned result. Command code does not write to the
transport directly.

The serialization turn lives on the executor, which is bound to a
`RedisDatabase`. Two commanders sharing the same database share the same
executor, so master and replica access cannot race. The kernel's existing
`suspend`/priority-reacquire flow becomes `ctx.park`.

## Transaction Model

Inside MULTI, queue `CommandPlan` objects instead of raw buffers.

Queue-time behavior:

- Parse and validate arity/syntax.
- Validate command is allowed in transactions.
- Validate cluster slot constraints when running in cluster mode.
- Return `QUEUED` for valid queued commands.
- Mark the transaction dirty for queue-time errors that should make EXEC fail.

EXEC behavior:

- If watched keys changed, return a null transaction result.
- If queue-time errors occurred, return EXECABORT.
- Otherwise execute queued plans through the same executor.
- Runtime command errors should become elements in the EXEC result array.
- Infrastructure failures may still escape as server errors.

## Cluster Model

Cluster should be implemented as an execution policy, not as special state logic.

Cluster policy responsibilities:

- Use parsed keys from the command plan.
- Reject cross-slot multi-key commands.
- Validate the local node owns the slot.
- Return MOVED with the correct owner when needed.
- Pin transaction slots for MULTI/EXEC.

Normal commands and transaction commands must go through the same cluster policy.

## Lua Model

Lua should call the same executor through a Lua-specific adapter:

- Parse Redis calls from Lua into raw command requests.
- Apply Lua command restrictions through `LuaPolicy`.
- Execute command plans against the same state.
- Convert Redis results into Lua reply values.
- Run cluster slot validation on every Redis call inside a script using the
  same `ClusterPolicy` as RESP calls. The current architecture only validates
  the outer EVAL `KEYS` argument; inner `redis.call(...)` invocations bypass
  routing.

Lua should not use transport capture as a command result mechanism. Lua
script identity (sha) lives on `RedisServerState.scriptCache`, not on the
database. `FLUSHDB` must not clear scripts; `SCRIPT FLUSH` must.

## RESP / Session Model

`ClientSession` should own per-client state:

- selected database
- transaction state (as a `SessionState` instance, State pattern preserved)
- watched keys
- client name
- subscribed channels and patterns
- active response streams
- protocol version (RESP2 vs RESP3)
- readonly flag, if needed

RESP transport should own only protocol parsing and writing:

```text
socket data -> RESP parser -> ClientSession.handle() -> RedisResult -> RESP encode
```

Remove normal command dependence on `CapturingTransport`. If a command needs to
close the connection, that should be represented in `RedisResult`.

`SessionState.handle` returns a discriminated `SessionDirective` instead of
the current `{ executeCommand?, executeBatch? }` bag:

```ts
type SessionDirective =
  | { kind: 'idle' }                                  // already responded
  | { kind: 'execute', plan: CommandPlan }
  | { kind: 'execute-batch', plans: CommandPlan[] }
  | { kind: 'subscribe', stream: ResponseStream }
  | { kind: 'park', wait: ParkRequest, plan: CommandPlan }
  | { kind: 'transition-only' }
```

Adding pubsub-subscribed mode, blocking, client-reply OFF/SKIP, or future
client-tracking states becomes a new variant rather than a new optional
field. Optional `watch`/`unwatch` methods come off the `SessionState`
interface and onto a dedicated `Watcher` collaborator owned by the session.

## Migration Plan

*Update: As decided, the library will undergo a hard cutover rather than an incremental migration alongside the old code. The old legacy architecture will be removed as the new core and commands are ported.*

### Phase 1: New Execution Engine (Core)

1. Add `RedisResult`, `RedisValue`, `CommandDefinition`, `CommandPlan`, and `CommandExecutor`.
2. Add a new schema/parser path that returns typed args.
3. Add foundational execution policy interfaces (`SyntaxPolicy`, `ClusterPolicy`, `TransactionPolicy`, `LuaPolicy`).

### Phase 2: State Management & Database

1. Introduce `RedisServerState` to hold databases, pubsub, script cache, and topology.
2. Refactor `RedisDatabase` (`db.ts`) to use byte-safe central mutation APIs and remove the old `EventEmitter` logic.
3. Add centralized mutation events internally.

### Phase 3: Transport & Session

1. Introduce `ConnectionTransport` abstract interface.
2. Wrap `net.Socket` into a `Resp2Transport` implementation.
3. Implement `ClientSession` to track state (selected DB, transaction state, watches).
4. Update adapter to use the new execution pipeline.

### Phase 4: Porting Commands (Hard Cutover)

Port all commands directly to the new `defineCommand` API and delete the old `SchemaCommand` definitions immediately:
1. Foundation commands (`PING`, `GET`, `SET`, `DEL`, `EXISTS`, etc.)
2. Strings, Hashes, Lists, Sets, Sorted Sets
3. Scripts and Cluster commands
4. PubSub, Blocking, and Streaming commands (using `ResponseStream` and `ctx.park`)

*Landed: foundation, all data-type families, scripts/cluster, the SCAN family,
client handshake, and the full Pub/Sub, blocking (`BLPOP`/`BRPOP`/`BLMOVE`/
`BLMPOP`/`BZPOPMIN`/`BZPOPMAX`/`BZMPOP`/`XREAD BLOCK`), MONITOR, `COMMAND`, and
stream command surfaces. `WAIT` remains unported (a no-op in a single-node mock
anyway). See [COMMANDS.md](./COMMANDS.md) for the authoritative per-command
status.*

### Phase 5: Cleanup

1. Delete transport-writing command implementations.
2. Delete old session state machinery and `capturing-transport.ts`.
3. Delete duplicate command factories and commander classes (`BaseCommander`, `ClusterCommander`).
4. Ensure all unit and integration tests pass with the new pipeline.

## Acceptance Criteria

The refactor is complete when:

- A normal command does not import transport/session/cluster/transaction/Lua
  modules.
- A normal command returns a Redis result instead of writing to a transport.
- Command schema parsing is the source of truth for arity and syntax.
- Key extraction happens from parsed args.
- Cluster validation runs for normal commands and transaction commands.
- Cluster validation also runs for `redis.call` invocations from Lua.
- WATCH sees every key mutation, including lazy eviction.
- Lua command execution uses the same executor as RESP command execution.
- `FLUSHDB` and script cache behavior are separated.
- Adding a normal command requires editing only the command file, command family
  registration, and tests.
- The command registry is open: external code can register additional
  commands without modifying core files.
- `RedisValue` represents every RESP3 reply shape. RESP2 downgrade lives in
  the encoder, not in commands.
- A blocking command (BLPOP) runs end-to-end without commands touching the
  kernel directly.
- `DBCommandExecutor` accepts a `ConnectionTransport` and no longer depends
  on `net.Socket`. An in-process transport runs the same command set.
- SUBSCRIBE/MONITOR work via `ResponseStream` without per-command transport
  capture shims.
- One serialization turn protects one `RedisDatabase`. Master and replica
  views of the same database share the same executor.
- The session's `AbortSignal` is the only cancellation signal in the system;
  no `new AbortController()` is constructed inside per-command state code.

## Non-Goals

- Do not optimize data structures for production Redis-scale performance.
- Do not implement every Redis command during the architecture refactor.
- Do not preserve internal APIs that make extension harder.
- Do not keep command-level transport writes for compatibility.
- Do not ship pubsub/blocking/streaming as bolted-on features after the new
  command contract is frozen. They must be designed into the contract from
  Phase 1 even if implementation lands later.

## Known Open Questions

- Does `ctx.park` need to surface batch-vs-single context so EXEC-time
  blocking commands behave the way real Redis does (which is "do not block,
  resolve immediately as nil")?
- Should `ResponseStream` model backpressure, or is fire-and-forget sufficient
  for a mock?
- Where does CLIENT TRACKING live — inside `PubSubBroker`, or as a separate
  invalidation channel on `RedisServerState`?
- Modules / `FUNCTION` commands: out of scope for this refactor, but the
  registry-is-open requirement should leave room without further redesign.
