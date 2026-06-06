# Phase 2 / 3 / 4 Slice Review

Scope: new `src/state/`, `src/commands/`, `src/core/client-session.ts`,
`src/core/resp-encoder.ts`, `src/core/turn-queue.ts`,
`src/core/transports/connection-transport.ts`,
`src/core/transports/in-memory-connection-transport.ts`,
`src/core/transports/socket-connection-transport.ts`,
`src/core/transports/resp2/{decoder.ts,session-adapter.ts}`, plus updates to
`command-executor.ts`, `command-schema.ts`, `redis-context.ts`,
`redis-error.ts`, `command-definition.ts`. New tests:
`commands-foundation.test.ts`, `core-resp-encoder.test.ts`,
`core-transport-session.test.ts`, `state-core.test.ts`. Old code untouched.

Tests pass: 30 / 30.

## Phase 1 Issues — Resolved

- #1 `resolveExecutionResult` dead branch removed.
- #2 `executePlan` catches `RedisCommandError` and converts to error result.
- #3 `onStream` policy hook added; before/after symmetric for non-stream path.
- #5 `t.bigInteger()` added.
- #6 `createDefaultParkHandler` honors `timeoutMs` + `signal`;
  `createNoopParkHandler` aliases it.
- #7 concrete context types (`RedisDatabase`, `RedisServerState`,
  `RedisClientSession`).
- #11 `UnknownRedisCommandError` binary-safe (printable check + hex escape +
  truncation).
- #14 RESP2 encoder shipped; RESP3 deterministic-throw guard.
- #15 bridge wired (`ClientSession.execute` → `Resp2SessionAdapter` →
  encoder → transport).
- #16 streaming consumer pattern committed (`frames(signal)`).
- #17 `pubsub` / `subscribed` flags added.
- #20 streaming + park tests present.

## What Shipped (Plan Coverage)

- **Phase 2** state: `RedisServerState`, `RedisDatabase`, `RedisKeyspace`,
  `RedisMutationBus`, `RedisScriptCache`, `RedisPubSubBroker` (stub),
  `RedisClusterTopology` (stub), byte-safe `Buffer` storage, defensive clones,
  lazy eviction emits `evict` event.
- **Phase 3** foundational commands: `PING`, `QUIT`, `SELECT`, `GET`, `SET`
  (NX/XX/EX/PX/EXAT/PXAT/KEEPTTL/GET), `MGET`, `DEL`, `EXISTS`, `TYPE`,
  `DBSIZE`, `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `PERSIST`, `FLUSHDB`,
  `FLUSHALL`.
- **Phase 4 (partial)** `ClientSession` with WATCH/UNWATCH wired to mutation
  events; transaction interface skeleton (no MULTI/EXEC commands yet).
- **Phase 6.5** `SerialTurnQueue` with priority re-acquire; `ctx.park` via
  session-wrapped park handler; cross-session blocking test passes.
- **Phase 6.75** `ConnectionTransport` abstraction; `SocketConnectionTransport`
  + `InMemoryConnectionTransport`; `Resp2SessionAdapter` runs the loop.
- RESP2 decoder added (multibulk + inline).

## Bugs / Architectural Issues

### Critical

#### 1. Turn queue not bound to database — default mode is unsafe

`src/core/client-session.ts:62`

```ts
this.turnQueue = options.turnQueue ?? new SerialTurnQueue()
```

Each `ClientSession` gets a **fresh** queue unless caller passes one. Park
test only works because both sessions explicitly share `turnQueue`. In real
wiring (one factory per connection), every session has its own queue → **no
cross-connection serialization on the shared `RedisDatabase`**. Race by
default.

Plan acceptance criterion violated: "One serialization turn protects one
`RedisDatabase`. Master and replica views of the same database share the
same executor."

Fix: hang `RedisTurnQueue` off `RedisDatabase` (or `RedisServerState`) and
pull from there inside `ClientSession`. Drop the optional constructor param
or keep only as test injection.

#### 2. RESP2 decoder rejects `*0\r\n`

`src/core/transports/resp2/decoder.ts:59-61`

```ts
if (count < 1) {
  throw new Resp2ParseError('Protocol error: invalid multibulk length')
}
```

Real Redis accepts `*0\r\n` as an empty multibulk (no command). The mock
disconnects the client instead. Accept `count === 0` as a no-op frame; throw
only on `count < -1`.

#### 3. RESP2 decoder rejects empty inline lines

`src/core/transports/resp2/decoder.ts:125-127`

Bare `\r\n` from a client (idle heartbeat / telnet enter) throws `Protocol
error: empty inline command` and tears down the connection. Real Redis
ignores empty inline lines. Skip the frame instead.

#### 4. Inline parser doesn't handle quoted strings

`src/core/transports/resp2/decoder.ts:118-123`

`.split(/\s+/)` only. Real Redis inline parser supports `"..."` and `\xNN`
escapes — required for `redis-cli` telnet usage with values containing
spaces. Mock will silently misparse. Document as limitation if not fixing.

#### 5. Encoder mangles `Infinity` / `-Infinity` / `NaN`

`src/core/resp-encoder.ts:88-94`

`formatNumber` only special-cases `-0`. `Infinity.toString() === 'Infinity'`;
real Redis writes `inf` / `-inf` / `nan` for floats. ZSCORE / ZADD with
infinity scores will fail client parsing.

#### 6. Error encoder doesn't sanitize `\r\n`

`src/core/resp-encoder.ts:61`

```ts
return Buffer.from(`-${formatError(value)}\r\n`)
```

Error message containing `\r\n` corrupts the protocol stream — client
receives a half-frame plus garbage. Replace `\r` / `\n` with space (or
reject at construction).

### High

#### 7. SET command bypasses schema combinators

`src/commands/strings.ts:102-172`

Hand-rolled imperative parser. `t.object` cannot express the NX|XX /
EX|PX|EXAT|PXAT / KEEPTTL / GET option grammar. Pattern will repeat for
every complex Redis command (ZADD, GEORADIUS, BITCOUNT, etc.). Either:

- Extend schema combinator with `t.flag('NX')`,
  `t.taggedValue('EX', t.integer({min:1}))`, `t.oneOf([...])` for
  mutually-exclusive groups, or
- Accept a parser-callback pattern as first-class and document.

Currently a foot in two camps. Foundational command set hides the cost.

#### 8. ResponseStream + `await` fragility

`src/core/command-executor.ts:68`

```ts
const result = await plan.definition.execute(plan.args, ctx)
```

Works because `ResponseStream` is not thenable. If anyone adds a `then`
property to `ResponseStream` (or a similar push type), `await` consumes it.
Guard:

```ts
const raw = plan.definition.execute(plan.args, ctx)
if (isResponseStream(raw)) {...}
const result = await raw
```

#### 9. MULTI / EXEC not wired

`src/core/client-session.ts:100-133`

`ClientSession` exposes `beginTransaction` / `queueTransaction` /
`drainTransaction` / `discardTransaction` but:

- No `MULTI` / `EXEC` / `DISCARD` commands exist.
- `CommandExecutor` has no `executeBatch(plans, ctx)`.
- No `TransactionPolicy` runs queue-time validation.

Plan Phase 4 — partial. Either complete in this slice or document the gap
explicitly.

#### 10. Cluster + Lua routing absent

Plan Phase 5 / Phase 6. Acceptance criteria require cluster routing on
normal AND transaction AND `redis.call` invocations. None implemented yet.
Document the gap.

### Medium

#### 11. `ClientSession.signal` field declared after methods

`src/core/client-session.ts:74`

```ts
constructor(...) {
  ...
  this.signal = options.signal // before declaration line
}
...
readonly signal: AbortSignal
```

Works under current `tsconfig`. With `useDefineForClassFields: true` the
declaration would clobber the constructor assignment with `undefined`. Move
the declaration into the class header alongside other fields and use `!` or
initialize-then-assign.

#### 12. Backpressure not honored by `SocketConnectionTransport.write`

`src/core/transports/socket-connection-transport.ts:45-60`

`socket.write(chunk, cb)` ignores the boolean return / `writableNeedDrain`.
Adapter writes can outpace the kernel send buffer on a slow consumer. Phase
6.9 (pubsub fan-out) will surface this. Note in plan.

#### 13. `InMemoryConnectionTransport` event semantics misleading

`src/core/transports/in-memory-connection-transport.ts:71-78`

`emit('drain')` fires after EVERY write — drain event in TCP means "buffer
was full and is now drained". Tests passing here will mask code that
depends on a real drain event. Either omit drain or implement a real
backpressure mode.

#### 14. Test-only API leaks into runtime type

`InMemoryConnectionTransport.feed/endRead/getWritten/clearWritten/getWrittenBuffer`
are public on the runtime class. Easy to call accidentally from production
code. Split into a test subclass or namespace as `__test` prefix.

#### 15. `commands/helpers.ts` re-implements integer parsing

`src/commands/helpers.ts:59-71` duplicates `t.integer()` logic. SET uses it
because it hand-rolls. Once #7 is addressed this duplication goes away.

#### 16. `expireKey` parses duration but schema already produced number

`src/commands/keys.ts:186-188`

```ts
if (!Number.isSafeInteger(duration)) throw new ExpectedIntegerError()
```

`t.integer()` already enforces safe-integer. Redundant guard. Remove.

#### 17. `RedisKeyspace.size()` iterates ALL entries and evicts during iteration

`src/state/keyspace.ts:164-174`

Uses `Array.from(entries.values())` (snapshot first — good) and calls
`evictIfExpired` which mutates the map. Snapshot makes it safe. But O(n)
on every `DBSIZE`. Mock acceptable. Worth a TODO.

#### 18. `RedisMutationBus.emit` iterates live listener sets

`src/state/mutation-events.ts:71-91`

If a listener mutates `globalListeners` or `keyListeners.get(id)` during
iteration, behavior is iterator-order-dependent. Snapshot via
`Array.from(...)` before for-of.

#### 19. `RedisPubSubBroker` stub is callable

`src/state/pubsub-broker.ts` returns 0. PUBLISH command (when added) will
silently no-op. Either `throw new Error('not implemented')` or expose
feature flag so consumers don't get false success.

#### 20. `RedisClusterTopology` stub has no lookup methods

`src/state/cluster-topology.ts` carries `nodes` only. No `getNodeBySlot`,
`getMaster`, `getReplicas`. ClusterPolicy in Phase 5 will need them.

### Low

#### 21. Encoder allocates per encode call

`src/core/resp-encoder.ts:65-69, 77-81` — small `Buffer.concat` calls per
frame. Fine for mock; flag for batching later.

#### 22. `defineCommand` double-lowercases name

Phase 1 #12 still present. `defineCommand` lowercases; registry lowercases
again. Pick one site.

#### 23. `ClientSession.close()` doesn't tear down adapter listeners

Adapter holds session reference; session abort fires its signal. Adapter's
`run` loop checks `signal.aborted` after each frame. Cleanup is implicit.
Document.

#### 24. `commands/index.ts` lists command families manually

Each family exports an array; index concatenates. Adding hashes/lists means
editing this file. Once registry is open via `register`, command families
could self-register. Minor.

#### 25. `RedisServerState.flushAllDatabases` does not flush script cache

Matches real Redis (`FLUSHALL` keeps scripts; `SCRIPT FLUSH` is separate).
Behavior correct — add a doc comment because the natural reading suggests
"flush all state".

## Verdict

Strong slice. Phase 1 review items mostly closed. Phase 2 + 3 + 6.5 + 6.75
effectively done; Phase 4 half-done (WATCH wired, MULTI/EXEC missing).

**Must fix before Phase 4 completion / Phase 5:**

- **#1** turn queue ownership — silent races by default is the worst kind of
  bug.
- **#2, #3, #4** decoder protocol mismatches — real clients hit these on day
  one.
- **#5, #6** encoder correctness — same.

**Cheap and high-value:**

- **#7** decide schema strategy now — every command from here pays the cost.
- **#8** guard against ResponseStream-thenable foot-gun.
- **#11** move `signal` declaration to header.
- **#18** snapshot listener sets in mutation bus.

**Plan-tracked deferrals:** #9 (MULTI/EXEC), #10 (cluster/Lua), #19, #20
(pubsub/topology stubs). Acceptable but call out explicitly in plan progress
notes.

**Architectural standout:** `ctx.park` end-to-end works with a real
two-client test — that is the load-bearing piece for everything blocking.
Keep it.
