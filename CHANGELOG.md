# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The package publishes two entry points and they are treated differently here:

- **`js-redis-server`** — the curated consumer facade (`src/index.ts`). Changes
  here affect everyone.
- **`js-redis-server/core`** — the hand-wiring subpath (`src/internal.ts`):
  command definitions, schema parsing, execution policies, transports, state,
  Lua. It is smaller but still published API, so removals from it are marked
  **BREAKING (`/core`)** below and are not folded into "internal refactor".

The same content ships under the `js-valkey-server` name; the subpath is
`js-valkey-server/core`.

Entries are added when a change lands on `main`, not when a release is cut —
releases are manual (`git tag vX.Y.Z` triggers `.github/workflows/release.yml`),
so the PR body is not a durable home for a breaking-change note.

## [Unreleased]

### Removed

- **BREAKING (`/core`)** `CommandIntrospection.firstKey`, `.lastKey` and
  `.keyStep` were removed, and `.arity` became optional ([#370]). `COMMAND` /
  `COMMAND INFO` now derive all three key positions from the definition: from
  `introspection.keySpecs` when present (folded the way Redis's
  `populateCommandLegacyRangeSpec` does), otherwise from the key positions
  of `schema`. Arity is derived from `schema` too. A definition that set the
  removed fields now fails to type-check. Delete them and let them be derived:

  ```
  introspection: { arity: -2, firstKey: 1, lastKey: -1, keyStep: 1, ... }
    -> introspection: { ... }        // schema: t.variadic(t.key(), { min: 1 })
  ```

  `arity` stays available as an override (a number, or
  `(profile) => number` when it differs by compatibility profile), for a
  schema that cannot express it. Two related things changed on `t` and
  `CommandSchema`, both additive:

  - `CommandSchema` gained an optional `layout` (token counts and key
    offsets), set by every `t` builder. `t.key()` now marks a key position,
    so use it only for key arguments and `t.bulk()` for members, fields and
    values. A hand-written `t.custom(parse)` counts as any number of tokens
    with no keys; declare its layout with `t.custom(layout, parse)` or
    `t.withLayout(schema, layout)` for accurate `COMMAND INFO` output.
  - A schema built by hand as `{ parse }` (no `layout`) keeps working, and so
    does a definition registered through `extraCommands` with one. It is
    reported with arity -1 and no key range.

- **BREAKING (`/core`)** The three hand-rolled in-memory transports were
  replaced by [`stream.duplexPair()`](https://nodejs.org/api/stream.html#streamduplexpairoptions),
  which raises the minimum Node version to **22.6** (`engines.node: ">=22.6"`)
  ([#360]). Four things left `/core`:

  ```
  InMemoryConnectionTransport      -> no replacement; it only ever backed this
                                      repo's own tests. Wrap one end of a
                                      duplexPair in SocketConnectionTransport.
  ConnectionTransport.on(...)      -> no replacement; 'close' | 'drain' | 'error'
                                      had zero subscribers. Use the transport's
                                      `signal`, or listen on your own stream.
  ConnectionTransportEvent         -> removed with it.
  ConnectionTransportListener      -> removed with it.
  ConnectionTransportUnsubscribe   -> removed with it.
  VirtualClientSocket (class)      -> type only; the VALUE export is gone. Get
                                      an instance from createVirtualConnection().
  ```

  That last one is not a type-level break — the runtime binding disappears:

  ```js
  import { VirtualClientSocket } from 'js-redis-server/core' // SyntaxError at
  // load time under ESM, which takes the whole importing module down with it.
  socket instanceof VirtualClientSocket                      // TypeError, not false
  ```

  Under CJS the same `require(...).VirtualClientSocket` is `undefined`, so
  `new` and `instanceof` both throw where they previously worked. Replace an
  `instanceof` check with a duck-type test (or `stream.Duplex`), and construct
  via `createVirtualConnection()`.

  `SocketConnectionTransport` now accepts any `Duplex`, not just a `net.Socket`
  — a widening, so existing callers are unaffected. Behavior is unchanged:
  `createIoredisMock` keeps its backpressure-free semantics (the virtual wire
  is created with an effectively unbounded high-water mark, so an in-process
  server never blocks on a client that has not read yet), and tearing down
  either end (client `destroy()` or server `close()`) still ends the session.

  Virtual-connection teardown now follows TCP, the same on Node 22 and 24:

  - A server-side close (`close()`, `QUIT`, a protocol error) half-closes. The
    session ends immediately, and the client socket receives any unread reply
    bytes, then `'end'`, `'finish'` and `'close'` — as against real Redis —
    whenever it reads them, including a client that was paused at the time and
    resumes later. Previously a client that was not reading at that moment
    lost its buffered reply and never saw `'end'`. A client that never reads
    stays half-open until its owner destroys it, as a real socket would.
  - While half-open, a client write is accepted, as a TCP kernel accepts a
    write to a closed peer, and `end(cb)` calls back; the unread reply and EOF
    are still delivered. That includes a write made synchronously in the
    client's `'end'` handler. Any later write fails its callback with
    `ERR_STREAM_WRITE_AFTER_END`, because the client (`allowHalfOpen: false`)
    has ended its own writable by then; a `net.Socket` reports `EPIPE` there.
    Neither emits `'error'`. Previously the client socket was destroyed
    outright, so writes failed with `ERR_STREAM_DESTROYED`, also without an
    `'error'` event.
  - A client `end()` (as ioredis `disconnect()` sends) makes the server close
    its side too, so the client sees `'finish'`, `'end'`, `'close'`.

  - An error passed to `destroy()` on one end is not carried to the other; the
    far end is torn down cleanly, which is what Node 24's own `duplexPair`
    does.

  For `SocketConnectionTransport` over any `Duplex`, what matters is which side
  ended first. If the client ends first (EOF, or it destroys its end), the
  connection is torn down promptly, like Redis's `freeClient` and like main
  over TCP. That happens even if the server had already begun closing
  (e.g. `CLIENT KILL`). Output that is already queued and can still go out
  gets one turn to flush, so a `SUBSCRIBE a b c` sent together with the EOF
  still gets all three confirmations. Output backed up behind a client that
  has stopped reading is dropped. If the server ends first, the connection
  half-closes as described above. One limit: the client's EOF is only seen
  while the read loop is reading. A bounded stream whose loop is blocked
  writing an ordinary reply to a client that stopped reading stays parked
  until the stream closes. Neither shipped path hits this.

  ioredis and node-redis always read, so none of this changes what they see.

- **BREAKING (`/core`)** `ResponseStream`, `isResponseStream` and
  `ExecutorResult` are removed ([#366]). A command's `execute` now returns
  `RedisResult | Promise<RedisResult>`, and server-initiated frames have one
  channel, the session push queue. The two shipped producers moved onto it:

  ```
  SUBSCRIBE a b c (any (UN)SUBSCRIBE family) -> one RedisResult pre-encoded
                                                with every confirmation frame;
                                                `value` is the first frame,
                                                `options.trailingFrames` the rest
  MONITOR                                    -> +OK, then feed lines as session
                                                pushes (ClientSession.startMonitor)
  ExecutorResult                             -> RedisResult
  ```

  A custom command that returned a long-lived stream now returns an ordinary
  `RedisResult` and produces through `ctx.session`, which gained what that
  needs:

  ```ts
  execute: (args, ctx) => {
    const flush = ctx.session.deferPushesUntilAfterReply() // frames after +OK
    const unsubscribe = source.subscribe(frame => ctx.session.enqueuePush(frame))
    ctx.session.onReset(unsubscribe) // runs once on RESET or connection close
    return RedisResult.create(RedisValue.simpleString('OK'), { afterReply: flush })
  }
  ```

  On `RedisClientSession`, `registerResponseStreamCleanup` becomes `onReset`
  and `resetResponseStreams` becomes `resetPushProducers`; `enqueuePush`,
  `monitoring` and `startMonitor` are new. A front end that reads
  `RedisResult.value` instead of wire bytes should deliver
  `options.trailingFrames` as pushes after the reply and run
  `options.afterReply`, as `InMemoryRedisClient` now does (so its `pushes()`
  still yields the 2nd..Nth confirmations of `SUBSCRIBE a b c`). Both
  `InMemoryRedisClient` and the node-redis mock now run `afterReply`, which
  they previously skipped — e.g. `CLIENT KILL` of the caller's own connection
  now takes effect there.

- **BREAKING (`/core`)** The `afterExecute` and `onStream` hooks are gone from
  `ExecutionPolicy` ([#359]). None of the four shipped policies (auth, cluster,
  subscribed-mode, transaction) ever implemented them — only tests did — and
  supporting them forced a result/stream rewriting loop into both executor
  paths, plus an `assertSyncPolicyResult` guard to reject an async hook on the
  synchronous (Lua) path.

  `beforeExecute` is unchanged and still the place to short-circuit a command
  (queue / redirect / reject); it may still be async on the network path, and
  is still rejected when it returns a promise under `redis.call`. A custom
  policy that rewrote results or wrapped streams has no drop-in replacement —
  do it inside the command definition, or wrap `CommandExecutor`. Because the
  hooks were optional, a policy object that still declares them compiles and
  runs, silently doing nothing.

- **BREAKING (`/core`)** `CommandExecutor.executeRawWithPlan()` is removed
  ([#359]). It was a public method on the exported `CommandExecutor` class with
  exactly one caller — `executeRaw`, which discarded the `plan` half of its
  return value — so it is folded into `executeRaw`. The `RawExecutionResult`
  type it returned is gone with it (that type was never re-exported, so only the
  method is a break).

  ```
  (await executor.executeRawWithPlan(cmd, args, ctx)).result
    -> await executor.executeRaw(cmd, args, ctx)
  ```

  There is no replacement for the `plan` half. A caller that wants the
  `CommandPlan` builds it with the still-public `executor.plan(cmd, args)` and
  passes it to `executor.executePlan(plan, ctx)`. That is not equivalent to
  `executeRaw`: `plan()` throws on any planning error (unknown command, arity,
  argument parse) where `executeRaw` returns a RESP error reply, and the pair
  skips the MULTI-dirty/EXECABORT handling `executeRaw` applies to those errors.

- **BREAKING** and **BREAKING (`/core`)**: 79 single-message error classes
  are removed from `src/core/redis-error.ts` ([#364]). Each one only fixed a
  message string on a `RedisCommandError`. A client over TCP, or a socketless
  mock, only ever sees its own error type carrying that message, so none of
  these classes was what a consumer actually caught. The message text on the
  wire is unchanged.

  - **`js-redis-server`** (root) loses the 33 of them it exported:

    ```
    CountGreaterThanZeroError  DiscardWithoutMultiError  ExecWithoutMultiError
    ExpectedFloatError         ExpectedIntegerError      HashValueNotFloatError
    HashValueNotIntegerError   IndexOutOfRangeError      InvalidExpireTimeError
    LimitCantBeNegativeError   MinMaxNotFloatError       NoPasswordConfiguredError
    NoScriptError              NoSuchKeyError            NumKeysGreaterThanZeroError
    OffsetOutOfRangeError      PositiveCountError        RedisSyntaxError
    ResultingScoreNaNError     ScriptCallNoCommandError  ScriptDebugModeError
    ScriptFlushOptionError     ScriptUnknownCommandError StreamElementTooLargeError
    StreamIdExhaustedError     StringExceedsMaxSizeError TransactionDiscardedError
    WatchInsideMultiError      WrongNumberOfKeysError    WrongPassError
    ZaddGtLtNxConflictError    ZaddIncrPairError         ZaddNxXxConflictError
    ```

  - **`js-redis-server/core`** used to re-export the whole module
    (`export * from './core/redis-error'`), so it loses all 79. That is the 33
    above plus 46 that only `/core` exported (`BitOffsetError`,
    `GeoUnsupportedUnitError`, `InvalidStreamIdError`, `SameObjectError`,
    `DbIndexOutOfRangeError`, `NoProtoError`, `InvalidHllError`, and so on).
    `/core` now names its error exports explicitly: the classes kept below,
    plus `errorReplyBody` and `errorReplyBytes`. The new `errors` message
    factories are internal and are not exported from either entry point.

  Both entry points still export `RedisCommandError` and these subclasses:

  - `WrongNumberOfArgumentsError`, `UnknownRedisCommandError` and
    `UnknownSubcommandError`: code checks them with `instanceof`. The last
    is new since 0.3.0 and is now exported from the root as well.
  - `WrongTypeRedisError`: raised by the state layer.
  - `RedisMovedError`, `RedisCrossSlotError` and `RedisClusterDownError`:
    raised by the cluster policy.
  - `NoAuthError`: raised by the auth policy.
  - `ExecCommandAbortError`: raised by the executor. It is newly exported
    from the root.

  To migrate, catch `RedisCommandError` and check its `code` or `message`, for
  example `err.code === 'NOSCRIPT'` or `err.message === 'syntax error'`. To
  build a replacement error, use `new RedisCommandError(message, code)`.

- **BREAKING** `UnknownScriptSubcommandError` and `UnknownClusterSubcommandError`
  are removed from the root facade and from `/core` ([#430]). Both hard-coded
  the Redis 7.0+ wording (`unknown subcommand '%s'. Try SCRIPT HELP.`) with no
  compatibility profile in reach, so on a `redis-6.2` profile they produced a
  message real 6.2 never sends. There is no drop-in replacement class — the
  reply is now built by a profile-aware helper that needs the profile as an
  argument:

  ```
  new UnknownScriptSubcommandError(sub)   -> unknownSubcommandError('SCRIPT', sub, ctx.server.profile)
  new UnknownClusterSubcommandError(sub)  -> unknownSubcommandError('CLUSTER', sub, ctx.server.profile)
  ```

  `unknownSubcommandError` lives in `src/commands/helpers.ts` and is **not**
  exported from either entry point, because a caller outside the command layer
  has no `RedisExecutionContext` to take the profile from. A `/core` consumer
  that was constructing these classes by hand was, by construction, emitting the
  wrong text on older profiles; the two remaining uses of them were both inside
  this package. Catching them still works through `RedisCommandError`, which
  they extended and which every other error in the package extends too.

- **BREAKING (`/core`)** `RedisMonitorCommandEvent.timestampMs` is renamed to
  `timestampMicros` and its unit changes from milliseconds to **microseconds**
  ([#410]). Real Redis stamps `MONITOR` lines from `gettimeofday()`, so the six
  fractional digits it prints carry microsecond resolution; the old field was
  derived from `Date.now()` and left the last three digits permanently `000`.

  ```
  event.timestampMs        -> event.timestampMicros / 1000
  new Date(event.timestampMs) -> new Date(event.timestampMicros / 1000)
  ```

  A consumer that still reads `event.timestampMs` gets `undefined`, and any
  arithmetic on it `NaN`, so this is a silent break rather than a type error at
  runtime. Subscribers to `RedisMonitorFeed` are the only affected callers.

  Two helpers are added to `/core` alongside it: `monitorTimestampMicros()`, the
  clock the server now stamps with, and `formatMonitorTimestamp(micros)`, which
  renders the `<unix-seconds>.<6 digits>` field. Both live in `src/core/clock.ts`.

- **BREAKING (`/core`)** The six pub/sub methods on the `RedisClientSession`
  interface were collapsed into two. External implementors (test doubles) and
  callers (custom commands) need this mapping ([#376]):

  ```
  subscribePubSubChannels(ch)        -> pubsubSubscribe('channel', ch)
  unsubscribePubSubChannels(ch)      -> pubsubUnsubscribe('channel', ch)
  subscribePubSubShardChannels(ch)   -> pubsubSubscribe('shard', ch)
  unsubscribePubSubShardChannels(ch) -> pubsubUnsubscribe('shard', ch)
  subscribePubSubPatterns(p)         -> pubsubSubscribe('pattern', p)
  unsubscribePubSubPatterns(p)       -> pubsubUnsubscribe('pattern', p)
  ```

  Also removed from `ClientSession` (not on the interface, but public via
  `/core`):

  ```
  pubsubRegularSubscriptionCount  -> no replacement; it was channels + patterns.
                                     Use pubsubChannelCount + pubsubPatternCount.
  ```

  Wire output is unchanged — `pmessage` still carries the pattern ahead of the
  channel (a 4-element push frame, against 3 for `message` / `smessage`),
  channel and pattern confirmations still report the combined regular count
  while shard confirmations report the shard count, and the two counters are
  still not unified.

- **BREAKING (`/core`)** `RedisKeyspace` and `WrongRedisTypeError` are no longer
  re-exported ([#375]). The keyspace was collapsed into `RedisDatabase`, which
  owns its `Map<keyId, KeyspaceEntry>` directly; `src/state/keyspace.ts` is now
  types-only (`KeyspaceEntry`, `SetOptions`, `ExpirationState`,
  `KeyspaceMutationTracker`). The state-layer `WrongRedisTypeError` is gone —
  the core `WrongTypeRedisError` is thrown directly, so there is one fewer error
  class and one fewer rethrow. Behavior is unchanged: WATCH dirty semantics,
  empty-collection deletion and lazy expiry all carry over verbatim.

- **BREAKING (`/core`)** Dead flexibility removed from the core pipeline
  ([#377]):

  | Removed                          | Replacement                                        |
  | -------------------------------- | -------------------------------------------------- |
  | `RedisTurnQueue` (type)          | `SerialTurnQueue`, already exported                |
  | `ClientSessionOptions.turnQueue` | none — no caller ever passed it                    |
  | `createNoopParkHandler`          | `createDefaultParkHandler` — it was an alias of it |
  | `CommandRegistry.override(d)`    | `register(d, { override: true })`                  |
  | `CommandRegistry.has(n)`         | `get(n) !== undefined`                             |
  | `CommandRegistry.getNames()`     | `getAll().map(d => d.name)`                        |
  | `CommandPlan.flags`              | `plan.definition.flags`                            |

  Nothing changed on the root barrel. One residue worth knowing about for
  `/core` consumers who pass their own `signal` to `ClientSession`: aborts now
  surface the caller's `signal.reason` (a `DOMException`, or whatever was passed
  to `abort()`) instead of a fixed `Error`, because `ClientSession` uses
  `signal.throwIfAborted()`, which rethrows the reason verbatim.

  For an ordinary reason there is no wire-visible change — the adapter maps it
  to `-ERR internal server error` either way. But the mapping is not
  unconditional: `Resp2SessionAdapter.writeError` passes a `RedisCommandError`
  (and a `Resp2ParseError`) straight through to the client. So
  `controller.abort(new WrongTypeRedisError(...))` now puts `-WRONGTYPE …` on
  the wire where it previously produced `-ERR internal server error`. Only
  reasons that are neither of those two classes are masked.

- **BREAKING (`/core`)** The `encoder` option was removed from
  `Resp2ServerOptions`, `AttachSessionOptions`, `Resp2SessionAdapterOptions` and
  `CreateVirtualConnectionOptions` ([#374]). It could never have any effect:
  `Resp2SessionAdapter.writeRedisResult` spread the stored encoder and then
  immediately overrode its only field with the session's negotiated protocol
  version. A caller that was passing `encoder:` was getting nothing from it and
  will now see a TypeScript excess-property error; runtime behavior is
  identical. `RespEncodeOptions` itself is **kept** — it is still the options
  parameter of `encodeRedisValue` / `encodeRedisResult`.

- **BREAKING** The deprecated `buildRedisCluster` alias is gone from both the
  root entry point and `/core` ([#365]). It was `createRedisCluster` under its
  pre-rename name; import `createRedisCluster` instead — same function, same
  un-started `RedisCluster`. TypeScript reports it at compile time; under ESM a
  leftover named import now fails at load time; under CJS
  `require(...).buildRedisCluster` is `undefined`.

- **BREAKING (`/core`)** `RedisDatabase.activeNotifyCommand` was removed
  ([#444]). It was a mutable per-database tag holding the name of the command
  running against that database, which keyspace notifications read to name write
  events. A command that parked (BLPOP) or resumed out of order left a stale
  name behind. The name now travels with each event as
  `RedisMutationEvent.command`, stamped by a per-command view of the database:
  `db.withOrigin(name)` returns a handle onto the same database whose events carry
  `name`, and `db.origin` reads it (`undefined` on the database itself). Code
  that set the field to name its own writes should write through a view instead:

  ```
  db.activeNotifyCommand = 'lpush'; db.updateList(key, ...)
    -> db.withOrigin('lpush').updateList(key, ...)
  ```

### Changed

- **BREAKING (`/core`)** `RedisServerState.notifyKeyspaceEvents` is now the
  parsed flag set (`ReadonlySet<KeyspaceNotifyFlag>`), not the canonical
  string ([#371]); both types are now exported from `/core`. CONFIG SET parses the value once instead of the notifier
  re-parsing the string on every mutation. Code that assigned the field
  directly must now assign a set with `A` already expanded:

  ```
  state.notifyKeyspaceEvents = 'KEA'
    -> state.notifyKeyspaceEvents = new Set(['K', 'E', 'g', '$', 'l', 's',
                                             'h', 'z', 'x', 'e', 't', 'd'])
  ```

  Or send `CONFIG SET notify-keyspace-events KEA` through a client, which
  validates it. In plain JS an old string assignment is not caught at compile
  time: the next key write throws `TypeError: flags.has is not a function`
  from the mutation-bus subscriber, after the write has already been applied.
  Reads that expected the string should render it with CONFIG GET.

- **BREAKING** The two socketless clients now decode maps, doubles, big
  numbers and booleans according to the protocol the connection negotiated
  ([#414]). They used to decode a map to an object, and those three scalars to
  their RESP3 JS types, whatever the protocol. Affected: `createInMemoryClient()`'s
  `command()`, and on `createNodeRedisMock()` the raw paths — `sendCommand()`,
  `eval()` and `multi().addCommand(…).exec()`. Both clients start on RESP2, so
  on a connection that never sends `HELLO 3` the visible replies change:

  ```
  HGETALL / CONFIG GET                 { f1: 'v1' }   -> ['f1', 'v1']
  XREAD                                { s: […] }     -> [['s', […]]]
  ZSCORE / ZINCRBY                     2.5            -> '2.5'
  ZRANGE … WITHSCORES                  ['a', 1]       -> ['a', '1']
  big number (Lua)                     12345678n      -> '12345678'
  boolean (Lua, under redis.setresp(3)) true / false  -> 1 / 0
  ```

  The `WITHSCORES` row was already flat at RESP2 ([#408] made the pair shape
  protocol-dependent); only its scores change. This is what real node-redis
  returns on the same paths, which the old shapes contradicted at RESP2. Two
  ways forward for a caller that wants the object and number shapes back: send
  `HELLO 3` on the connection (a real RESP3 client is what produces them), or,
  on the node-redis facade, use the curated method — `hGetAll()` still returns
  an object at both protocols, because node-redis' own `transformReply` builds
  it from the flat array. `createIoredisMock()` drives the real RESP2-only
  `ioredis` and is unaffected by this entry.

- **BREAKING (`/core`)** `Resp2CommandDecoder` is now pull-based, and its
  constructor requires the live bulk-length limit ([#431]). Two breaks to the
  exported class:

  1. `push(chunk)` only buffers and returns `void`; it no longer returns
     `{ frames, error }`. Frames are taken one at a time from the new `next()`,
     which returns a `Resp2CommandFrame`, or `null` when more bytes are needed,
     and **throws** the `Resp2ParseError` instead of returning it. The decoder
     is terminal after that throw: every later `next()` re-throws the same
     error and `push()` is ignored, because a RESP stream cannot be
     resynchronised mid-frame.
  2. `new Resp2CommandDecoder()` with no argument now throws
     `TypeError: Cannot read properties of undefined (reading 'maxBulkLength')`.
     Pass `{ maxBulkLength: () => bigint }` — the `proto-max-bulk-len` in force,
     read on every bulk header so a `CONFIG SET` applies to the next frame.

  ```ts
  // before
  const decoder = new Resp2CommandDecoder()
  const { frames, error } = decoder.push(chunk)
  for (const frame of frames) handle(frame)
  if (error) fail(error)

  // after
  const decoder = new Resp2CommandDecoder({
    maxBulkLength: () => server.protoMaxBulkLen, // 536870912n is Redis' default
  })
  decoder.push(chunk)
  try {
    for (let frame; (frame = decoder.next()); ) handle(frame)
  } catch (error) {
    if (!(error instanceof Resp2ParseError)) throw error
    fail(error) // terminal: report it and close the connection
  }
  ```

  Pulling one frame at a time is what makes the limit live: the session runs
  each command before framing the next, so a pipelined
  `CONFIG SET proto-max-bulk-len` governs the frames behind it even when they
  arrived in the same read. Only direct users of the decoder are affected;
  `Resp2SessionAdapter`, `attachSession` and the servers built on them are
  migrated and keep their signatures.

  `Resp2ParseError` gains a `messageBytes` field and an optional second
  constructor argument carrying the error text as raw bytes. This is additive:
  existing callers of `new Resp2ParseError(message)` are unaffected.

- **BREAKING (`/core`)** One `SerialTurnQueue` per server instead of per
  database ([#369]). `RedisDatabase.turnQueue` is gone; the queue is now
  `RedisServerState.turnQueue`, and every command on every database, the
  active-expiry sweep (one turn per tick for all databases) and seeding take
  their turn from it. Sessions on different databases of the same server now
  serialize against each other, as they do on single-threaded Redis — the mock
  previously let them run in parallel. Cluster nodes each keep their own queue.
  Code that held `state.getDatabase(n).turnQueue` to fence a database should
  hold `state.turnQueue` instead. Blocking commands still release the turn
  while parked.

- **BREAKING (`/core`)** `RedisMutationEvent` changed shape ([#379], [#486]):

  - It has a new `notify` variant (`{ type: 'notify', database, key,
    valueType }`): a keyspace notification with no modified-key signal, as in
    real Redis where `notifyKeyspaceEvent` and `signalModifiedKey` are
    independent. It is emitted for in-place stream consumer-group / last-id
    changes and for the removal (`hdel`, `lpop`, ...) that empties a collection,
    just before its `delete`. `RedisMutationBus` delivers it to `subscribe()`
    listeners only, never to `subscribeKey()` ones (WATCH, blocked clients).
    An exhaustive `switch (event.type)` stops type-checking. A non-exhaustive
    listener must ignore `notify` rather than treat it as a write: it carries no
    value, and a replica applying it would have nothing to apply.
  - Every variant gained an optional `command`: the name of the command it was
    emitted on behalf of (see `RedisDatabase.withOrigin` above).
  - `write` events now carry a required `valueType`, so code that constructs
    them (tests, custom buses) must set it.
  - A delivered `write` event's `value` is now a lazy getter. Each listener's
    copy is cloned from the key's *live* value on the listener's first read,
    instead of being cloned for every listener up front, which made filling a
    collection one element at a time quadratic. A listener that needs the value
    as of that write must read it synchronously, during dispatch. Read after a
    later mutation, it shows the later state. `{ ...event }` inside the listener
    takes such a snapshot.

### Added

- `PubSubKind` (`'channel' | 'shard' | 'pattern'`) is exported from `/core`,
  because it appears in the signature of the published `RedisClientSession`
  interface and declaration emit requires it ([#376]).

### Fixed

- `COMMAND` / `COMMAND INFO` report each command's real arity and
  first/last/step key positions ([#370]); most commands used to answer arity
  -1 and `0 0 0`. The version-dependent ones follow the profile: `EXPIRE`
  family and `XSETID` are arity 3 before 7.0, `ZRANK`/`ZREVRANK` 3 before
  7.2, `COMMAND GETKEYS`/`GETKEYSANDFLAGS` -4 on 7.0. The parsers enforce the
  same arity, so `ZRANK ... WITHSCORE` before 7.2, `XSETID` options on 6.2
  and any extra `EXPIRE` token on 6.2 are `wrong number of arguments`.
  `GEOPOS`/`GEOHASH` accept a key with no members and `QUIT` ignores extra
  arguments, as in Redis.

- The unknown-command error echoes the command name and args the way real
  Redis does ([#384]). Real Redis formats it with C `printf`
  (`'%.128s'` for the name, then `'%.*s' ` per arg against a 128-byte budget),
  so the name and args are now echoed as raw bytes instead of hex-dumping any
  non-printable one, each is cut at its first NUL, the name is cut at 128
  bytes, and the args stop when their 128-byte budget is spent (the last one
  cut to what is left, possibly mid UTF-8 sequence) rather than at 61 chars
  plus `...` per arg. On the `redis-6.2` profile the reply takes 6.2's form
  (new gate `error.unknown-command-wording`): backtick quotes, `, ` between
  args and no cap on the name. The separator counts against the same budget,
  so 6.2 echoes fewer args.

- On the `redis-6.2` profile, errors the scripting layer raises itself (an
  unknown or not-allowed command, wrong arity, no command, a non-string
  argument) use 6.2's wording, for example `Unknown Redis command called from
  Lua script`, with no `ERR` code. A `redis.call` abort also gets 6.2's inner
  `@user_script: <line>: ` position, byte for byte against redis-server
  6.2.24. A `redis.pcall` rejection still lacks that position, because the Lua
  engine does not pass the calling line to the host.

- Double replies are spelled the way the emulated version spells them ([#451]).
  Redis 6.2 / 7.0 print `%.17g`; Redis 7.2+ and every Valkey print
  `d2string()`: every digit of an integer within ±2^62, otherwise Redis's
  Grisu2 `fpconv_dtoa`, which is now ported rather than approximated with JS
  `toString()`. Affected: `ZSCORE`, `ZMSCORE`, `ZINCRBY`, every `WITHSCORES`
  reply, RESP3 `,` doubles, `ZSCAN` scores, and a score a script reads with
  `redis.call` (the Lua bridge had its own copy of the old formatter). For
  example:

  ```
  score               redis-6.2 / 7.0             redis-7.2+ / valkey   old (every profile)
  0.1                 0.10000000000000001         0.1                   0.1
  0.0000123           1.2300000000000001e-05      1.23e-5               0.0000123
  2^62                4.6116860184273879e+18      4611686018427387904   4611686018427388000
  1e20                1e+20                       1e+20                 100000000000000000000
  ```

  GEO coordinates (`GEOPOS`, `WITHCOORD` on `GEOSEARCH` / `GEORADIUS*`) are
  now a `,` double on RESP3 — they were a bulk string — and follow their own
  version split: Redis 8.0 prints `d2string()` (`13.361389338970184`), while
  Redis 6.2–7.4 and every Valkey print `%.17Lf` with the trailing zeros
  trimmed (`13.36138933897018433`). The coordinate *values* still come from
  the mock's own geohash decode, which can differ from Redis's in the last
  digits; that is a separate issue.

  Pinned against 1,203 values captured from real redis-server 6.2.14 / 7.0.15 /
  7.2.4 / 7.4.4 / 8.0.0 / 8.0.6 and valkey-server 8.0.0 / 9.0.0
  (`tests/fixtures/redis-double-format.json`). `encodeRedisValue` /
  `encodeRedisResult` (`js-redis-server/core`) take an optional `profile`
  for this; without one they use the default profile's spelling.
  `RedisValue.double()` takes an optional exact `text` for replies whose
  spelling is not `addReplyDouble()`'s.

- `SORT` / `SORT_RO` scan their options when they run, left to right, the way
  `sortCommand()` does, and the cluster `BY` / `GET` pattern guard moved from
  `ClusterPolicy` into that scan ([#417]). The first offending option in
  argument order is now the one reported (it was always `BY` before `GET`); a
  denied pattern is reported before a later token fails to parse (a trailing
  syntax error used to win); and inside `MULTI` every SORT option error — the
  cluster denial, `ERR syntax error`, a bad `LIMIT` integer — replies `+QUEUED`
  and surfaces as an element of the `EXEC` array, where it used to fail at
  queue time and abort the transaction with `EXECABORT`. Holds for both the
  pre-7.4 and the 7.4+ wordings. `ClusterPolicy` no longer knows about SORT.

- `SORT` tie order and option handling now match Redis ([#443]): elements that
  compare equal keep their load order under `DESC` too (`ALPHA DESC` used to
  reverse them); under `ALPHA` a missing `BY` weight orders before an empty
  one; a constant `BY` disables sorting even when a glob `BY` comes after it,
  and otherwise the *last* glob is the one looked up; and a set whose members
  are all canonical 64-bit integers is read in ascending numeric order, as an
  intset is stored, so `SORT s BY nosort` returns it sorted. The source key is
  also read once rather than twice. Set order still differs where it depends
  on the set's encoding history, which the mock does not track: a set created
  from an integer keeps its integers sorted ahead of later non-integer members
  in Redis (`SADD s 3 1 a` → `1 3 a`; the mock gives `3 1 a`).

- A command pipelined behind a multi-channel `SUBSCRIBE` / `PSUBSCRIBE` /
  `SSUBSCRIBE` could have its reply written between the confirmations. They
  now go out as one reply, in Redis's order ([#455]).
- A multi-channel `SUBSCRIBE` queued in `MULTI` replied `Streaming command is
  not allowed in transaction` from `EXEC`. It now runs, and `EXEC` embeds every
  confirmation in its array exactly as Redis does ([#366]).
- `MONITOR` queued in `MULTI` now fails with Redis's `MONITOR isn't allowed for
  DENY BLOCKING client`, and a repeated `MONITOR` gets no reply and does not
  double the feed, as in Redis ([#366]).

- On the `redis-6.2` profile a script that aborts — a failing `redis.call`, or
  a Lua runtime error — now carries Redis 6.2's decoration,
  `-ERR Error running script (call to f_<sha>): @user_script:<line>: <error>`,
  instead of the 7.0 suffix `<error> script: <sha>, on @user_script:<line>.`
  ([#442]). As in 6.2 the reply is always `-ERR`: a failing command's own code
  (`WRONGTYPE ...`) is folded into the body, and a runtime error shows its
  position twice. Gated as `script.abort-error-suffix` (Redis 7.0 / Valkey 7.2).
  The frame is exact for errors a *command* returns and for Lua runtime errors;
  rejections raised by the scripting layer itself (unknown or not-allowed
  command, wrong arity, no arguments, bad argument type) still differ on 6.2,
  which words them differently and adds an inner `@user_script: <line>: `
  position the engine does not expose ([#439]).

- `proto-max-bulk-len` is now enforced where Redis primarily enforces it: in the
  protocol reader, for every command ([#431], [#415]). A bulk argument longer
  than the limit is refused from its header, before the payload is read and
  before any handler runs, with `-ERR Protocol error: invalid bulk length`, and
  **the server then closes the connection**. Previously only `APPEND` and
  `SETRANGE` checked the limit, so `SET`, `MSET`, `LPUSH`, `HSET` and the rest
  accepted arguments of any size. A bulk exactly the size of the limit is still
  accepted. Identical on Redis 6.2, 7.2 and 8.0, so it is not profile-gated.

- `SETBIT`, `GETBIT`, `BITFIELD` and `BITFIELD_RO` derive their bit-offset
  ceiling from the live `proto-max-bulk-len` — `(offset >> 3) >= limit` is
  refused — instead of a hardcoded 2^32 ([#431], [#415]). They agree at the 512MB
  default and diverge once the limit is lowered. For operations that allocate
  (`SETBIT`, and `BITFIELD`'s `SET` / `INCRBY`) the ceiling is additionally
  capped at 512MB, as `APPEND` / `SETRANGE` already are, so raising the setting
  cannot make the test process materialise an unbounded string; reads are not
  capped, because they never allocate.

  Their argument errors are now **runtime** errors, as in Redis: inside `MULTI`
  an out-of-range offset, a bad bit value, or a malformed `BITFIELD` operation
  replies `+QUEUED` and surfaces as an element of the `EXEC` array, where it
  used to fail at queue time and abort the transaction with `EXECABORT`. Only
  arity errors still fire at queue time. `BITFIELD_RO`'s GET-only check now runs
  after the whole operation list parses, and a `BITFIELD` operation missing its
  arguments answers `ERR syntax error` before its type is read, both matching
  Redis.

- After a `SELECT`, `MOVE` and `COPY … DB` into the database that was selected
  *before* it no longer publish their keyspace notifications as `select`
  ([#359]). With `notify-keyspace-events KEA`, keyevent channel shown (the
  keyspace channel carries the same event names):

  ```
  SELECT 1; SET k v; MOVE k 0
    real Redis 7.2: __keyevent@1__:move_from k   __keyevent@0__:move_to k
    before:         __keyevent@1__:del k         __keyevent@0__:select k
    now:            __keyevent@1__:del k

  SELECT 1; SET s v; COPY s c DB 0
    real Redis 7.2: __keyevent@0__:copy_to c
    before:         __keyevent@0__:select c
    now:            (nothing)
  ```

  The executor names write events through a per-database tag, and `SELECT`
  restored that tag onto the database it switched *to*, leaving the one it
  switched *from* tagged `select`. Every later command restores the tag it
  saved, so the stale value was never cleared, and `MOVE` / `COPY … DB` write
  into a database the executor never tags. Because the tag lives on the
  database rather than the connection, one client's `SELECT` mislabelled
  another client's `MOVE`; it also happened through `MULTI`/`EXEC` and `EVAL`.

  This removes the wrong event; it does not add the right ones. `MOVE` and
  `COPY … DB` still publish nothing on the target database, where real Redis
  sends `move_to` / `copy_to`. That gap predates this change and is tracked
  separately.

- Every container command's unknown-subcommand reply now matches real Redis on
  every profile ([#430], closing [#413]). The message had fifteen hand-rolled
  copies across the command modules with three wordings live at once; they are
  replaced by one profile-aware helper, which fixes three things at once:

  - The wording is gated on `error.unknown-subcommand-wording`
    (Redis 7.0 / Valkey 7.2), so `redis-6.2` gets
    `Unknown subcommand or wrong number of arguments for '<name>'. Try <CMD> HELP.`
    where 7.0+ gets `unknown subcommand '<name>'. Try <CMD> HELP.`. Previously
    `CONFIG` sent the 7.0 form on every profile and `XGROUP` sent the 6.2 form
    on every profile.
  - The echoed name is truncated the way real Redis truncates it: at the first
    NUL byte on every profile, and then to 128 **bytes** from 7.0 (`%.128s`).
    A cut that lands inside a multi-byte character emits the partial byte, as
    real Redis does.
  - The echoed name reaches the wire byte for byte. It was decoded as UTF-8, so
    `CONFIG \xff\xfe\xfd` came back as three U+FFFD replacement characters —
    nine bytes where real Redis sends three. Error replies are now assembled as
    bytes end to end, including across the Lua boundary, so a nested
    `redis.call` error keeps its bytes too.

  `addReplySubcommandSyntaxError` — the `unknown subcommand or wrong number of
  arguments for '<name>'. Try <CMD> HELP.` reply a container raises for a known
  subcommand with unusable arguments — took the same 7.0 case flip and is now
  gated alongside it. It reaches `PUBSUB` only so far ([#437]).

- `CONFIG SET` failures under the `redis-6.2` profile now match real 6.2
  ([#416]). Before, the mock sent the 7.0+ wording (or behaviour) on every
  profile in four places:
  - an invalid `notify-keyspace-events` value now gets
    `Invalid argument '<value>' for CONFIG SET '<name>'`, with no ` - <detail>`
    suffix (6.2 hand-parses this parameter);
  - the parameter name is echoed exactly as the client typed it (7.0+ echoes it
    lower-cased);
  - an unknown parameter gets `Unsupported CONFIG parameter: <name>`;
  - the `n` (new-key) class, which Redis only added in 7.0, is rejected
    (gated as `notify.keyspace.new-key-class`, Redis 7.0 / Valkey 7.2).

  7.0+ profiles are unchanged.

- `CONFIG <unknown-subcommand>` now matches real Redis, and is gated on the
  profile ([#410]). Redis 7.0 moved container commands into the command table,
  which replaced `Unknown subcommand or wrong number of arguments for '%s'. Try
  CONFIG HELP.` with `unknown subcommand '%s'. Try CONFIG HELP.` and added
  `%.128s` truncation of the echoed name. The mock previously sent the 6.2 form
  on every profile. Gated as `error.unknown-subcommand-wording`
  (Redis 7.0 / Valkey 7.2); the other 14 hand-rolled copies of this message are
  tracked in [#413].

- The node-redis mock cluster client now routes every command by the keys the
  executor actually extracts (`executor.plan(name, args).keys`) instead of a
  hand-rolled heuristic that took the single argument after the command name
  ([#378]). That heuristic mis-routed `EVAL` (by the script text), `ZDIFF` /
  `LMPOP` / `SINTERCARD` (by the numkeys literal), `BITOP` (by the operation
  name), and routed `MSET`, `RENAME`, `SINTERSTORE`, `COPY`, `ZUNIONSTORE` and
  `GEORADIUS … STORE` by one key while they span several. CROSSSLOT reporting is
  preserved.

- Keyspace notifications ([#379], [#381], [#444], [#446]) now match real
  Redis in these cases:

  - Removing the last element of a hash, list, set or sorted set publishes the
    removal event (`hdel`, `lpop`, `srem`, `zrem`, `spop`, ...) and then `del`;
    before, it published only `del`.
  - XGROUP CREATE / CREATECONSUMER / SETID / DELCONSUMER / DESTROY publish
    `xgroup-create`, `xgroup-createconsumer`, ... (before: nothing, or `xgroup`
    for MKSTREAM), and XSETID publishes `xsetid`. Neither dirties a WATCH on
    the stream. XREADGROUP / XCLAIM / XAUTOCLAIM publish
    `xgroup-createconsumer` when they create a consumer.
  - Blocking, multi-key and move-style commands are named after the operation,
    not the command. `BLPOP`/`BRPOP`/`LMPOP`/`BLMPOP` publish `lpop`/`rpop`.
    `BZPOPMIN`/`BZPOPMAX`/`ZMPOP`/`BZMPOP` publish `zpopmin`/`zpopmax`.
    `LMOVE`/`BLMOVE`/`RPOPLPUSH` publish `lpush`/`rpush` on the destination,
    then `lpop`/`rpop` on the source. `SMOVE` publishes `srem` then `sadd`.
    A same-key `LMOVE` rotates the list without deleting it, and a same-key
    `SMOVE` changes nothing.
  - `HGETDEL` publishes `hdel`; the `HEXPIRE` family and `HGETEX EX/PX/...`
    publish `hexpire` (`hdel` for a time already past); `HGETEX PERSIST`
    publishes `hpersist`; `HSETEX` publishes `hset` then `hexpire` / `hdel`.
  - `SORT ... STORE` and `ZRANGESTORE` over an existing key publish one
    `sortstore` / `zrangestore` instead of `del` followed by the command name.
  - A blocking command parked on a database no longer lends its name to other
    commands' writes into that database, and blocking commands resumed out of
    order no longer leave a stale name that every later MOVE / COPY into it
    reused.

- Hash-field TTLs expire actively ([#486]): a field is dropped at its deadline
  by the server's active-expiry sweep, publishing `hexpired` (then `del` if the
  hash empties), with no command touching the key, and `EXISTS` then reports an
  emptied hash as gone. Before, fields were dropped only on the next access to
  the hash, which then published under that command's name (`hgetall`, `hlen`,
  ...). Between sweeps, any hash command still drops an expired field first.
  That differs from real Redis with active expiry off, where only a field
  lookup expires it.

- `XAUTOCLAIM` and `XCLAIM` validate their arguments like real Redis ([#486]):

  - **XAUTOCLAIM** checks all its arguments before the key, each with its own
    message:
    - `ERR Invalid min-idle-time argument for XAUTOCLAIM`
    - `ERR COUNT must be > 0` for a COUNT outside `1..LONG_MAX/16`, including
      0 (previously accepted, with a consumer created)
    - `ERR invalid start ID for the interval`; `-`, `+` and exclusive `(`
      starts are accepted
  - **XCLAIM** checks WRONGTYPE / NOGROUP first, then parses min-idle-time,
    ids up to the first non-id, then options:
    - `ERR Unrecognized XCLAIM option '<token>'` for any other token (e.g.
      `notanid` was `Invalid stream ID`)
    - `ERR Invalid IDLE|TIME|RETRYCOUNT option argument for XCLAIM`
    - out-of-range times are clamped instead of rejected, as in Redis
  - Neither creates a consumer when it rejects the call.

- `XINFO CONSUMERS` reports `inactive` as `-1` until the consumer is delivered
  new entries or claims one ([#486]). Before, it echoed `idle` for a consumer
  that never got an entry, and every read or claim attempt refreshed it. Now
  only a `>` read that returns entries, or a claim that claims something, does.

- Filling one hash field by field is no longer quadratic ([#486]): 20,000
  single-field `HSET`s into one key took minutes and now take well under a
  second. Every write used to clone the whole value for each mutation
  listener; the clone is now made only when a listener reads it (see the
  `RedisMutationEvent` change above).

## [0.3.0] and earlier

Released before this file existed. See the
[release tags](https://github.com/fatal10110/js-redis-server/tags) and the pull
requests they contain.

[#360]: https://github.com/fatal10110/js-redis-server/issues/360

[#359]: https://github.com/fatal10110/js-redis-server/issues/359
[#374]: https://github.com/fatal10110/js-redis-server/pull/374
[#375]: https://github.com/fatal10110/js-redis-server/pull/375
[#376]: https://github.com/fatal10110/js-redis-server/pull/376
[#377]: https://github.com/fatal10110/js-redis-server/pull/377
[#378]: https://github.com/fatal10110/js-redis-server/pull/378
[#408]: https://github.com/fatal10110/js-redis-server/pull/408
[#410]: https://github.com/fatal10110/js-redis-server/pull/410
[#413]: https://github.com/fatal10110/js-redis-server/issues/413
[#414]: https://github.com/fatal10110/js-redis-server/issues/414

[#430]: https://github.com/fatal10110/js-redis-server/pull/430
[#437]: https://github.com/fatal10110/js-redis-server/issues/437
[#442]: https://github.com/fatal10110/js-redis-server/issues/442
[#439]: https://github.com/fatal10110/js-redis-server/issues/439

[#415]: https://github.com/fatal10110/js-redis-server/issues/415
[#431]: https://github.com/fatal10110/js-redis-server/pull/431
[#451]: https://github.com/fatal10110/js-redis-server/issues/451
[#417]: https://github.com/fatal10110/js-redis-server/issues/417
[#443]: https://github.com/fatal10110/js-redis-server/issues/443
[#365]: https://github.com/fatal10110/js-redis-server/issues/365
[#366]: https://github.com/fatal10110/js-redis-server/issues/366
[#369]: https://github.com/fatal10110/js-redis-server/issues/369
[#455]: https://github.com/fatal10110/js-redis-server/issues/455
[#371]: https://github.com/fatal10110/js-redis-server/issues/371
[#416]: https://github.com/fatal10110/js-redis-server/issues/416
[#370]: https://github.com/fatal10110/js-redis-server/issues/370
[#379]: https://github.com/fatal10110/js-redis-server/issues/379
[#381]: https://github.com/fatal10110/js-redis-server/issues/381
[#444]: https://github.com/fatal10110/js-redis-server/issues/444
[#446]: https://github.com/fatal10110/js-redis-server/issues/446
[#486]: https://github.com/fatal10110/js-redis-server/pull/486
[#364]: https://github.com/fatal10110/js-redis-server/issues/364
[#384]: https://github.com/fatal10110/js-redis-server/issues/384
[unreleased]: https://github.com/fatal10110/js-redis-server/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/fatal10110/js-redis-server/releases/tag/v0.3.0
