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

- **BREAKING** The `geo.store-keyspec-variable-flags` member of `FeatureId`
  was removed ([#494]). `variable_flags` on Valkey's GEORADIUS STORE key
  specs is now one of the many per-version facts the real command table
  holds, so no code asks for the gate any more. A caller that tested it with
  `profile.has(...)` should compare `profile.flavor` / `profile.versionNum`
  instead.

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

- **BREAKING (`/core`)** The set handle `RedisDatabase.updateSet()` passes
  its callback lost two methods ([#504]). A set's members now stay in the
  order Redis stores them, and neither method kept that order:

  ```
  randomMemberEntries()          -> memberEntries(), same [hex, member] pairs,
                                    in storage order
  replaceMembers(ids, buffers)   -> replaceWith(setData), which also copies
                                    the source's encoding
  ```

  `addMember()` takes an optional `SetEncodingRules` second argument and
  `RedisSetData` gains an optional `intset` flag; code that builds a
  `RedisSetData` without it gets a set that is never an intset. Likewise, a
  new set becomes an intset only if the callback calls
  `prepareForAdd(first, sizeHint, rules)` before its first `addMember()`, as
  `SADD` does; adding members to a new key without it gives a set that is
  never an intset.

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
  - `WrongTypeRedisError`: raised by the state layer (and by commands that
    check a key's type themselves).
  - `RedisMovedError`, `RedisCrossSlotError` and `RedisClusterDownError`:
    raised by the cluster policy.
  - `NoAuthError`: raised by the auth policy (and by HELLO before auth).
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

- Scripts run on **lua-redis-wasm 2.1** (was 1.5) ([#449], [#502], [#503]).
  What a script sees changes to match real Redis, byte for byte against
  redis-server 6.2.24 to 8.0.6 and valkey-server 7.2.14 to 9.0.6, except for
  the known gaps at the end of this list:
  - `{double=…}`, `{big_number=…}`, `{map=…}`, `{set=…}` and
    `{verbatim_string=…}` tables convert at either protocol level; they came
    back as `[]` unless the script had called `redis.setresp(3)` ([#449]).
    On `redis-6.2` a `{big_number=…}` or `{verbatim_string=…}` table still
    replies `[]`, at any depth, because Redis 6.2 has neither type (new gate
    `script.big-number-verbatim-returns`).
  - Number arguments to `redis.call` / `redis.pcall` are spelled in the
    shortest form that round-trips, and whole numbers as integers:
    `1/3` → `0.3333333333333333` and `1e15` → `1000000000000000` (were
    `0.33333333333333` and `1e+15`, from Lua's `%.14g`).
  - A returned function or userdata becomes a null reply, at any depth,
    instead of failing the script. A returned number outside the 64-bit
    range replies `-9223372036854775808` instead of saturating. A
    `{ok=…}` / `{err=…}` reply string is cut at its first NUL byte, and CR / LF
    become spaces.
  - A script that runs out of its instruction budget aborts with `Script
    killed by fuel limit` in the profile's decoration (the message used to
    start with `user_script:<line>: `), and `pcall` can no longer catch it.
  - After `redis.setresp(3)` a null reply reaches the script as `nil`, not
    `false`, so `HMGET h f nope f` returns `['v']` as in Redis ([#449]).
  - `return 1, 2` replies `1`, the first value (was `2`), and `math.random`
    uses Redis's generator, so a seeded sequence is the one Redis gives.
  - `error()`, `error(nil)` and `error({err=...})` abort with `-ERR nil
    script: …` or the table's own error (`-WRONGTYPE x script: …`, `-boom
    script: …`) from 7.0, instead of `script execution failed` and a NUL
    byte. `error('ERR x', 0)` keeps its leading `ERR` (`-ERR ERR x …`).
  - `redis.error_reply('-MY x')` replies `-MY x` from 7.0 and `--MY x` on
    6.2; `redis.error_reply('x')` is `-ERR x` from 7.0 and `-x` on 6.2.
  - `redis.sha1hex()` with no argument, or more than one, is `wrong number of
    arguments`.
  - A `redis.pcall` argument that is not a string or number returns the
    error instead of aborting the script ([#492]).
  - From 7.0 (and on Valkey) `redis.call` raises an error table, so an
    `xpcall` handler receives `{err=...}`; `pcall` still returns the string.
  - `redis.log` checks its level (`Invalid log level.`, `Invalid debug
    level.` before 7.4) and arguments with each version's wording.
  - Known gaps, which need engine work ([#540]): on 6.2 and 7.0 Redis spells
    number arguments with `%.17g` (`0.1` → `0.10000000000000001`, `1/3` →
    `0.33333333333333331`) and 7.2.0 to 7.2.4 spell `1e15` as `1e+15`; the
    mock uses the shortest form on every profile. On 6.2, `error()`,
    `error(nil)` and `error({…})` make Redis's own error handler fail
    (`@err_handler_def:9: ... attempt to concatenate local 'err'`); the mock
    reports the value.

  **BREAKING (`/core`)** `setLuaWasmLoadOptions()` takes lua-redis-wasm 2.x
  `LoadOptions`: `limits.maxMemoryBytes` is gone (it was never enforced),
  limits must be non-negative integers, and a custom `.wasm` / glue given
  through `wasmPath` / `wasmBytes` / `modulePath` must be built from
  lua-redis-wasm 2.1. Its `redisProps` are merged over the ones the server
  sets (see Fixed).

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

- **BREAKING** `createNodeRedisMock()` (standalone and cluster) now starts on
  the protocol the installed node-redis negotiates by default, not always on
  RESP2 ([#489]). On node-redis 6 that is RESP3, the protocol a
  `createClient()` with no `RESP` option gets from its connect-time `HELLO 3`
  (node-redis 6 exports `DEFAULT_RESP = 3`). Without `redis` installed the
  facade also uses RESP3, as it models v6 elsewhere. On node-redis 4 and 5,
  which default to RESP2, nothing changes. The raw paths of a default facade
  therefore now return the RESP3 column of the [#414] entry above: `HGETALL`
  through `sendCommand()` is `{ f1: 'v1' }`, `ZSCORE` is `2.5`. To keep the
  RESP2 shapes, pass the new `RESP: 2` option, as you would to node-redis:

  ```
  await createNodeRedisMock()              -> await createNodeRedisMock({ RESP: 2 })
  ```

  The curated methods (`hGetAll()`, `zRange()`, …) return the same values at
  both protocols and are unaffected.

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

- **BREAKING (`/core`)** `CommandPlan` is now a discriminated union: a
  normal plan has `args`, and a command queued inside MULTI whose own argument
  parsing failed has `args: undefined` and a `deferredError`, the error its
  EXEC slot answers, raised after the policy chain. A custom
  `ExecutionPolicy` that reads `plan.args` must check `plan.deferredError`
  first. With a typed `CommandPlan<TArgs>` TypeScript narrows `args` on that
  check, but a policy that casts the untyped plan's args (`(plan.args as
  Foo).x`) still compiles without it and fails at runtime. That plan's `keys`
  are the ones Redis would route by. `CommandCapabilities.clusterMode` gained a third value,
  `'multiDbOnly'` (MOVE: refused in a cluster unless it has databases, as a
  Valkey 9 cluster does), so an exhaustive `switch` over it stops
  type-checking; `'forbidden'` still means refused outright.

### Added

- **`/core`** A `CommandDefinition` can declare `rawKeys(argv)`, its getkeys
  procedure: the keys it finds in the raw arguments, each with its flags
  (`KeyWithFlags`, now exported), for commands whose keys the key specs or
  the legacy key range cannot express. It routes a queued command whose
  parser failed and answers `COMMAND GETKEYS` / `GETKEYSANDFLAGS` when the
  key specs cannot.

- `PubSubKind` (`'channel' | 'shard' | 'pattern'`) is exported from `/core`,
  because it appears in the signature of the published `RedisClientSession`
  interface and declaration emit requires it ([#376]).

- `createNodeRedisMock()` takes node-redis' `RESP: 2 | 3` client option, for
  the standalone and the cluster facade ([#489]). `NodeRedisMockClient`'s
  `duplicate()` copies it, and, like node-redis' `duplicate(overrides)`,
  takes `{ RESP }` to override it (an explicit `RESP: undefined` falls back
  to node-redis' default). The facade also gains
  `zRangeWithScores(key, min, max, options)`, which returns node-redis'
  `{ value, score }` members at both protocols ([#488]). New exported types:
  `NodeRedisMockClientOptions`, `NodeRedisRespVersion`,
  `NodeRedisZRangeOptions`.

### Fixed

- From 7.0 (and on Valkey) `EVAL`, `EVALSHA`, `EVAL_RO` and `EVALSHA_RO` run
  a script that opens with a `#!lua [flags=...]` shebang ([#536]). It used to
  be a compile error near `#`. The engine does not skip the shebang yet
  (fatal10110/lua-redis-wasm#98, tracked in [#540]), so the server hands it
  the body with the shebang line blanked: line numbers still count that line,
  and errors name the SHA of the script as sent. `EVAL` and `SCRIPT LOAD`
  check the shebang with Redis's errors and cache nothing when it is refused:
  `Invalid script shebang`, `Invalid engine in script shebang`, `Unexpected
  engine in script shebang: #!notlua`, `Unknown lua shebang option: name=x`
  and `Unexpected flag in script shebang: bogus`. Valkey 8.1+ reads the
  options first and looks the engine up by name, ignoring case, so `#!LUA`
  runs there and `#!notlua` is `Could not find scripting engine 'notlua'`
  (new gate `script.shebang-engine-lookup`). The flags are checked as in
  Redis's `scriptPrepareForRun`. `no-writes` refuses writes like `EVAL_RO`.
  `EVAL_RO` / `EVALSHA_RO` refuse a shebang script without `no-writes`
  (`Can not execute a script with write flag using *_ro command.`), and
  `EVAL_RO` still caches it. `no-cluster` refuses to run on a cluster node
  (`Can not run script on cluster, 'no-cluster' flag is set.`), for a
  function's `no-cluster` flag too. A function flagged `no-writes` now runs
  read-only under `FCALL` as well, not only under `FCALL_RO`. Known gaps:
  `allow-oom`, `allow-stale` and `allow-cross-slot-keys` are accepted but
  change nothing (no `maxmemory`, no stale replicas, no per-script
  cross-slot check), and a `no-writes` script is still routed as a write
  command. Checked against redis-server 7.0.15 and the transcripts in the
  issue. `/core`: `renderScriptError` takes an optional `sha` to name.

- `FUNCTION LOAD` compiles the library before registering it ([#538]). A
  library that is not valid Lua replies `-ERR Error compiling function:
  user_function:<line>: <Lua's message>`, counting the metadata line, and
  registers nothing; under `REPLACE` the old library stays. It used to reply
  `No functions registered`, or register the library. Errors now come in
  Redis's order: missing metadata, then `Library '<name>' already exists`
  (before the code is compiled), then the compile error, then `No functions
  registered`. Code that does not open with `#!` is `Missing library
  metadata`, as in Redis, even if a later line holds the metadata. Checked
  against redis-server 7.0.15. Known gap: the library is not run while it
  loads, so a load-time error (`Error registering functions: ...`) is not
  reproduced.

- A Lua engine that faulted is replaced ([#539]). Since lua-redis-wasm 2.0
  an exception that escapes the WASM module (a `WasmFault`, an Emscripten
  abort, a trap) leaves the engine unusable, and every later `EVAL`,
  `EVALSHA`, `EVAL_RO`, `SCRIPT LOAD`, `FUNCTION LOAD` and `FCALL` on that
  server replied `-ERR LuaEngine is unusable: ...` until the process
  restarted. The command that hits the fault still gets an `ERR` reply; the
  next script command gets a fresh runtime, with the script cache and the
  function libraries kept. A runtime that failed to load is retried too.
  `/core`: `RedisLuaRuntime` gained `usable` and `dispose()`.

- The node-redis facade's `zRange(key, min, max, options)` no longer ignores
  its options ([#488]). It used to drop `BY`, `REV` and `LIMIT` and run a
  plain index range, so `zRange(key, 0, -1, { REV: true })` came back in
  ascending order and `zRange(key, 5, 2, { BY: 'SCORE', REV: true })` came
  back empty. It now builds the ZRANGE the way node-redis does: numeric bounds
  spelled as Redis parses them (`Infinity` becomes `+inf`), then `BYSCORE` /
  `BYLEX`, `REV` and `LIMIT offset count`. The replies match real node-redis
  against real Redis, including the server's errors (`LIMIT` without `BY`).

- The node-redis facade's pub/sub session now runs at the client's protocol
  ([#489]). It used to stay on RESP2 even after `HELLO 3` on the client, so
  `CLIENT LIST` reported the subscriber as `resp=2` where real node-redis
  reports `resp=3`. It also follows a `HELLO` sent while subscribed, straight
  away, as a RESP3 node-redis client's single connection does.

- Stream consumer-group error paths follow real Redis' order ([#507]).
  `XINFO STREAM` reads its `FULL [COUNT n]` options after the key, so a
  missing key is `no such key` (a string key `WRONGTYPE`) before any option
  error. `XGROUP CREATE` / `SETID` read their options, then check the key and
  group, then the argument count, then the id. Every `XGROUP` subcommand
  except `CREATE ... MKSTREAM` answers a missing key with `The XGROUP
  subcommand requires the key to exist...`, where `SETID`, `CREATECONSUMER`
  and `DELCONSUMER` used to answer NOGROUP and `DESTROY` `:0`. A missing
  group is `NOGROUP No such consumer group '<g>' for key name '<k>'`, XGROUP's
  own wording, which `XINFO CONSUMERS` now uses too. `ENTRIESREAD -1` is accepted (the "unknown" counter), a lower
  value is `value for ENTRIESREAD must be positive or -1`, and `SETID`
  takes `-` and `+` as ids. `XINFO STREAM ... FULL COUNT` treats a negative
  count as the default 10 and 0 as no limit, and `FULL` lists `entries`
  before `groups`.

- `XCLAIM` ignores a `LASTID` behind the group's last-delivered id instead of
  moving the group back, and `FORCE` adds a missing entry to the PEL as one
  delivery that the min-idle-time does not apply to (it used to start at 0
  deliveries and be skipped by any min-idle-time) ([#498]). `XAUTOCLAIM`
  examines at most `COUNT * 10` pending entries, counts deleted entries
  towards `COUNT` from 7.0, and returns the pending id after the last one
  examined as its cursor, as Redis does.

- The `redis-6.2` and `redis-7.0` profiles model three more version deltas
  ([#498], [#507]). New gate `stream.consumer-group-lag` (7.0): 6.2 has no
  `ENTRIESREAD` (its `XGROUP CREATE` takes exactly `MKSTREAM`, checked before
  the key), no `entries-read` / `lag` in `XINFO GROUPS` or `XINFO STREAM
  FULL`, and no `max-deleted-entry-id` / `entries-added` /
  `recorded-first-entry-id` in `XINFO STREAM`. The existing
  `stream.xautoclaim-deleted-ids` gate (7.0) now also covers `XCLAIM`: on 6.2
  it claims a deleted entry, replies nil for it and keeps it pending, and
  `XAUTOCLAIM COUNT` goes up to `LONG_MAX`, with `COUNT * 10` wrapping as a
  64-bit signed value, as in 6.2. New gate
  `stream.consumer-active-time` (7.2): before it `XINFO CONSUMERS` has no
  `inactive`, and `XREADGROUP` / `XCLAIM` / `XAUTOCLAIM` create a missing
  consumer only once they deliver or claim something (an `XREADGROUP`
  history read creates it even when nothing is pending).

- Keyspace notifications for writes that set a TTL, and for `MOVE` and
  `COPY … DB`, now match real Redis ([#380], [#445]). The sequences were checked
  on Redis 6.2, 7.0, 7.2, 7.4 and 8.0 and on Valkey 7.2, 8.0 and 9.0, and are
  the same on all of them except the past-deadline row (see below). With
  `notify-keyspace-events KEA`, keyevent channel shown (the keyspace channel
  carries the same event names):

  ```
  SET k v EX 100 / SETEX / PSETEX / SET ... PX|EXAT|PXAT
    real:   set k | expire k        before: set k
  GETEX k EX 100 (also PX / EXAT / PXAT)
    real:   expire k                before: getex k
  GETEX k EXAT 1 (a time already past; Redis 6.2-8.0, Valkey 7.2-8.0)
    real:   del k                   before: getex k, then expired k on access
  SELECT 1; SET k v; MOVE k 0
    real:   @1 move_from k | @0 move_to k        before: @1 del k
  SELECT 1; SET k v; COPY k c DB 0
    real:   @0 copy_to c                         before: (nothing)
  ```

  `SET ... KEEPTTL` publishes no `expire`, and neither does a TTL that
  `RENAME`, `MOVE` or `COPY` carries over. `expire`, `move_from`, `move_to` and
  `copy_to` belong to the generic class (`g`). `COPY … DB` naming the selected
  database publishes `copy_to` as well; before, it published nothing.
  `RedisDatabase.set` takes a new `SetOptions.expireEvent` flag, which follows
  the write with an `expire` mutation. On Valkey 8.0 and 9.0, a deadline that is
  already past behaves differently (Valkey 9.0 publishes `expired k` for the
  `GETEX` row). That is tracked in [#527].

- `SET … EX|PX` and `GETEX … EX|PX` queued in `MULTI` count the TTL from
  `EXEC`, as real Redis does, not from the moment they were queued. Before, a
  transaction run later than the TTL made `SET` publish `expired` and lose the
  key at once, and made `GETEX` publish `del` and delete it. Only `GETEX …
  EXAT|PXAT` with a time already past deletes the key.

- A script that is not valid Lua replies `-ERR Error compiling script (new
  function): user_script:1: unexpected symbol near '+'` on every profile, as
  Redis does, instead of a runtime error with the abort decoration ([#502]).
  `SCRIPT LOAD` compiles the script first and refuses invalid Lua with the
  same error (it returned a SHA), and neither `EVAL` nor `SCRIPT LOAD` caches
  a script that does not compile. From 7.0 (and on Valkey) the check skips a
  leading `#!lua` shebang line, keeping its line feed, so `SCRIPT LOAD
  "#!lua\nreturn 1"` still returns the SHA (new gate `script.shebang`; the
  shebang itself and `EVAL` of a shebang script are covered by the [#536]
  entry above). `/core`: `RedisLuaRuntime.compile(script)` returns
  the compile error or `null`.

- On `redis-6.2` a `redis.pcall` rejected by the scripting layer (an unknown
  or not-allowed command, wrong arity, no command) carries the calling line,
  `-@user_script: 2: Unknown Redis command called from Lua script`, and
  `=[C]: -1: ` when called through the global `pcall`, as in Redis 6.2
  ([#503], [#492]).

- Scripts have the `redis.*` members the engine leaves to the host
  ([#502]): `REPL_NONE` / `REPL_AOF` / `REPL_SLAVE` / `REPL_REPLICA` /
  `REPL_ALL`, `set_repl()` (a no-op), `replicate_commands()` (`true`) and the
  debugger hooks `breakpoint()` / `debug()` on every version, and from 7.0
  `REDIS_VERSION` / `REDIS_VERSION_NUM` (new gate
  `script.redis-version-props`). Valkey reports `7.2.4` there, like its
  INFO, plus `SERVER_NAME`, `VALKEY_VERSION` and `VALKEY_VERSION_NUM`.
  Known gaps ([#540]): `set_repl` does not check its argument, and on 6.2
  `replicate_commands()` returns `true` even after the script has written,
  where Redis 6.2 returns `false`. `acl_check_cmd` (7.0+) is not provided.

- A Valkey 7.2 profile (`{ flavor: 'valkey', version: '7.2.x' }`) gets
  Valkey 7.2's Lua sandbox: Redis 7.2's (no `os`, `Invalid debug level.`,
  `Lua redis lib command arguments ...`) with the `server` alias. It used to
  get Valkey 8.0's.

- `EVALSHA` of an uncached script replies `-NOSCRIPT No matching script.` on
  Valkey 8.0+, which dropped ` Please use EVAL.` (new gate
  `script.noscript-short-wording`).

- `COMMAND` / `COMMAND INFO` report every command's flags, ACL categories,
  tips and key specs as the real server of the profile's version does
  ([#494]). They come from a command table captured from redis-server
  6.2.24, 7.0.15, 7.2.4, 7.4.4, 8.0.6 and valkey 8.0.11 / 9.0.6
  (`src/core/compatibility/command-table.ts`, regenerated by
  `scripts/capture-command-table.ts`); most commands used to answer flags
  and categories guessed from their behaviour flags (`PING`: `readonly fast
  subscribed`, `@read @fast`; real: `fast`, `@fast @connection`), no tips and
  no key specs. The reply types match too: flags, categories and key-spec
  flags are status strings in sets (they were bulk strings in arrays), tips
  a set, key specs maps, and an entry without subcommands ends in an empty
  set on Redis and an empty array on Valkey; RESP2 output changes only from
  bulk to status strings. A command the real table does not know (for
  example one added with `extraCommands` under a new name) still reports what
  its `introspection` declares. The cluster-mode commands (`CLUSTER` and its
  subcommands, `READONLY`, `READWRITE`) read the table too (`READONLY`:
  `loading stale fast`, `@fast @connection`; it was `readonly fast`, `@read
  @fast`). The metadata is the captured patch release's: the `redis-6.2`
  preset (6.2.14) reports 6.2.24's `denyoom` on `SUBSCRIBE` / `PSUBSCRIBE`,
  and the `valkey-8.0` / `valkey-9.0` presets (8.0.0 / 9.0.0) answer as
  8.0.11 / 9.0.6, whose entries end in `*0` where 8.0.0, 9.0.0 and 9.0.1
  still send `~0`.
- `COMMAND GETKEYS` / `GETKEYSANDFLAGS` find keys through those real key
  specs ([#493], [#494]), so a key's flags are its real spec's (`LPUSH k v`:
  `RW insert`, it was `RW access update`) and a command is no longer parsed
  to find its keys. `BITFIELD` and `SORT_RO` gained Redis's getkeys
  procedures, which their specs defer to (`BITFIELD k GET u8 0` is `RO
  access`).
- On the `redis-6.2` profile `COMMAND` behaves like 6.2's ([#494]): no `QUIT`
  entry (`COMMAND INFO quit` is nil, `COMMAND GETKEYS QUIT` an invalid
  command), no `COMMAND LIST`, a bare `COMMAND INFO` answers an empty array,
  and a wrong argument count (`COMMAND GETKEYS`, `COMMAND COUNT x`) answers
  `Unknown subcommand or wrong number of arguments for 'GETKEYS'. Try
  COMMAND HELP.` instead of the 7.0 per-subcommand arity error.
- `COMMAND HELP` returns real Redis's text as status lines, per profile: 6.2's
  shorter list, `Print this help.` from 7.2, and no "Redis" on Valkey 8.0+.
  It used to be bulk strings with a LIST line on every profile. New gates
  `command.list`, `command.help-valkey-wording` and
  `command.info-subcommands-array`.

- Importing the package no longer throws in a browser ([#499]).
  `src/core/clock.ts` called `process.hrtime.bigint()` at module load, and
  browser `process` polyfills have no `hrtime`. The clock now uses
  `process.hrtime.bigint()` when it exists and falls back to
  `performance.now()`, then `Date.now()`. MONITOR timestamps in Node still
  have microsecond resolution. The browser demo's `process-shim.ts`, which
  patched `hrtime` in for the demo only, is gone.

- `HSCAN ... NOVALUES` is accepted on the `valkey-8.0` profile ([#214]). The
  `hscan.novalues` gate said Valkey 9.0, but valkey-server 8.0.0 already
  takes it (7.2.14 refuses it). Before the gate (Redis 6.2 - 7.2), `NOVALUES`
  on SCAN / SSCAN / ZSCAN is now `syntax error`, the same as any unknown
  option. It used to be `NOVALUES option can only be used in HSCAN`, which
  those versions never send.

- HSCAN / SSCAN / ZSCAN parse their options after the key lookup, as Redis
  does ([#214]). A missing key answers the empty scan reply whatever follows
  the cursor (`HSCAN missing 0 COUNT 0`, `SSCAN missing 0 NOVALUES`), and a
  key of the wrong type answers WRONGTYPE before any option error. The cursor
  is still checked first. An option missing its value (`HSCAN h 0 MATCH`,
  `SCAN 0 COUNT`) is `syntax error` instead of `wrong number of arguments`.

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
  6.2.24. A `redis.pcall` rejection gets the same position as an error reply
  ([#503]). Only an argument count the command
  table rejects gets the scripting layer's arity error, and it now comes
  before the noscript / read-only checks, as in Redis. A count the table
  accepts but the command refuses (`HSET h f v x`, an odd `MSET`) returns the
  command's own error, `ERR` code included, on every profile ([#492]). On
  `redis-6.2` that error reads `wrong number of arguments for MSET` for
  `MSET` / `MSETNX` and `wrong number of arguments for XADD` for `XADD` with
  an odd or empty field/value tail (`XADD s MAXLEN 10 *`) (new gate
  `error.odd-pairs-arity-wording`).

- A script call whose argument count the command table rejects answers the
  scripting layer's arity error on Redis 7.0+ and Valkey too: `Wrong number of
  args calling Redis command from script`, without `Redis` on Valkey 8.0+
  ([#492]). It used to pass the command's own `wrong number of arguments for
  '<cmd>' command` through. Valkey 8.0+ also words the other script-level
  errors its own way (`Please specify at least one argument for this call`,
  `Command arguments must be strings or integers`), and Valkey 9.0 refuses a
  `noscript` command with `This Valkey command is not allowed from script`
  (new gate `script.not-allowed-valkey-wording`).

- MULTI queues a command whose own argument checks fail (`MSET a b c`,
  `HSET h f v x`, `SET k v BOGUS`, `INCRBY k x`, ...) and reports the error in
  that command's slot of EXEC's reply, as Redis does; the rest of the
  transaction runs. Only an unknown command or subcommand, or an argument
  count the command table rejects, is still refused at queue time and aborts
  EXEC with `-EXECABORT`. Previously every parse error aborted the
  transaction.

  The command-table arity is checked at lookup for every caller (a client,
  MULTI, a script), as Redis's `processCommand` does, against the entry lookup
  resolves to: from 7.0 the subcommand's own entry for every container
  (`CLIENT REPLY` is `wrong number of arguments for 'client|reply' command`,
  `FUNCTION DUMP x` is `... for 'function|dump' command`; also real
  subcommands this server does not implement, such as `client|pause`),
  otherwise the command's own (`XREAD COUNT` is `wrong number of arguments for
  'xread' command` before XREAD's option parser runs). 6.2 has no subcommand
  entries, so there the check is always against the command's own entry; it
  now runs at lookup on 6.2 too, with the same replies as before.

  What clients see changes accordingly:

  ```
  MULTI; SET a 1; MSET b v c; EXEC
    ioredis     multi().exec() used to reject with EXECABORT; it now resolves
                [[null, 'OK'], [SimpleError("ERR wrong number of arguments
                for 'mset' command"), null]]
    node-redis  multi().exec() used to reject with EXECABORT; it now rejects
                with MultiErrorReply, errorIndexes [1], .replies holding the
                OK and the error
  ```

  In a cluster such a command is routed by the keys Redis's
  `getKeysFromCommand` finds in the raw arguments, without running the
  command's parser: the command's getkeys procedure where Redis has one
  (numkeys commands read the count like `atoi`, so `ZUNIONSTORE d 2abc a b` is
  a `CROSSSLOT` and `EVAL s 1x a` a `MOVED`; `XREAD`/`XREADGROUP` find no keys
  for an odd tail or an unknown option, so `XREAD STREAMS a b 0` and
  `XREAD BOGUS STREAMS a 0` are queued on any node and answer their
  `Unbalanced ...` / `syntax error` at EXEC; `GEORADIUS*` add the `STORE`
  destination), otherwise the legacy first/last/step key range of the entry
  lookup resolved (from 7.0 `xinfo|stream`, so `XINFO STREAM a x` is a
  `MOVED`; on 6.2 the container's own 2,2,1). A numkeys past the end of the
  command (`ZUNIONSTORE a 5 b`) leaves it keyless. `SELECT 1` inside a
  cluster MULTI is queued too, and its `SELECT is not allowed in cluster
  mode` fills its EXEC slot; `SELECT x` answers `value is not an integer or
  out of range` there instead of dropping the connection. `MOVE` is queued
  the same way and answers `MOVE is not allowed in cluster mode` at EXEC.

  `XREAD` / `XREADGROUP` parse their options as Redis does: `syntax error`
  for an unknown option, `value is not an integer or out of range` for a bad
  `COUNT`, the `timeout` errors for a bad `BLOCK`, the `NOACK` error from
  `XREAD`, and the `Unbalanced ...` wording of the emulated version (new gates
  `stream.xread-unbalanced-wording`, and `stream.xread-unbalanced-plus-wording`
  for the `'+'` XREAD's error lists from Redis 8.0.0; 7.4 accepts `+` but
  does not list it).

  `COMMAND INFO` now reports Redis's key specs for the movable-key commands
  (numkeys: `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFFSTORE`, `ZUNION`/`ZINTER`/
  `ZDIFF`/`ZINTERCARD`, `SINTERCARD`, `LMPOP`/`BLMPOP`/`ZMPOP`/`BZMPOP`,
  `EVAL`/`EVALSHA`/`EVAL_RO`/`EVALSHA_RO`/`FCALL`/`FCALL_RO`; keyword:
  `XREAD`/`XREADGROUP` `STREAMS`, `GEORADIUS`/`GEORADIUSBYMEMBER`
  `STORE`/`STOREDIST`, whose destination specs carry `variable_flags` on
  Valkey 8.0+, new gate `geo.store-keyspec-variable-flags`). From 7.0 the
  `XINFO` / `XGROUP` containers are bare entries whose subcommand entries
  (`xinfo|consumers` with its `nondeterministic_output` tip) carry the
  details; on 6.2 they keep their single entry (`XINFO`: `readonly`,
  `random`, keys 2,2,1, `@read @stream @slow`; `XGROUP`: `write`, `denyoom`)
  and no container lists subcommand entries. `COMMAND COUNT` counts
  command-table entries, not subcommands. `COMMAND DOCS` summaries for the
  stream containers follow the version's wording (7.2+ ends them with a
  period, new gate `docs.summary-7.2-wording`). On `redis-6.2` each
  `COMMAND INFO` entry has 6.2's 7 fields, ending with the ACL categories,
  instead of also carrying tips, key specs and subcommands (new gate
  `command.info-extended-fields`).

- `COMMAND GETKEYS` / `GETKEYSANDFLAGS` find keys the way Redis does,
  without running the command: they look it up (from 7.0 an unknown
  subcommand is `Invalid command specified`), then answer `The command has no
  key arguments` for an entry without keys before checking its arity
  (`COMMAND GETKEYS CLIENT REPLY`, `CONFIG GET`, `CLIENT KILL`; from 7.0 per
  subcommand, so `XINFO HELP` too), then `Invalid number of arguments
  specified for command` for a count the entry's table arity rejects. From
  7.0 the key specs find the keys, and the command's getkeys procedure when a
  spec cannot be applied or is `variable_flags` (`XREAD STREAMS a b 0` is
  `a`, `ZUNIONSTORE d 2abc a b` is `a b d`, `EVAL s 2 a` an empty list); 6.2
  asks the getkeys procedure or the legacy key range, and answers `Invalid
  arguments specified for command` when they find nothing.
  `GETKEYSANDFLAGS` reports each key with the flags of the key spec that
  found it (`ZUNIONSTORE d 2 a b`: `d` `OW update`, `a`/`b` `RO access`),
  of a container subcommand's own entry (`XGROUP CREATE`: `RW insert`,
  `XGROUP DESTROY`: `RW delete`), or of the getkeys procedure (`SET k v`:
  `OW update`, `SET k v GET`: `RW access update`; none for a numkeys
  command); each key's flags are a RESP3 set. On Valkey, whose `GEORADIUS`
  `STORE` / `STOREDIST` specs are `variable_flags`, only the last
  destination is reported, as its procedure finds it. `SPUBLISH`,
  `SSUBSCRIBE` and `SUNSUBSCRIBE` declare Redis's `not_key` channel spec, so
  they answer `The command has no key arguments` (`GETKEYS SPUBLISH ch msg`
  used to return `ch`). A command whose keys come only from its `keys(args)`
  (one added with `extraCommands`, with no key specs, getkeys procedure or
  key positions in its schema) is answered from its parsed keys, and has no
  key arguments when there are none or the call does not parse.

- `GEORADIUS` / `GEORADIUSBYMEMBER` with several `STORE` / `STOREDIST`
  options store into the last one, as that kind, and a cluster routes the
  command by that destination, as Redis does; the first one used to win.

- In a Valkey 9 cluster, which has databases, `MOVE` runs (a single-database
  node answers `DB index is out of range`) instead of being refused with
  `MOVE is not allowed in cluster mode`.

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
  also read once rather than twice.

- Sets are read in the order Redis stores them ([#504]). A set of integers is
  an intset, kept sorted, until it gains a non-integer or grows past
  `set-max-intset-entries`, and it never becomes an intset again. A small
  intset that gains a non-integer keeps its integers sorted ahead of the
  later members. So `SADD s 3 1` reads `1 3`, `SADD s 3 1 a` reads `1 3 a`
  and `SADD s a 3 1` then `SREM s a` reads `3 1` in `SMEMBERS`, `SSCAN`,
  `SORT ... BY nosort` and `SRANDMEMBER` with a count that covers the set;
  these used to follow insertion order, and SORT re-derived the order from
  the current members. (An intset that grows past the limit, or gains a
  non-integer at 128+ members, is a hashtable in Redis, whose order is
  undefined; the mock keeps the order above there.) The set commands build
  their results the way Redis does: `SINTER` walks the smallest set,
  `SINTERSTORE` stores an all-integer result as an intset, `SUNION`, `SDIFF`
  and their `STORE` forms build the result from an empty intset (and `SDIFF`
  picks between Redis's two algorithms, which can leave different orders),
  `SPOP` with a count that covers the set replies an `SUNION` of the key,
  `SMOVE` creates its destination like `SADD`, `COPY` keeps the encoding and
  seeding a set works like `SADD`. On the `redis-8.0` / Valkey profiles (new
  gate `set.union-diff-hashtable`) a non-`STORE` `SUNION` / `SDIFF` with a
  non-intset source keeps the order it walks the sources in, as Redis builds
  that result as a hashtable: after `SADD u x 3 1` and `SREM u x`,
  `SUNION u` replies `3 1` there (Valkey 9's order) and `1 3` on 7.4 and
  earlier. `SRANDMEMBER` and `SPOP` with a smaller count now reply in storage
  order, which is the order Redis replies a small set's sample in. On the
  `redis-7.2+` / Valkey profiles (new gate `set.listpack-encoding`) `SADD`
  creates an intset only when its member count fits
  `set-max-intset-entries`; on 6.2 / 7.0 a large `SPOP` that keeps only
  integers turns the survivors into an intset.

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

- `INCRBYFLOAT` and `HINCRBYFLOAT` parse their increment and the stored value
  the way Redis's `string2ld()` (C `strtold`) does ([#234]), the same on every
  Redis and Valkey version:
  - C99 hex floats are valid: `INCRBYFLOAT k 0x10` on `1` answers `17`, and
    `0x1.8p3`, `-0x.8` and `0x1e5` (485) work too. `0b11` and `0o7` are still
    invalid floats.
  - A token of 5120 bytes or more is refused, and so is a nonzero value that
    underflows an 80-bit `long double` to zero (`1e-4952`, `0x1p-16446`).
    `1e-4950` is still accepted.
  - `INCRBYFLOAT` checks the key's type before it parses the increment, so a
    hash key with a bad increment answers `WRONGTYPE`, not `value is not a
    valid float`.
  - `HINCRBYFLOAT` with an infinite increment answers `ERR value is NaN or
    Infinity`. It used to answer `value is not a valid float`. A stored `inf`
    is now a valid operand, so the sum fails with `ERR increment would
    produce NaN or Infinity`. That is also the error for an infinite sum,
    which used to be `hash value is not a float`.
  - Known limit: values past the `double` range but inside the `long double`
    range (`1e400`, `0x1p1024`) are still refused, because the arithmetic is
    `double` ([#512]).

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
[#488]: https://github.com/fatal10110/js-redis-server/issues/488
[#489]: https://github.com/fatal10110/js-redis-server/issues/489
[#449]: https://github.com/fatal10110/js-redis-server/issues/449
[#492]: https://github.com/fatal10110/js-redis-server/issues/492
[#502]: https://github.com/fatal10110/js-redis-server/issues/502
[#503]: https://github.com/fatal10110/js-redis-server/issues/503
[#234]: https://github.com/fatal10110/js-redis-server/issues/234
[#512]: https://github.com/fatal10110/js-redis-server/issues/512
[#504]: https://github.com/fatal10110/js-redis-server/issues/504
[#498]: https://github.com/fatal10110/js-redis-server/issues/498
[#507]: https://github.com/fatal10110/js-redis-server/issues/507
[#380]: https://github.com/fatal10110/js-redis-server/issues/380
[#445]: https://github.com/fatal10110/js-redis-server/issues/445
[#527]: https://github.com/fatal10110/js-redis-server/issues/527
[#536]: https://github.com/fatal10110/js-redis-server/issues/536
[#538]: https://github.com/fatal10110/js-redis-server/issues/538
[#539]: https://github.com/fatal10110/js-redis-server/issues/539
[#540]: https://github.com/fatal10110/js-redis-server/issues/540
[#493]: https://github.com/fatal10110/js-redis-server/issues/493
[#494]: https://github.com/fatal10110/js-redis-server/issues/494
[#499]: https://github.com/fatal10110/js-redis-server/issues/499
[#214]: https://github.com/fatal10110/js-redis-server/issues/214
[unreleased]: https://github.com/fatal10110/js-redis-server/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/fatal10110/js-redis-server/releases/tag/v0.3.0
