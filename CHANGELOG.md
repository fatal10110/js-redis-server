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

### Changed

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

### Added

- `PubSubKind` (`'channel' | 'shard' | 'pattern'`) is exported from `/core`,
  because it appears in the signature of the published `RedisClientSession`
  interface and declaration emit requires it ([#376]).

### Fixed

- A command pipelined behind a multi-channel `SUBSCRIBE` / `PSUBSCRIBE` /
  `SSUBSCRIBE` could have its reply written between the confirmations. They
  now go out as one reply, in Redis's order ([#455]).
- A multi-channel `SUBSCRIBE` queued in `MULTI` replied `Streaming command is
  not allowed in transaction` from `EXEC`. It now runs, and `EXEC` embeds every
  confirmation in its array exactly as Redis does ([#366]).
- `MONITOR` queued in `MULTI` now fails with Redis's `MONITOR isn't allowed for
  DENY BLOCKING client`, and a repeated `MONITOR` gets no reply and does not
  double the feed, as in Redis ([#366]).

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

[#415]: https://github.com/fatal10110/js-redis-server/issues/415
[#431]: https://github.com/fatal10110/js-redis-server/pull/431
[#366]: https://github.com/fatal10110/js-redis-server/issues/366
[#455]: https://github.com/fatal10110/js-redis-server/issues/455
[unreleased]: https://github.com/fatal10110/js-redis-server/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/fatal10110/js-redis-server/releases/tag/v0.3.0
