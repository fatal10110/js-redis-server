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

### Added

- `PubSubKind` (`'channel' | 'shard' | 'pattern'`) is exported from `/core`,
  because it appears in the signature of the published `RedisClientSession`
  interface and declaration emit requires it ([#376]).

### Fixed

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

[#359]: https://github.com/fatal10110/js-redis-server/issues/359
[#374]: https://github.com/fatal10110/js-redis-server/pull/374
[#375]: https://github.com/fatal10110/js-redis-server/pull/375
[#376]: https://github.com/fatal10110/js-redis-server/pull/376
[#377]: https://github.com/fatal10110/js-redis-server/pull/377
[#378]: https://github.com/fatal10110/js-redis-server/pull/378
[#410]: https://github.com/fatal10110/js-redis-server/pull/410
[#413]: https://github.com/fatal10110/js-redis-server/issues/413
[unreleased]: https://github.com/fatal10110/js-redis-server/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/fatal10110/js-redis-server/releases/tag/v0.3.0
