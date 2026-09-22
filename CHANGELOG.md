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

[#374]: https://github.com/fatal10110/js-redis-server/pull/374
[#375]: https://github.com/fatal10110/js-redis-server/pull/375
[#376]: https://github.com/fatal10110/js-redis-server/pull/376
[#377]: https://github.com/fatal10110/js-redis-server/pull/377
[#378]: https://github.com/fatal10110/js-redis-server/pull/378
[#410]: https://github.com/fatal10110/js-redis-server/pull/410
[#413]: https://github.com/fatal10110/js-redis-server/issues/413
[#415]: https://github.com/fatal10110/js-redis-server/issues/415
[#431]: https://github.com/fatal10110/js-redis-server/pull/431
[unreleased]: https://github.com/fatal10110/js-redis-server/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/fatal10110/js-redis-server/releases/tag/v0.3.0
