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
[unreleased]: https://github.com/fatal10110/js-redis-server/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/fatal10110/js-redis-server/releases/tag/v0.3.0
