# Architecture Review — js-redis-server (new core)

Single source of truth for the `codex/redis-architecture-refactor` review.
Supersedes and consolidates the prior slice reviews (Phase 1, Phase 2/3/4),
the refactor findings, and the SCAN review notes.

- Last updated: 2026-06-20
- Reviewed against: [ARCHITECTURE-REFACTOR-PLAN.md](./ARCHITECTURE-REFACTOR-PLAN.md)
- Command coverage tracked separately in [COMMANDS.md](./COMMANDS.md)
- Test harness documented in [TEST-INTEGRATION.md](./TEST-INTEGRATION.md)

> Scope: review only. No production code changed by this review.

> **Status refresh (2026-06-20):** the architecture cutover is complete and the
> originally-deferred features have landed. Pub/Sub, MONITOR, the full stream
> command set, blocking commands (`BLPOP`/`BRPOP`/`BLMOVE`/`BLMPOP`/`BZPOPMIN`/
> `BZPOPMAX`/`BZMPOP`/`XREAD BLOCK`), and `COMMAND` are all implemented and
> wired. The phase scorecard (§4) and remaining-gaps checklist (§6) below have
> been updated accordingly. The §1 test counts and §3 findings are preserved as
> the dated snapshot from the original review.

---

## 1. Status

_Snapshot from the 2026-06-07 review run; counts grow as commands land. Run
`npm run test:all` for current figures._

| Check                                                 | Result                                   |
| ----------------------------------------------------- | ---------------------------------------- |
| Typecheck (`tsc --noEmit`)                            | ✅ clean                                 |
| Lint (`npm run lint`)                                 | ✅ 0 errors / 1 declaration-file warning |
| Unit tests (`npm test`)                               | ✅ 88 pass / 0 fail (12 suites)          |
| Integration, mock backend (new server + real clients) | ✅ 143 pass / 0 fail                     |
| Integration, real Redis backend                       | ✅ 143 pass / 0 fail                     |

The P0 integration hang reported in the earlier findings is **resolved** by
commit `b390d11` (client-handshake commands: HELLO/CLIENT/INFO/AUTH/RESET/
COMMAND). SCAN/KEYS/HSCAN/SSCAN/ZSCAN have since been added.

---

## 2. Architecture — strengths

The pipeline is clean and the abstractions hold:

- `CommandExecutor`: registry → schema-parse → policies (before/stream/after)
  → `execute` → `RedisResult`. Commands return a value, never touch transport.
  (`src/core/command-executor.ts:57`)
- Sync path `executePlanSync` reuses the **same** policies (incl. cluster
  routing) for Lua `redis.call`, so inner calls cannot bypass routing.
  (`src/core/command-executor.ts:108`)
- Policies composable; MULTI slot-pinning via `WeakMap<session, slot>`.
  (`src/core/execution-policies/cluster-policy.ts:54-68`)
- Per-DB `SerialTurnQueue`; `park` → `suspend` releases the turn then
  re-acquires with priority → blocking-capable without deadlock on the normal
  path. (`src/core/turn-queue.ts:33`, `src/core/client-session.ts:255`)
- Mutation bus clones events; WATCH = per-key subscribe + lazy-evict emits an
  `evict` event. (`src/state/mutation-events.ts:68`)
- State layering clean: `ServerState → Database → Keyspace`; `scriptCache` is
  server-wide and survives `FLUSHDB`. (`src/state/server-state.ts:44-52`)
- Transport decoupled via `ConnectionTransport` (socket + in-memory impls).

### Acceptance criteria — met

Normal command returns `RedisResult` without writing to transport; schema is the
single source of truth for arity/syntax; keys extracted from parsed args; cluster
validation runs for normal **and** transaction commands **and** Lua `redis.call`;
WATCH sees lazy eviction; FLUSHDB vs SCRIPT FLUSH separated; open registry
(`register`/`override`/`registerAll`); one serialization turn per DB; session
`AbortSignal` is the cancellation signal; MULTI/EXEC semantics correct
(watch-dirty → null-array, queue error → EXECABORT, runtime errors → array
elements).

---

## 3. Findings

### P1 — Resolved: Phase-5 cleanup completed

The legacy commander stack and old transport/session machinery were removed:
`src/commanders/custom`, `src/types.ts`, `src/core/errors.ts`,
`src/core/cluster/network.ts`, and the old session/adapter files are gone.
`Logger` now lives in `src/logger.ts`, `Resp2Transport` is no longer public API,
and legacy-targeting unit tests were deleted or migrated to the new core.

Follow-up checks:

- No `src`/unit/integration test references remain for `commanders/custom`,
  `Resp2Transport`, `DBCommandExecutor`, `CommandMetadata`, or `UserFacedError`.
- `tests/command-registry.test.ts` now targets the new open `CommandRegistry`
  API.
- `tests/cluster-network.test.ts` keeps slot-range coverage against the live
  `src/cluster.ts` module.

### P2 — Resolved: direct replica routing no longer diverges

Replica topology entries no longer own slots locally. They still appear under
their master in `CLUSTER SLOTS`/`CLUSTER SHARDS`, but direct keyed reads and
writes to replicas now return `MOVED` to the master instead of reading/writing
an empty independent state.

Coverage:

- `tests-integration/ioredis/cluster-integration.test.ts` starts mock clusters
  with one replica per master and compares direct replica `GET`/`SET` behavior
  against real Redis.
- `tests-integration/test-config.ts` can now host multiple mock cluster shapes
  in one runner, which preserves existing node-redis/ioredis mixed tests.

`READONLY`/`READWRITE` are now implemented as connection-local cluster read
mode. Replica databases receive master mutation events through a replication
link, with a configurable mock delay hook (`replicaUpdateDelayMs`) for future
lag simulation.

### P3 — Resolved: HELLO 3 switches to RESP3 replies

`HELLO 3` now stores protocol version on the client session and the RESP
adapter encodes replies with the session's active protocol. The RESP3 encoder
covers the existing `RedisValue` model (`map`, `set`, `push`, null, bool,
double, big number, verbatim, etc.).

Coverage:

- `tests-integration/ioredis/connection-integration.test.ts` uses a raw TCP
  connection against both backends, sends `HELLO 3`, asserts a RESP3 map reply,
  then asserts `CLIENT GETNAME` returns RESP3 null.

Remaining RESP3 work: the request decoder still accepts the RESP2-compatible
command frames clients normally send; RESP3-specific request frame extensions
are not implemented.

### P4 — Defensive deep-clones on hot paths

- `src/state/mutation-events.ts:97-104` clones the value on every write to every
  listener. The WATCH listener only flips a dirty bit → the cloned value is pure
  waste. A large watched hash = a full clone per write.
- `src/state/keyspace.ts:176-192` `entriesSnapshot` clones the whole DB per
  SCAN/KEYS call, but `src/commands/scan.ts` only reads `key` + `value.type`.
  Full iteration is O(N²) clones. Fix: keys-only snapshot returning
  `{ key, type }`, no value clone.
- Theme: clones bought for "safety" that single-threaded JS does not need on the
  read/notify paths.

### P5 — EXEC park context = latent deadlock

`src/core/client-session.ts:156-159`: queued plans run with
`createExecutionContext()` (no turn access) → `park` uses the default handler
that does not release the outer turn. Moot now (no blocking command is queueable;
`pushOnly` is rejected in MULTI). Becomes real if blocking lands inside MULTI.

### SCAN family — secondary findings

(From the SCAN review; typecheck clean, scan unit 5/5, scan integration 5/5.)

- **Cursor = offset into a freshly-rebuilt snapshot** (`scan.ts:273-302`).
  Add/del between calls shifts offsets → keys skipped or duplicated. Real Redis
  guarantees every key live for the full scan is returned ≥ once. Mock gap; fine
  single-threaded, breaks under concurrent mutation.
- **COUNT applied after MATCH/TYPE filter** (`scan.ts:278-282`). Redis applies
  COUNT to buckets before MATCH, so real Redis can return an empty page with a
  nonzero cursor; this never does. Does not break `while cursor != 0` loops.
- **Glob `[a-]` trailing dash** was checked against real Redis and the earlier
  review note was incorrect: Redis 7.0 treats it as a range from `]` through
  `a`, so it matches `^` and `a` for the current fixture. The implementation
  already matches this, and integration coverage now pins it.
- Verified OK: live ref (no clone) for hscan/sscan/zscan; WRONGTYPE thrown;
  type filter `'zset'` matches TYPE output; cursor/count error messages match;
  huge-cursor termination; itemWidth 1/2 pagination; `['readonly','random']`
  flags.

---

## 4. Phase scorecard

| Phase                      | State      | Notes                                                                                     |
| -------------------------- | ---------- | ----------------------------------------------------------------------------------------- |
| 1. Execution engine (core) | ✅ Done    | RedisResult/Value, CommandDefinition, Executor, schema `t.*`, policies.                   |
| 2. State & database        | ✅ Done    | ServerState/Database/Keyspace, mutation bus, lazy eviction emits events.                  |
| 3. Transport & session     | ✅ Done    | ConnectionTransport (+in-memory/socket), ClientSession, Resp2SessionAdapter, Resp2Server. |
| 4. Port commands           | ✅ Done    | Full surface: scan family, handshake, Pub/Sub, MONITOR, `COMMAND`, blocking, and streams. |
| 5. Cleanup (delete legacy) | ✅ Done    | See resolved P1.                                                                          |

**Phase 4 complete.** Pub/Sub is wired through `RedisPubSubBroker` +
`ResponseStream` (`SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`/`PUBLISH`/`SPUBLISH`/
`PUBSUB`, restricted subscribed-mode session enforced by `subscribed-policy.ts`);
`MONITOR` streams via `RedisMonitorFeed`; the full stream command set (`XADD`…
`XAUTOCLAIM`/`XINFO`) is implemented; blocking commands (`BLPOP`, `BRPOP`,
`BLMOVE`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `XREAD BLOCK`,
`XREADGROUP BLOCK`) run on `ctx.park`; keyspace notifications are bridged via
`KeyspaceNotifier`. `ResponseStream` + `pushOnly` are now exercised in
production paths. Genuine remaining gaps are tracked in §6 and
[COMMANDS.md](./COMMANDS.md) — notably `WAIT`, `OBJECT ENCODING`, `DUMP`/
`RESTORE`, `MIGRATE`, `LCS`, and the `FUNCTION`/`FCALL` family.

---

## 5. Minor / nits

- `createNoopParkHandler` returns the default handler — misnomer
  (`src/core/redis-context.ts`).
- The `'subscribed'` session mode is now live — enforced by
  `src/core/execution-policies/subscribed-policy.ts`, which restricts a
  subscribed connection to pubsub-only commands (earlier review flagged it as
  declared-but-unused; resolved).
- `RESET` now resets db/watch/tx, RESP protocol version, and cluster read mode.

---

## 6. Remaining acceptance checklist

The architecture cutover is done; what remains is incremental Redis
compatibility plus a few deferred robustness items. Distinct from the
**completed** cutover work above, these are the open gaps:

Compatibility gaps (tracked in [COMMANDS.md](./COMMANDS.md)):

- [ ] `WAIT` — accept and return immediately (no-op in a single-node mock).
- [ ] `OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ|HELP`.
- [ ] `DUMP` / `RESTORE` and `MIGRATE`.
- [ ] `LCS` (longest common subsequence).
- [ ] `FUNCTION` / `FCALL` family (Redis Functions, 7.0+).
- [ ] `HSCAN ... NOVALUES` (Redis 7.4).
- [ ] Server-introspection subcommands returning real data: `MEMORY USAGE`,
      `SLOWLOG`, `LATENCY`, `DEBUG OBJECT/SLEEP/RELOAD`.
- [ ] RESP3-specific *request* frame extensions (replies are already RESP3).

Robustness / fidelity (deferred, behavior-compatible today):

- [ ] **P4 perf** — keys-only SCAN snapshot; skip the value clone for
      dirty-only WATCH listeners.
- [ ] **SCAN cursor stability** under concurrent mutation (§3 SCAN findings).
- [ ] **EXEC park-context** (§3 P5) — must be resolved before any blocking
      command becomes queueable inside `MULTI`.

Validation gates:

- [x] Mock-backend integration suite green (run `npm run test:all`).
- [x] Real-Redis backend integration suite green (`TEST_BACKEND=real`).
- [ ] Periodic stale-issue cleanup against the GitHub compatibility tracker so
      this checklist and the board stay in sync.
