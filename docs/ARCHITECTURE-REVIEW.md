# Architecture Review — js-redis-server (new core)

Single source of truth for the `codex/redis-architecture-refactor` review.
Supersedes and consolidates the prior slice reviews (Phase 1, Phase 2/3/4),
the refactor findings, and the SCAN review notes.

- Last updated: 2026-06-07
- Reviewed against: [ARCHITECTURE-REFACTOR-PLAN.md](./ARCHITECTURE-REFACTOR-PLAN.md)
- Command coverage tracked separately in [COMMANDS.md](./COMMANDS.md)
- Test harness documented in [TEST-INTEGRATION.md](./TEST-INTEGRATION.md)

> Scope: review only. No production code changed by this review.

---

## 1. Status

| Check                                                 | Result                                   |
| ----------------------------------------------------- | ---------------------------------------- |
| Typecheck (`tsc --noEmit`)                            | ✅ clean                                 |
| Lint (`npm run lint`)                                 | ✅ 0 errors / 1 declaration-file warning |
| Unit tests (`npm test`)                               | ✅ 92 pass / 0 fail (13 suites)          |
| Integration, mock backend (new server + real clients) | ✅ 111 pass / 0 fail                     |
| Integration, real Redis backend                       | ✅ 111 pass / 0 fail                     |

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

### P2 — Replica nodes broken (latent: only when `replicasPerMaster > 0`)

`src/cluster.ts:100-111` gives each replica its own **empty**
`RedisServerState`, no replication, and shares the master's `slots` array by
reference. So `nodeOwnsSlot(replica, slot)` is true →
`src/core/execution-policies/cluster-policy.ts:87` accepts **reads and writes
locally** instead of MOVED-ing to master. No READONLY handling.

- Read from replica → empty/wrong. Write to replica → silent divergence.
- Tests use masters-only (default replicas 0) → never exercised.

Fix: replica must not own slots for writes (MOVED to master), gate reads behind
READONLY, or implement real replication.

### P3 — RESP3 handshake lies

`src/commands/connection.ts:382-391`: HELLO echoes `proto = requested version`.
But the encoder throws on v3 (`src/core/resp-encoder.ts:22`), the adapter
encoder is fixed at RESP2, and the session stores no version. `HELLO 3` →
reply claims `proto:3` while the server keeps speaking RESP2 → a RESP3 client
desyncs. Latent (test clients use RESP2).

Fix: reject `HELLO 3` with `NOPROTO` until a RESP3 encoder exists, OR implement
RESP3 + a per-session version threaded into the encoder.

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
- **Glob `[a-]` trailing dash** treated as a range (`scan.ts:414-431`); Redis
  treats a trailing `-` as literal. Edge case.
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
| 4. Port commands           | ⚠️ Partial | ~115 commands incl. scan family + handshake. Missing: pub/sub, blocking, streams.         |
| 5. Cleanup (delete legacy) | ✅ Done    | See resolved P1.                                                                          |

**Not ported (Phase 4):** Pub/Sub (`RedisPubSubBroker` exists, unwired),
blocking `BLPOP/BRPOP/WAIT` (park infra exists, no command), streams
(placeholder type). `ResponseStream` + `pushOnly` capability scaffolded but
unused. RESP3 encoding absent (see P3).

---

## 5. Minor / nits

- `createNoopParkHandler` returns the default handler — misnomer
  (`src/core/redis-context.ts`).
- `SessionDirective` union + `'subscribed'` session mode declared, never used.
- `reset` resets db/watch/tx but not protocol version (no version state to
  reset).

---

## 6. Suggested priority

1. **P3 HELLO-3 reject** — one-line correctness, cheap.
2. **P4 perf** — keys-only snapshot; skip the clone for dirty-only watchers.
3. **P2 replica semantics, P5, and Phase-4 features** (pub/sub, blocking,
   streams, RESP3) — larger efforts.
