# Architecture Refactor — Review Findings

Review of the implementation against [ARCHITECTURE-REFACTOR-PLAN.md](./ARCHITECTURE-REFACTOR-PLAN.md).
Date: 2026-06-06. Branch: `codex/redis-architecture-refactor`.

> Scope: review only. No code was changed.

---

## 1. Status Summary

| Check | Result |
|---|---|
| Typecheck (`tsc --noEmit`) | ✅ clean |
| Unit tests (`npm test`) | ✅ 290 pass / 0 fail (80 suites) |
| Integration tests, mock backend (= our new server, real ioredis/node-redis clients) | ❌ **HANG** |

The new core is solid in isolation (unit-tested), but the assembled server does
**not** work end-to-end with real Redis clients. See §2.

---

## 2. P0 — Integration tests hang against the new server

`tests-integration/` mock backend builds our server via `buildRedisCluster()`
(`src/cluster.ts`) and connects real `ioredis` / `node-redis` cluster clients to
it. That suite hangs.

Reproduction (single file, per-test timeout added to fail fast instead of
hanging forever):

```
TEST_BACKEND=mock node --import tsx --no-warnings --test-timeout=15000 \
  --test ./tests-integration/ioredis/string-integration.test.ts
```

Result:

```
not ok 1 - tests-integration/ioredis/string-integration.test.ts
  failureType: 'testTimeoutFailure'
  error: 'test timed out after 15000ms'
# pass 0  # fail 0  # cancelled 1
```

The client never reaches the test body — it stalls during connect/ready-check.
This matches the earlier session observations (integration suite stuck ~8 min,
zero TAP output, single test hangs in isolation).

**Root cause not confirmed.** Candidates, in priority order:

1. **Missing client-handshake commands.** New server registers no `INFO`,
   `CLIENT`, `HELLO`, `COMMAND`, `AUTH`, `RESET` (only `ping`, `quit`,
   `select`). `ioredis` cluster with `enableReadyCheck` issues `INFO` and waits
   for `loading:0`; `node-redis` issues `CLIENT`/`HELLO`. An unknown-command
   error on the ready-check path makes clients retry forever. **Most likely.**
2. Cluster topology handshake — appears OK on inspection: `CLUSTER SLOTS` reads
   `node.port` live and `RedisCluster.listen()` backfills the OS-assigned port
   into `topology.nodes[index].port` (`src/cluster.ts:52-58`). Port advertised
   should be the real bound port. Lower probability but not runtime-verified.
3. RESP framing / decoder edge case in `Resp2SessionAdapter` read loop.

Action: needs a raw-socket / single-node repro to confirm which command the
client blocks on. (A raw probe was attempted but blocked by a tsx/node-22 ESM
quirk in the sandbox; run under node ≥24.)

---

## 3. Phase Completion vs Plan

| Phase | State | Notes |
|---|---|---|
| 1. New execution engine (core) | ✅ Done | `RedisResult`, `RedisValue`, `CommandDefinition`, `CommandPlan`, `CommandExecutor`, schema (`t.*`), policies. |
| 2. State management & database | ✅ Done | `RedisServerState`, `RedisDatabase`, `RedisKeyspace` central mutation APIs, `RedisMutationBus`, lazy eviction emits events. |
| 3. Transport & session | ✅ Done (new path) | `ConnectionTransport`, `SocketConnectionTransport`, `InMemoryConnectionTransport`, `ClientSession`, `Resp2SessionAdapter`, `Resp2Server`. |
| 4. Port commands (hard cutover) | ⚠️ Partial | ~110 data commands ported. Missing families below. |
| 5. Cleanup (delete legacy) | ❌ Not done | Legacy tree intact; see §4. |

Ported command families: strings, keys, hashes, lists, sets, zsets, scripts
(EVAL/EVALSHA/SCRIPT), transactions (MULTI/EXEC/DISCARD/WATCH/UNWATCH),
connection (PING/QUIT/SELECT), CLUSTER (SLOTS/SHARDS/NODES/INFO/MYID).

**Not ported (Phase 4 incomplete):**

- Pub/Sub: `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUBLISH`/`MONITOR`.
- Blocking: `BLPOP`/`BRPOP`/`BRPOPLPUSH`/`WAIT`/`XREAD BLOCK`.
- Client/handshake: `HELLO`/`CLIENT`/`INFO`/`COMMAND`/`AUTH`/`RESET`.
- Scan family: `KEYS`/`SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`.
- Streams (`StreamDataType` placeholder only).

---

## 4. Phase 5 not done — legacy still present and partly wired

The plan calls for a hard cutover: delete the legacy architecture as commands
are ported. None of it is deleted.

- **`src/commanders/custom/**` — 8,617 LOC** still present (old commanders,
  `redis-kernel`, `db.ts`, data-structures, schema, all old command classes).
- **Dead new-core files — 812 LOC**: `src/core/cluster/network.ts`,
  `src/core/transports/resp2/adapter.ts` (`RespAdapter`),
  `src/core/transports/session.ts` (old `Session`),
  `src/core/transports/session-types.ts`,
  `src/core/transports/transaction-state.ts`,
  `src/core/transports/capturing-transport.ts`.
- **All of it is compiled and shipped.** `tsconfig.json` has
  `"include": ["src/**/*.ts"]`, so ~9.4k LOC of dead code lands in `dist/` and
  is type-checked on every `npm test`.

### 4.1 New core still imports legacy (blocks deletion)

| New/live file | Legacy import |
|---|---|
| `src/types.ts` | `CommandMetadata` from `commanders/custom/commands/metadata` |
| `src/core/cluster/network.ts` (dead) | `createCustomClusterCommander` from `commanders/custom/clusterCommander` |
| `src/core/transports/session.ts` (dead) | `CommandRequest`, `RedisKernel` from `commanders/custom/redis-kernel` |
| `src/core/transports/session-types.ts` (dead) | `CommandRequest` |
| `src/core/transports/transaction-state.ts` (dead) | `CommandRequest` |

`src/types.ts` is the live coupling that matters: it is imported by live new
code (e.g. `Resp2Server`/`cluster.ts` pull `Logger` from it), and it drags
legacy `metadata.ts` into the live build graph. It also still defines the old
contracts (`Command`, `Transport`, `CommandResult`, `ExecutionContext`,
`DBCommandExecutor` with `createAdapter(logger, socket: net.Socket)`).

### 4.2 Duplication the plan said to remove

- **Two Session classes**: new `ClientSession` (`src/core/client-session.ts`,
  live) and old `Session` (`src/core/transports/session.ts`, dead).
- **Two RESP adapters**: new `Resp2SessionAdapter` (`session-adapter.ts`, live)
  and old `RespAdapter` (`adapter.ts`, dead).
- **Two `Resp2Transport`**: old class in `resp2/index.ts` (uses
  `DBCommandExecutor.createAdapter(net.Socket)`), only consumed by dead
  `network.ts` — yet **re-exported as public API** from `src/index.ts`. The
  shipped package therefore exposes a dead, net.Socket-bound transport.
- `capturing-transport.ts` still exists and is used by the dead old `Session`.
  Plan Phase 5 explicitly: delete it.

### 4.3 Old tests keep legacy alive

~11 test files still import `commanders/custom` and exercise the legacy code,
not the new pipeline: `tests/redis/*` (hash/key/list/set/sorted-set/string/
script), `tests/reactive-store.test.ts`, `tests/cluster-router.test.ts`,
`tests/command-registry.test.ts`, `tests/command-test-utils.ts`. So the green
unit suite is partly testing code slated for deletion, and these tests block the
cutover. Acceptance "all tests pass with the new pipeline" is technically green
but misleading.

---

## 5. Acceptance Criteria Scorecard

### Met ✅

- A normal command returns a `RedisResult` and does not write to a transport.
- Command schema parsing is the single source of truth for arity/syntax.
- Key extraction happens from parsed args (`definition.keys(args)`).
- Cluster validation runs for normal **and** transaction commands
  (`ClusterPolicy.beforeExecute`, slot pinning across MULTI via WeakMap).
- **Cluster validation runs for `redis.call` from Lua** — `runRedisCommand`
  routes through `ctx.executor.executePlanSync`, which runs all policies
  including `ClusterPolicy`. Fixes the old "inner calls bypass routing" gap.
- WATCH sees every mutation, including lazy eviction — `evictIfExpired()` emits
  an `evict` event; `ClientSession.watch()` subscribes per-key and marks dirty.
- Lua command execution uses the same executor as RESP.
- `FLUSHDB` vs script cache separated — `scriptCache` lives on
  `RedisServerState`; `FLUSHDB` → `db.flush()` (keyspace only); `SCRIPT FLUSH`
  → `scriptCache.flush()`.
- Open registry — `register` / `override` / `registerAll`; `extraCommands`
  passthrough in `createRedisCommandExecutor`.
- One serialization turn per `RedisDatabase` — `db.turnQueue`; session acquires
  per command; `ctx.park` releases/reacquires via `turn.suspend`.
- Session `AbortSignal` is the cancellation signal; no `new AbortController()`
  in per-command code (session may construct one once if none is supplied).
- Transaction semantics correct — EXEC: watch-dirty → null-array; queue error
  dirty → EXECABORT; else drain + execute; runtime errors become array
  elements.

### Not met / partial ❌

- **SUBSCRIBE/MONITOR via `ResponseStream`** — not implemented. `ResponseStream`
  + `pushOnly` capability + `RedisPubSubBroker` exist, but no commands and the
  broker is not wired to the session.
- **Blocking command (BLPOP) end-to-end** — not implemented. `ctx.park` infra
  exists, no blocking command uses it.
- **`RedisValue` represents every RESP3 shape** — the type model does, and
  RESP2 downgrade lives in the encoder. But RESP3 **encoding** is absent:
  `encodeRedisValue` throws `"RESP3 encoding is not implemented yet"` for
  version 3, and there is no `HELLO` to negotiate it.
- **`DBCommandExecutor` accepts a `ConnectionTransport` and no longer depends on
  `net.Socket`** — the new path satisfies the spirit (`ConnectionTransport` +
  `InMemoryConnectionTransport` exist with a test), but the named
  `DBCommandExecutor` interface in `src/types.ts` is still
  `createAdapter(logger, socket: net.Socket)` (legacy, dead).
- **`SessionDirective` discriminated union** (plan §RESP/Session) — not
  implemented. `ClientSession.execute` returns `ExecutorResult` directly. The
  `idle`/`subscribe`/`park`/`transition-only` variants and the dedicated
  `Watcher` collaborator are absent. `ClientSessionMode` includes `'subscribed'`
  but it is never used.
- Session does not own a protocol version (RESP2/RESP3) field, despite the plan
  listing it as per-client state.

---

## 6. Minor / Correctness Nits

- `createNoopParkHandler()` just returns `createDefaultParkHandler()` —
  misnamed; it is not a no-op (`src/core/redis-context.ts`).
- EXEC executes queued plans with `createExecutionContext()` (no turn access),
  so `ctx.park` inside EXEC uses the plain handler that does not release the
  turn — latent deadlock if a blocking command were ever queued in MULTI.
  Currently moot (no blocking commands; real Redis resolves these as nil). Maps
  to the plan's open question on batch-vs-single park context.
- `RedisKeyspace.emitWrite` emits `entry.value` (the stored object reference,
  not a clone). A misbehaving listener could mutate internal state. A passing
  test claims clones on mutation events — verify the bus clones, not just reads.
- `node` engine is pinned `>=24`; some barrel ESM named-export resolution
  behaves differently under node 22 (sandbox), not a real bug under node ≥24.

---

## 7. Suggested Priority Order (for a follow-up, not done here)

1. **Unblock integration** (§2): add the handshake commands clients need on
   connect (`INFO`, `CLIENT`, likely `HELLO`/`COMMAND`); confirm with a raw
   single-node socket repro.
2. Finish Phase 4: pub/sub + blocking via the existing `ResponseStream`/
   `ctx.park` contract; scan family.
3. Phase 5 cleanup: move `Logger` (and any kept types) out of `src/types.ts`,
   drop the legacy contracts, delete `src/commanders/custom/**`, the dead
   new-core transport/cluster files, and `capturing-transport.ts`; port or drop
   the legacy-targeting tests; stop re-exporting the old `Resp2Transport`.
4. RESP3 encoder + `HELLO` negotiation; `SessionDirective` + `subscribed` mode.
