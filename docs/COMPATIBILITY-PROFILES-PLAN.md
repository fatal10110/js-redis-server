# Compatibility Profiles: emulate Redis 6.2 / 7.0 / 7.2 / 7.4 / Valkey

## Context

The mock currently hardcodes one behavior set = newest Redis (`REDIS_VERSION = '7.4.4'`).
Consumers testing against an older Redis (6.2, 7.0) or against Valkey get false
greens: commands and options that did not exist yet (e.g. `EXPIRETIME`, `EXPIRE … NX`,
`SET … GET`) are accepted by the mock but rejected by the real server they ship against.

Goal: make a single server instance emulate a chosen **flavor + version** so the same
test suite can be pointed at "redis 6.2", "redis 7.0", "valkey 9.0", etc., and the mock
gates command/option/behavior availability and reports the matching version exactly like
the real target. Default stays newest Redis so all existing tests/behavior are unchanged.

The architecture already supports this cleanly — commands are pure `(args, ctx)` and the
registry/executor are constructed per-server. We add one **profile** object, derive a
**feature set** from it, and gate at four sites. No command rewrites.

## Design

One resolved `CompatibilityProfile` is the single source of truth, reachable from every
layer of the pipeline. It is held in **two** places (which see the same object):

1. **CommandExecutor** (constructor-held) — needed for the two sites that run *before* any
   `ctx` exists, inside `executor.plan()`:
   - **Registry filtering**: build the registry with only the commands that exist in the
     target version → absent commands return real `unknown command`.
   - **Parse-time option gating**: inject the profile into `ParseContext` so arg schemas
     reject too-new options.
2. **RedisServerState** (`ctx.server.profile`) — reachable at *execute* and *policy* time
   (every `execute(args, ctx)` and every `ExecutionPolicy.beforeExecute(plan, ctx)` already
   carries `ctx`), used for version strings **and** version-divergent semantics/routing.

Divergences are **not** only command/option existence. They span four gate sites, all
reading the same profile via one evaluation primitive (`gateSatisfied(gate, profile)` /
`profile.has(feature)`):

| Site | Where | Example divergence |
|------|-------|--------------------|
| Registry build | `createRedisCommandRegistry` (executor) | `EXPIRETIME` absent < 7.0 ⇒ `unknown command` |
| Arg parser | `t.custom` schemas via `ParseContext.profile` | `EXPIRE … NX` invalid < 7.0; `SET … GET/EXAT` invalid < 6.2 |
| **Execution policy** | `ExecutionPolicy.beforeExecute(plan, ctx)` via `ctx.server.profile` | **Valkey cluster allows non-zero `SELECT` / multi-DB; Redis cluster forbids it** |
| **Command execute** | `execute(args, ctx)` via `ctx.server.profile` | version strings, reply-shape/semantic tweaks |

Two declarative primitives feed all four sites:
- **Command existence** → `since` (a `VersionGate`) on the `CommandDefinition`.
- **Everything else** (options, policy behavior, semantics) → named **feature flags** in a
  central table, checked via `profile.has('feature')`.

Both decouple call sites from raw version math, which matters because **Valkey forked at
Redis 7.2.4** — gates are keyed per-flavor, not by a single number. Policies and commands
need **no construction-time wiring** — they already receive `ctx`, so reading
`ctx.server.profile` is enough.

## New module: `src/core/compatibility/`

`profile.ts`:
```ts
export type RedisFlavor = 'redis' | 'valkey'

// minimum version a thing appears in, per flavor; absent flavor = "never present"
export type VersionGate = { redis?: string; valkey?: string }

export type FeatureId =
  | 'expire.conditions'   // EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT  NX|XX|GT|LT  (parser)
  | 'set.get'             // SET … GET                                       (parser)
  | 'set.exat-pxat'       // SET … EXAT|PXAT                                 (parser)
  | 'cluster.multi-db'    // SELECT non-zero db / multi-DB in cluster mode   (policy)

export interface CompatibilityProfile {
  readonly flavor: RedisFlavor
  readonly version: string      // display, e.g. '6.2.14'
  readonly versionNum: number   // comparable: major*10000 + minor*100 + patch
  has(feature: FeatureId): boolean
}

export type CompatibilitySpec =
  | CompatibilityProfile
  | { flavor?: RedisFlavor; version?: string }
  | 'redis-6.2' | 'redis-7.0' | 'redis-7.2' | 'redis-7.4'
  | 'valkey-8.0' | 'valkey-9.0' // presets (arbitrary {flavor,version} also accepted)

export function resolveCompatibilityProfile(spec?: CompatibilitySpec): CompatibilityProfile
export function gateSatisfied(gate: VersionGate, profile: CompatibilityProfile): boolean
```

- `resolveCompatibilityProfile()` with no arg ⇒ newest Redis (**`redis-7.4` / `7.4.4`**) ⇒ identical to today.
- Named presets map to `{ flavor, version }`; arbitrary versions allowed (`{ flavor:'valkey', version:'9.0.0' }`) so "valkey 9.0" works even though it post-dates real releases.
- `versionNum` via a `parseVersion()` helper; `has()` is precomputed at resolve time from the gate table.
- `gateSatisfied(gate, p)` = `gate[p.flavor] !== undefined && p.versionNum >= parseVersion(gate[p.flavor])`.

`feature-gates.ts`:
```ts
export const FEATURE_GATES: Record<FeatureId, VersionGate> = {
  'expire.conditions': { redis: '7.0.0', valkey: '7.2.0' },
  'set.get':           { redis: '6.2.0', valkey: '7.2.0' },
  'set.exat-pxat':     { redis: '6.2.0', valkey: '7.2.0' },
  // No `redis` key ⇒ never present in any Redis cluster; Valkey added cluster
  // multi-DB. Version is per the user's "valkey 9" example and easily tuned.
  'cluster.multi-db':  { valkey: '9.0.0' },
}
// Valkey forked from Redis 7.2.4, so any modeled valkey >= 7.2 has the 7.2 features above.
```

`index.ts` re-exports the types/functions.

## Changes by file

### Plumb the profile into parsing
- `src/core/command-schema.ts`: extend `ParseContext` → `{ commandName; profile: CompatibilityProfile }`. `parseCommandArgs(schema, input, commandName, profile = resolveCompatibilityProfile())` builds the ctx (optional param keeps the public export backward-compatible).
- `src/core/command-executor.ts`: add `profile?: CompatibilityProfile` to `CommandExecutorOptions` (default resolved-newest); store it; pass it in `createPlan` → `parseCommandArgs(...)`.

### Command-existence gating
- `src/core/command-definition.ts`: add `readonly since?: VersionGate` to `CommandDefinition`. `defineCommand` passes it through unchanged. Untagged commands (≈180 legacy ones) are always present.
- Tag the version-introduced commands with `since` (full-retrofit set):
  - `getexCommand`, `getdelCommand` in `src/commands/strings.ts` → `{ redis:'6.2.0', valkey:'7.2.0' }`
  - `copyCommand` in `src/commands/keys.ts` → `{ redis:'6.2.0', valkey:'7.2.0' }`
  - `expiretimeCommand`, `pexpiretimeCommand` in `src/commands/keys.ts` → `{ redis:'7.0.0', valkey:'7.2.0' }`
- `src/commands/index.ts`: `createRedisCommandRegistry(extraCommands, profile = resolveCompatibilityProfile())` registers a base command only when `def.since === undefined || gateSatisfied(def.since, profile)`. `extraCommands` (cluster) always registered. `createRedisCommandExecutor` gains `compatibility?: CompatibilitySpec`, resolves it once, filters the registry with it, and forwards the resolved profile to `new CommandExecutor({ …, profile })`.

Side benefit: `COMMAND COUNT/DOCS/INFO` read `registry.getAll()`, so introspection auto-reflects the profile.

### Option / behavior gating (parse-time)
- EXPIRE family in `src/commands/keys.ts`: the existing `expireConditionSchema` custom parser, when it sees `NX|XX|GT|LT` but `!ctx.profile.has('expire.conditions')`, **does not consume** the token and returns `undefined`. The trailing arg then trips `parseCommandArgs`' length check → `WrongNumberOfArgumentsError` — which is exactly what real 6.2 (fixed arity 3) returns. When the feature is on, behavior is unchanged.
- SET option loop in `src/commands/strings.ts` (`createSetSchema`): when the loop encounters `GET` and `!ctx.profile.has('set.get')`, or `EXAT|PXAT` and `!ctx.profile.has('set.exat-pxat')`, throw `RedisSyntaxError` (SET is variadic arity, so an unknown option is `ERR syntax error` on the real server — matches).

Rule of thumb for any future option gate: replicate what the real old server does — fixed-arity commands surface a trailing unsupported option as `WrongNumberOfArgumentsError` (don't consume), variadic commands as `RedisSyntaxError` (throw).

### Policy / semantic gating (execute-time, no new wiring)
Policies and `execute` already receive `ctx`, so they read `ctx.server.profile` directly — `createClusterPolicy` etc. keep their current signatures.
- `src/core/execution-policies/cluster-policy.ts` (line 36): the unconditional `SELECT is not allowed in cluster mode` becomes
  `if (plan.definition.name === 'select' && !ctx.server.profile.has('cluster.multi-db')) throw …`.
  On a `valkey-9` profile the ban lifts, so `SELECT 1` succeeds in cluster mode (per-session DB already works; configure `databasesPerNode > 1` when building the cluster). Real Redis / older Valkey still get the error.
- This is the template for any future policy- or execute-layer divergence (routing rules, reply shape, default RESP version): add a `FeatureId`, branch on `ctx.server.profile.has(...)`. No policy/command constructor changes.

### Version reporting reads the profile
- `src/state/server-state.ts`: add `compatibility?: CompatibilitySpec` to `RedisServerStateOptions`; add `readonly profile: CompatibilityProfile = resolveCompatibilityProfile(options?.compatibility)`.
- `src/commands/connection.ts`: delete the `REDIS_VERSION` const.
  - `INFO` server section: `redis_version:${ctx.server.profile.version}`; when `flavor === 'valkey'` also emit `server_name:valkey` and `valkey_version:${ctx.server.profile.version}` (Valkey keeps a compat `redis_version` line too — use a 7.x compat string for that line).
  - `HELLO`: `server` field = `ctx.server.profile.flavor`; `version` field = `ctx.server.profile.version`.

### Thread one profile through both construction sites
Both sites compose `RedisServerState` + `createRedisCommandExecutor` + `Resp2Server`. Resolve the profile **once** and hand the same object to both (state for version strings, executor for gating).
- `src/cluster.ts`: add `compatibility?: CompatibilitySpec` to `RedisClusterOptions`; resolve once in `buildRedisCluster`; pass the profile into `createClusterNodeStates` (→ each `new RedisServerState({ …, compatibility: profile })`) and into each `createRedisCommandExecutor({ …, compatibility: profile })` (cluster.ts:143, cluster.ts:233).
- `src/cli.ts` (cli.ts:163): read a spec from `--compat <preset>` / `REDIS_COMPAT` env, resolve once, pass to both the `RedisServerState` and `createRedisCommandExecutor`.
- Optional convenience (removes the "pass to two places" footgun): a small `buildRedisServer({ compatibility, databaseCount, requirepass, host, port, logger })` in `src/server.ts` that wires state+executor+`Resp2Server` from one profile; refactor `cli.ts` to use it. Export from `src/index.ts` alongside `buildRedisCluster`.

### Exports
- `src/index.ts`: export `resolveCompatibilityProfile`, `gateSatisfied`, and the `CompatibilityProfile` / `CompatibilitySpec` / `RedisFlavor` / `VersionGate` / `FeatureId` types.

## Tests

Unit (`tests/compatibility/` + co-located):
- `profile.test.ts`: `parseVersion`, `versionNum` ordering, `gateSatisfied` per flavor, named-preset resolution, default = `redis-7.4`, valkey ≥7.2 has all features.
- registry filtering: `createRedisCommandRegistry([], resolveCompatibilityProfile('redis-6.2'))` has no `expiretime`/`pexpiretime`; `redis-7.0` has them; default has all.
- parse gating: `EXPIRE k 10 NX` under `redis-6.2` → `WrongNumberOfArgumentsError`, under `redis-7.0` → parses to `condition:'NX'`; `SET k v GET` under a `set.get`-off profile → `RedisSyntaxError`, under `redis-6.2` → ok.
- policy gating: `createClusterPolicy` `beforeExecute` on a `select` plan → throws under a `redis` profile, passes under `valkey-9` (`cluster.multi-db` on).
- reporting: `INFO`/`HELLO` strings reflect `version` + `flavor` (incl. `valkey_version` line for valkey).

Integration (`tests-integration/`, **mock only** — in-process servers built with explicit `compatibility`), TDD red-first per the integration-first rule:
- standalone `compatibility:'redis-6.2'`: `EXPIRETIME k` → `unknown command`; `EXPIRE k 10 NX` → wrong-args error, asserted through the real client.
- cluster `buildRedisCluster({ …, compatibility:'valkey-9.0', databasesPerNode: 16 })`: `SELECT 1` succeeds; the same cluster on a redis profile rejects `SELECT 1` with `SELECT is not allowed in cluster mode`.
- Existing integration suites keep the **default** profile, so they (and the real-Redis backend) stay green unchanged.

Real cross-version matrix (spinning up redis:6.2 / 7.0 / valkey containers and asserting the mock matches each) is **out of scope** here — the real backend is one fixed version; this PR validates gating via unit + mock-integration. Note it as a follow-up.

## Verification

```bash
npm run build && npm run lint
npm test                          # unit incl. new compatibility tests
npm run test:integration:mock     # incl. new 6.2 standalone gating test
npm run test:integration:real     # must stay green — default profile unchanged
```
Manual smoke:
```bash
REDIS_COMPAT=redis-6.2 npm start
redis-cli -p <port> EXPIRETIME k   # (error) ERR unknown command 'EXPIRETIME'
redis-cli -p <port> EXPIRE k 10 NX # (error) ERR wrong number of arguments for 'expire'
redis-cli -p <port> INFO server    # redis_version:6.2.14
REDIS_COMPAT=valkey-9.0 npm start
redis-cli -p <port> INFO server    # server_name:valkey, valkey_version:9.0.0
# cluster smoke (valkey-9 allows multi-DB; redis rejects):
#   buildRedisCluster({ masters:1, basePort:0, compatibility:'valkey-9.0', databasesPerNode:16 })
#   redis-cli -c -p <port> SELECT 1   # OK on valkey-9, error on any redis profile
```

## Extensibility (designed-in, not built now)
- New version-introduced command → add `since` to its definition. Done.
- New optional/behavioral divergence (option, **policy rule, routing, reply shape, default RESP version**) → add a `FeatureId` + a `FEATURE_GATES` row, branch on `ctx.server.profile.has(...)` (policy/execute) or `ctx.profile.has(...)` (parser). No constructor/wiring changes.
- Valkey-only commands → `since: { valkey:'9.0.0' }` with no `redis` key.
- Per-version error-message wording fits the same `profile.has(...)` branch at the throw site.
