# Compatibility Profiles: emulate Redis 6.2 / 7.0 / 7.2 / 7.4 / Valkey

## Context

The mock currently hardcodes one behavior set = newest Redis (`REDIS_VERSION = '7.4.4'`).
Package users who depend on this mock in their own test suites can get false greens when
their production target is older Redis (6.2, 7.0) or Valkey: commands, subcommands, and
options that did not exist yet (e.g. `EXPIRETIME`, `COMMAND DOCS`, `CLIENT SETINFO`,
`EXPIRE … NX`, `SET … GET`) are accepted by the mock but rejected by the real server
they ship against.

Goal: give package users one public option that makes a mock server emulate a chosen
**flavor + version** so the same application test suite can be pointed at "redis 6.2",
"redis 7.0", "valkey 9.0", etc. The selected profile gates command/subcommand/option
availability and behavior, and reports matching server identity strings. Default stays
newest Redis so all existing users keep current behavior unless they opt in.

The architecture already supports this cleanly — commands are pure `(args, ctx)` and the
registry/executor are constructed per-server. We add one **profile** object, derive a
**feature set** from it, and gate at four sites. No command rewrites.

## User-facing API

The primary contract is consumer ergonomics. Users should configure compatibility from
the same construction point they already use to start an in-process mock; they should not
need to know that state and executor are separate internal objects.

```ts
import { buildRedisServer, buildRedisCluster } from 'js-redis-server'

const server = buildRedisServer({
  compatibility: 'redis-6.2',
  databaseCount: 16,
})
await server.listen(0)

const cluster = buildRedisCluster({
  masters: 3,
  replicasPerMaster: 0,
  basePort: 0,
  compatibility: 'valkey-9.0',
  databasesPerNode: 16,
})
await cluster.listen()
```

Public API requirements:
- `buildRedisServer({ compatibility })` is the documented standalone entry point and
  internally wires one resolved profile into both `RedisServerState` and
  `CommandExecutor`.
- `buildRedisCluster({ compatibility })` is the documented cluster entry point and applies
  the same resolved profile to every node.
- Low-level constructors can still accept `compatibility` for advanced tests, but package
  docs should steer users to the builders to avoid split-brain configuration.
- Compatibility profiles cover the implemented command surface. They are not a guarantee
  of perfect Redis/Valkey parity for unimplemented commands, but they must prevent known
  false greens for commands, subcommands, options, and behaviors that this package does
  implement.

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
| Arg parser / subcommand parser | `t.custom` schemas via `ParseContext.profile`, command `execute` where subcommands are parsed today | `EXPIRE … NX` invalid < 7.0; `SET … GET/EXAT` invalid < 6.2; `CLIENT SETINFO` absent < 7.2 |
| **Execution policy** | `ExecutionPolicy.beforeExecute(plan, ctx)` via `ctx.server.profile` | **Valkey cluster allows non-zero `SELECT` / multi-DB; Redis cluster forbids it** |
| **Command execute** | `execute(args, ctx)` via `ctx.server.profile` | version strings, reply-shape/semantic tweaks |

Two declarative primitives feed all four sites:
- **Command existence** → `since` (a `VersionGate`) on the `CommandDefinition`.
- **Everything else** (subcommands, options, policy behavior, semantics) → named
  **feature flags** in a central table, checked via `profile.has('feature')`.

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
  | 'command.docs'        // COMMAND DOCS                                    (subcommand)
  | 'command.getkeysandflags' // COMMAND GETKEYSANDFLAGS                     (subcommand)
  | 'client.setinfo'      // CLIENT SETINFO                                  (subcommand)
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
  'command.docs':      { redis: '7.0.0', valkey: '7.2.0' },
  'command.getkeysandflags': { redis: '7.0.0', valkey: '7.2.0' },
  'client.setinfo':    { redis: '7.2.0', valkey: '7.2.0' },
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
- Audit every implemented command and tag every root command introduced after Redis 6.0.
  Current known set includes:
  - Redis 6.2: `GETEX`, `GETDEL`, `COPY`, `HRANDFIELD`, `LMOVE`, `BLMOVE`,
    `SMISMEMBER`, `XAUTOCLAIM`, `ZMSCORE`.
  - Redis 7.0: `EXPIRETIME`, `PEXPIRETIME`, `LMPOP`, `BLMPOP`, `ZMPOP`,
    `BZMPOP`, `SINTERCARD`.
  - Redis 7.2: no root commands currently identified in the implemented surface, but
    run the audit before implementation rather than relying on this list.
- `src/commands/index.ts`: `createRedisCommandRegistry(extraCommands, profile = resolveCompatibilityProfile())` registers a base command only when `def.since === undefined || gateSatisfied(def.since, profile)`. `extraCommands` (cluster) always registered. `createRedisCommandExecutor` gains `compatibility?: CompatibilitySpec`, resolves it once, filters the registry with it, and forwards the resolved profile to `new CommandExecutor({ …, profile })`.

Side benefit: `COMMAND COUNT/DOCS/INFO` read `registry.getAll()`, so introspection auto-reflects the profile.

### Subcommand / option gating
- Subcommands need their own gates because the root command may predate the subcommand.
  Add `since?: VersionGate` to subcommand introspection metadata or filter the existing
  `subcommands` array through a helper before exposing `COMMAND INFO/DOCS`.
- `src/commands/command.ts`: gate `COMMAND DOCS` and `COMMAND GETKEYSANDFLAGS`
  behind Redis 7.0 / Valkey 7.2. Older profiles should return the same unknown
  subcommand error shape the real target returns, and introspection should not advertise
  gated-off subcommands.
- `src/commands/connection.ts`: gate `CLIENT SETINFO` behind Redis 7.2 / Valkey 7.2.
  This matters for package users running clients that probe `CLIENT SETINFO` during
  connection setup; an older profile must behave like the older real server.
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

### Public builders own profile wiring
Both public builders compose `RedisServerState` + `createRedisCommandExecutor` +
`Resp2Server`. Resolve the profile **once** and hand the same object to both state
(version strings / server-wide behavior) and executor (registry filtering / parsing).
- `src/server.ts`: add `buildRedisServer({ compatibility, databaseCount, requirepass,
  host, port, logger })`. This is the documented standalone API for package users.
  It resolves the profile once, constructs `RedisServerState({ …, compatibility: profile })`,
  constructs `createRedisCommandExecutor({ compatibility: profile })`, and returns the
  configured `Resp2Server`.
- `src/cluster.ts`: add `compatibility?: CompatibilitySpec` to `RedisClusterOptions`;
  resolve once in `buildRedisCluster`; pass the profile into `createClusterNodeStates`
  (→ each `new RedisServerState({ …, compatibility: profile })`) and into each
  `createRedisCommandExecutor({ …, compatibility: profile })` (cluster.ts:143,
  cluster.ts:233).
- `src/cli.ts`: read a spec from `--compat <preset>` / `REDIS_COMPAT` env, then call
  `buildRedisServer` or `buildRedisCluster`. CLI code should not manually construct
  state/executor pairs.
- Optional hardening: expose `CommandExecutor.profile` and assert in `Resp2Server`
  construction that `server.profile === executor.profile` when both are compatibility-aware.
  This catches advanced users who manually wire mismatched low-level objects.

### Exports
- `src/index.ts`: export `buildRedisServer`, `buildRedisCluster`, `resolveCompatibilityProfile`, `gateSatisfied`, and the `CompatibilityProfile` / `CompatibilitySpec` / `RedisFlavor` / `VersionGate` / `FeatureId` types.

## Package-user documentation

Add a short compatibility-profile section to the README/API docs:
- Show `buildRedisServer({ compatibility: 'redis-6.2' })` for standalone tests.
- Show `buildRedisCluster({ compatibility: 'valkey-9.0', databasesPerNode: 16 })`
  for cluster tests.
- State that the default is newest supported Redis (`redis-7.4`) for backward
  compatibility.
- State the scope clearly: profiles gate implemented commands/subcommands/options and
  known behavior differences to prevent false greens; unsupported Redis commands remain
  unsupported rather than simulated.
- Include a small false-green example:
  `EXPIRETIME key` returns unknown-command under `redis-6.2`, while default/newer
  profiles accept it.

## Maintainer verification

Tests should validate the package-user API first. Low-level state/executor tests are still
useful, but they are supporting coverage rather than the interface users should copy.

Unit (`tests/compatibility/` + co-located):
- `profile.test.ts`: `parseVersion`, `versionNum` ordering, `gateSatisfied` per flavor,
  named-preset resolution, default = `redis-7.4`, Valkey 7.2 inherits Redis 7.2-era
  gates, and Valkey 9.0 enables Valkey-specific gates such as `cluster.multi-db`.
- public builder wiring: `buildRedisServer({ compatibility:'redis-6.2' })` and
  `buildRedisCluster({ compatibility:'valkey-9.0' })` create state/executor pairs with
  the same resolved profile.
- registry filtering: `createRedisCommandRegistry([], resolveCompatibilityProfile('redis-6.2'))` has no Redis 7.0 root commands (`expiretime`, `lmpop`, `zmpop`, `sintercard`, etc.); `redis-7.0` has them; default has all.
- subcommand filtering: `COMMAND DOCS` / `COMMAND GETKEYSANDFLAGS` are absent under
  `redis-6.2`; `CLIENT SETINFO` is absent under `redis-7.0`; matching introspection
  responses do not advertise gated-off subcommands.
- parse gating: `EXPIRE k 10 NX` under `redis-6.2` → `WrongNumberOfArgumentsError`, under `redis-7.0` → parses to `condition:'NX'`; `SET k v GET` under a `set.get`-off profile → `RedisSyntaxError`, under `redis-6.2` → ok.
- policy gating: `createClusterPolicy` `beforeExecute` on a `select` plan → throws under a `redis` profile, passes under `valkey-9` (`cluster.multi-db` on).
- reporting: `INFO`/`HELLO` strings reflect `version` + `flavor` (incl. `valkey_version` line for valkey).

Integration (`tests-integration/`, **mock only** — in-process servers built through the
public builders with explicit `compatibility`), TDD red-first per the integration-first rule:
- standalone `buildRedisServer({ compatibility:'redis-6.2' })`: `EXPIRETIME k` →
  `unknown command`; `EXPIRE k 10 NX` → wrong-args error, asserted through a real client.
- standalone `buildRedisServer({ compatibility:'redis-7.0' })`: `CLIENT SETINFO` is
  rejected like Redis 7.0, while default/newest accepts it.
- cluster `buildRedisCluster({ …, compatibility:'valkey-9.0', databasesPerNode: 16 })`:
  `SELECT 1` succeeds; the same cluster on a redis profile rejects `SELECT 1` with
  `SELECT is not allowed in cluster mode`.
- Existing integration suites keep the **default** profile, so they (and the real-Redis backend) stay green unchanged.

Real cross-version matrix (spinning up redis:6.2 / 7.0 / 7.2 / valkey containers and
asserting the mock matches each) can be follow-up automation, but implementation should
capture enough real-server responses manually or in fixtures before encoding exact error
shapes for older profiles.

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
- New version-introduced subcommand → add a `FeatureId` + `FEATURE_GATES` row, gate
  subcommand dispatch, and filter subcommand introspection.
- New optional/behavioral divergence (option, **policy rule, routing, reply shape, default RESP version**) → add a `FeatureId` + a `FEATURE_GATES` row, branch on `ctx.server.profile.has(...)` (policy/execute) or `ctx.profile.has(...)` (parser). No constructor/wiring changes.
- Valkey-only commands → `since: { valkey:'9.0.0' }` with no `redis` key.
- Per-version error-message wording fits the same `profile.has(...)` branch at the throw site.
