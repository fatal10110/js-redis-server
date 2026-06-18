# Mock API Polish Plan

Goal: make `js-redis-server` smooth to adopt as a Redis mock in real test suites — trivial instance creation, friendly data preloading, clean packaging.

## Current pain (why the lib is hard to use as a test mock today)

Spinning up a standalone mock currently means hand-wiring three objects:

```ts
const state = new RedisServerState({ databaseCount: 16 })
const executor = createRedisCommandExecutor()
const server = new Resp2Server({ server: state, executor })
await server.listen(0)
const port = server.getPort()
const client = new Redis({ host: '127.0.0.1', port })
```

Problems:

1. **No high-level entry.** No standalone analog to `buildRedisCluster`. Users reinvent `cli.ts` `runSingle` wiring every time.
2. **No seed/preload API.** Preloading means running client commands, or poking `database.set(Buffer, RedisDataValue)` with raw `Buffer` keys + `createStringData(Buffer)`. No friendly seeding, no hash/list/set/zset/TTL sugar.
3. **CJS-only build.** `package.json` `exports` only has `require`; `main: dist/index.js`. Vitest/ESM users can't cleanly `import`. Adoption blocker.
4. **Export surface is everything.** `src/index.ts` dumps ~120 internal symbols flat. No curated "testing" facade — users can't tell what is public.
5. **No connection helpers.** Must stitch `host` + `getPort()` by hand into ioredis options. Cluster has `getAddresses()` but no spreadable options/url.
6. **No reset-between-tests convenience** at the instance level (`flushAllDatabases` exists on state but is not surfaced).
7. **Standalone default `databaseCount=1`** ≠ real Redis 16; every caller passes 16 manually.

## Decisions (locked)

- **Facade:** `createRedisMock()` — one async factory, both modes via options.
- **Packaging:** dual ESM + CJS now.
- **Seed input:** explicit entries array (`[{ key, type, value, ttlMs?, db? }]`).

## Target API

```ts
import { createRedisMock } from 'js-redis-server'

// standalone, 16 dbs, random free port
const mock = await createRedisMock()
// or: createRedisMock({ cluster: { masters: 3, replicas: 1 } })

await mock.seed([
  { key: 'user:1', type: 'string', value: 'alice' },
  { key: 'counter', type: 'string', value: 42 },
  { key: 'h:1', type: 'hash', value: { name: 'bob' }, ttlMs: 5000 },
  { key: 'l:1', type: 'list', value: ['a', 'b'] },
  { key: 's:1', type: 'set', value: ['x', 'y'] },
  { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
])

const client = new Redis(mock.connectionOptions()) // ioredis-shaped
// const cluster = new Redis.Cluster(mock.clusterNodes())

await mock.flush() // reset between tests
await mock.close()
mock.state // escape hatch -> RedisServerState for power users
```

`RedisMock` surface: `host`, `port`, `url`, `connectionOptions()`, `clusterNodes()`, `seed()`, `flush()`/`reset()`, `close()`, `state`/`nodes`.

## Phases

### Phase 1 — Standalone builder + facade (`src/mock.ts`, new)

- `createRedisServer(opts?)` — standalone analog to `buildRedisCluster`: builds `RedisServerState` (default **16 dbs**) + `createRedisCommandExecutor` + `Resp2Server`, `listen(opts.port ?? 0)`. Returns `{ host, port, state, server, close() }`.
- Refactor `cli.ts` `runSingle` onto it (no duplicated wiring).
- `createRedisMock(opts?)` async facade returning `RedisMock`. `opts.cluster?: { masters, replicas? }` switches to `buildRedisCluster`; otherwise standalone.
- `RedisMock` surface: `host`, `port`, `url`, `connectionOptions()` (ioredis-shaped options), `clusterNodes()`, `seed()`, `flush()`/`reset()` (→ `state.flushAllDatabases()` / cluster fan-out), `close()`, `state`/`nodes` escape hatch.

### Phase 2 — Seed API (`src/seed.ts`, new) — explicit entries array

```ts
type SeedEntry =
  | { key: string; type: 'string'; value: string | number; ttlMs?: number; db?: number }
  | { key: string; type: 'hash';   value: Record<string, string | number>; ttlMs?: number; db?: number }
  | { key: string; type: 'list';   value: (string | number)[]; ttlMs?: number; db?: number }
  | { key: string; type: 'set';    value: (string | number)[]; ttlMs?: number; db?: number }
  | { key: string; type: 'zset';   value: Record<string, number>; ttlMs?: number; db?: number }

mock.seed(entries: SeedEntry[]): Promise<void>
```

- Public contract: users provide only keys, types, values, optional `ttlMs`, and optional `db`; the mock owns placement and internal value conversion.
- `db` selects the logical Redis database/namespace. Phase 2 guarantees it for standalone mocks and keeps it in the seed shape so cluster namespace support can use the same contract later.
- Maps each entry → `RedisDataValue` via existing `create*Data`, string key → `Buffer`, `ttlMs` → `expiresAt`.
- In cluster mode, compute the Redis hash slot for each key, resolve the slot owner master, write the value into that node, and let existing replica propagation handle replicas.
- Stream seeding is deferred until the public stream entry shape is explicit (entries, IDs, groups/consumer metadata) instead of exposing internal stream state.
- Tests: unit per supported seed type; public integration coverage seeds a mock and reads back through real client libraries (`ioredis` and `node-redis`), including standalone and cluster mock modes.

### Phase 3 — Dual ESM + CJS packaging

- Dual build (tsup or twin tsc passes). `exports` map gets `import` + `require` + `types`.
- Verify `import { createRedisMock }` (ESM/vitest) **and** `require` both work.
- Curate root export: facade + builders + error classes promoted; deep internals → subpath `js-redis-server/core` (keep root re-exports for now → non-breaking).

### Phase 4 — Docs

- README "Use as a Redis mock in tests": node:test / vitest / jest `beforeEach`/`afterEach`, seeding, cluster, reset-between-tests recipes.
- Sync `docs/ARCHITECTURE.md`.

### Phase 5 (optional) — Socketless fast path

- High-level in-memory client over `InMemoryConnectionTransport` — skip loopback for users who do not need a real client lib.

## Order

1 → 2 → 3 → 4, with Phase 5 last/optional. Phases 1–2 unlock usability; Phase 3 unlocks adoption.
