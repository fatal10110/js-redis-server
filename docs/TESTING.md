# Testing guide

[Back to the quick start](../README.md)

Install the optional client package shown in each example (`ioredis@5` or
`redis`). TypeScript examples require your test runner's TypeScript setup.
See the [README](../README.md) for runnable JavaScript examples.

## Use as a Redis mock in tests

`createRedisMock()` owns the whole lifecycle: it starts a standalone server
(16 databases, random free port) or a cluster. Connect your real client, then
use `seed()` and `flush()` explicitly when needed; data is not reset automatically.

`RedisMock` surface:

| Member                  | Description                                                                         |
| :---------------------- | :---------------------------------------------------------------------------------- |
| `host` / `port` / `url` | Connection coordinates of the (first) node.                                         |
| `addresses()`           | `{ host, port }[]` — one entry standalone, every node for cluster. Client-agnostic. |
| `seed(entries)`         | Preload data (see [Seeding](#seeding)).                                             |
| `flush()` / `reset()`   | Clear all keyspace data between tests.                                              |
| `close()`               | Shut down the server / cluster.                                                     |
| `state` / `nodes`       | Escape hatches to the underlying `RedisServerState` / node handles.                 |

### Connecting your client

A mock is a real server on a random free port, so connect any standard client
to `mock.addresses()` / `mock.url`:

```typescript
// ioredis
import { Redis } from 'ioredis'
const redis = new Redis(mock.addresses()[0])

// node-redis
import { createClient } from 'redis'
const client = createClient({ url: mock.url })
await client.connect()
```

Connections start on RESP2 and upgrade to RESP3 when the client asks for it
(for example, node-redis supports a `RESP: 3` option). RESP3 support and
configuration depend on the client and version; the `ioredis@5` examples here
use RESP2.

### node:test

```typescript
import { test, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { createRedisMock, type RedisMock } from 'js-redis-server'

let mock: RedisMock
let client: Redis

beforeEach(async () => {
  mock = await createRedisMock()
  client = new Redis(mock.addresses()[0])
})

afterEach(async () => {
  client.disconnect()
  await mock.close()
})

test('basic set/get operations', async () => {
  await client.set('foo', 'bar')
  assert.strictEqual(await client.get('foo'), 'bar')
})
```

### vitest / jest

```typescript
import { beforeEach, afterEach, test, expect } from 'vitest' // or '@jest/globals'
import { Redis } from 'ioredis'
import { createRedisMock, type RedisMock } from 'js-redis-server'

let mock: RedisMock
let client: Redis

beforeEach(async () => {
  mock = await createRedisMock()
  await mock.seed([{ key: 'counter', type: 'string', value: 1 }])
  client = new Redis(mock.addresses()[0])
})

afterEach(async () => {
  client.disconnect()
  await mock.close()
})

test('increments a seeded counter', async () => {
  expect(await client.incr('counter')).toBe(2)
})
```

Prefer a fresh `createRedisMock()` per test for full isolation; to reuse one
instance across a file, call `await mock.flush()` in `afterEach` instead.

### Cluster mocks

Same facade — pass `cluster`, then point a cluster client at every node via
`mock.addresses()`:

```typescript
const mock = await createRedisMock({ cluster: { masters: 3, replicas: 1 } })
```

```typescript
// ioredis
const cluster = new Redis.Cluster(mock.addresses())
```

```typescript
// node-redis
import { createCluster } from 'redis'
const cluster = createCluster({
  rootNodes: mock
    .addresses()
    .map(n => ({ url: `redis://${n.host}:${n.port}` })),
})
await cluster.connect()
```

### Compatibility profiles

By default the mock exposes the newest implemented Redis behavior. Pass
`compatibility` when a test needs to match an older Redis or Valkey target:

```typescript
const redis62 = await createRedisMock({ compatibility: 'redis-6.2' })

const valkeyCluster = await createRedisMock({
  cluster: { masters: 3 },
  compatibility: 'valkey-9.0',
})
```

Profiles gate implemented commands, subcommands, options, and known behavioral
differences. For example, `EXPIRETIME key` is unavailable under `redis-6.2` but
available under newer Redis profiles. Unsupported commands remain unsupported
regardless of profile. See the current gate matrix in
[Compatibility Profiles](API.md#compatibility-profiles).

Supported presets: `redis-6.2`, `redis-7.0`, `redis-7.2`, `redis-7.4`,
`redis-8.0`, `valkey-8.0`, and `valkey-9.0`.

Valkey profiles model the Redis 7.0-era gates as enabled:

| Profile      | Redis 7.0 command/subcommand/option gates | Valkey-only modeled gate  |
| ------------ | ----------------------------------------- | ------------------------- |
| `valkey-8.0` | enabled                                   | cluster multi-DB disabled |
| `valkey-9.0` | enabled                                   | cluster multi-DB enabled  |

### Seeding

`seed()` takes an explicit entries array — you supply keys, types, values, and
optional `ttlMs` / `db`; the mock owns placement (including cluster slot
routing) and the internal value conversion.

```typescript
const mock = await createRedisMock()

await mock.seed([
  { key: 'user:1', type: 'string', value: 'alice' },
  { key: 'counter', type: 'string', value: 42 },
  { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
  { key: 'l:1', type: 'list', value: ['a', 'b', 1] },
  { key: 's:1', type: 'set', value: ['x', 'y'] },
  { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
  { key: 'ttl:1', type: 'string', value: 'temp', ttlMs: 50_000 },
  { key: 'in-db-3', type: 'string', value: 'scoped', db: 3 },
])

// any client connected to the mock now sees the seeded keys
// (e.g. new Redis(mock.addresses()[0]) — GET user:1 → 'alice')
```

Each entry's shape is checked against its `type`:

```typescript
type SeedEntry =
  | {
      key: string
      type: 'string'
      value: string | number
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'hash'
      value: Record<string, string | number>
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'list'
      value: (string | number)[]
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'set'
      value: (string | number)[]
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'zset'
      value: Record<string, number>
      ttlMs?: number
      db?: number
    }
```

`db` selects the logical database (standalone mocks). Streams are not seedable
yet. For anything beyond these shapes, drive your client directly or reach for
the `mock.state` escape hatch.

### `createRedisMock` options

```typescript
createRedisMock(options?: CreateRedisMockOptions): Promise<RedisMock>
```

| Parameter       | Type                                     | Default       | Description                                                  |
| :-------------- | :--------------------------------------- | :------------ | :----------------------------------------------------------- |
| `cluster`       | `{ masters: number; replicas?: number }` | `undefined`   | When set, builds a cluster mock instead of a standalone one. |
| `databaseCount` | `number`                                 | `16`          | Standalone-only: logical database count.                     |
| `compatibility` | `CompatibilitySpec`                      | `'redis-8.0'` | Redis / Valkey compatibility profile.                        |
| `port`          | `number`                                 | `0`           | Standalone bind port (`0` = OS-assigned).                    |
| `basePort`      | `number`                                 | `0`           | Cluster base port (`0` = each node OS-assigned).             |
| `logger`        | `Pick<Logger, 'error'>`                  | `undefined`   | Optional logger.                                             |

## Experimental: socketless client mocks

> **Experimental.** These return a client object directly — no socket, no
> port — so they skip the real network round-trip and (in some cases) real RESP
> encoding. They need no `addresses()` wiring, but skip operating-system networking,
> and their surfaces are still evolving. Prefer [`createRedisMock`](#use-as-a-redis-mock-in-tests) + a real
> client unless you have a specific reason not to.

Three flavours, depending on which client you want to look like:

| Helper                 | Looks like      | How                                                        |
| :--------------------- | :-------------- | :--------------------------------------------------------- |
| `createIoredisMock`    | `ioredis`       | the **real** ioredis client over a fake `net.Socket`       |
| `createNodeRedisMock`  | `node-redis`    | a hand-written facade mirroring node-redis' public surface |
| `createInMemoryClient` | our own bespoke | a thin client that returns native JS replies, no RESP      |

### `createIoredisMock` — virtual-socket ioredis client

`createIoredisMock()` is an option for tests otherwise using
[`ioredis-mock`](https://www.npmjs.com/package/ioredis-mock), but is not a
constructor-compatible or universally drop-in replacement. It returns a **real**
`ioredis` client wired to the
in-memory pipeline over a fake `net.Socket` — no TCP port, no loopback. Because
it's the genuine client speaking RESP, its command encoding and reply parsing
remain involved. The server's implemented commands, virtual transport and
lifecycle still have compatibility limits; validate your application's tests. `ioredis` is an optional peer
dependency, imported lazily — install `ioredis` yourself to use this helper.

```typescript
import { createIoredisMock } from 'js-redis-server'
import type { Redis } from 'ioredis'

const redis = (await createIoredisMock()) as Redis // 16 logical DBs by default

await redis.set('k', 'v')
await redis.get('k') // 'v'
await redis.hset('h', 'f1', 'a', 'f2', 'b')
await redis.hgetall('h') // { f1: 'a', f2: 'b' }

await redis.quit() // tears down the in-memory state
```

Pass `cluster` for a real `Cluster` client; keyed commands follow `MOVED`
in-process across the synthetic nodes:

```typescript
import type { Cluster } from 'ioredis'

const cluster = (await createIoredisMock({
  cluster: { masters: 3, replicasPerMaster: 1 }, // replicasPerMaster optional
})) as Cluster

await cluster.set('alpha', '1') // routed to its owning master
await cluster.get('alpha') // '1'

await cluster.quit()
```

Preload data with a `seed` array (same [`SeedEntry`](#seeding) shapes as
`createRedisMock().seed()`). The keyspace is populated before the client
connects, so it's ready on the first command. In cluster mode each key is
routed to its slot-owning master:

```typescript
const redis = (await createIoredisMock({
  seed: [
    { key: 'user:1', type: 'string', value: 'alice' },
    { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
    { key: 'temp', type: 'string', value: 'x', ttlMs: 50_000 },
  ],
})) as Redis

await redis.get('user:1') // 'alice'

// cluster: createIoredisMock({ cluster: { masters: 3 }, seed: [...] })
```

### `createNodeRedisMock` — node-redis in-memory mock

node-redis exposes no socket hook, so this can't drive the real client over a
virtual socket the way `createIoredisMock` does. Instead `createNodeRedisMock()`
returns a **hand-written facade** that mirrors node-redis' public surface — a
curated set of camelCase methods with node-redis-correct return types — and
routes every command through the same in-memory pipeline. Anything not curated
falls through to the generic `sendCommand()` escape hatch, which decodes replies
to native JS.

```typescript
import { createNodeRedisMock } from 'js-redis-server'

const client = await createNodeRedisMock() // 16 logical DBs by default

await client.set('k', 'v')
await client.get('k') // 'v'
await client.sendCommand(['HSET', 'h', 'f1', 'a']) // escape hatch

await client.quit() // tears down the in-memory state
```

Pass `cluster` for a cluster facade; keyed commands route by slot in-process:

```typescript
const cluster = await createNodeRedisMock({
  cluster: { masters: 3, replicas: 1 },
})

await cluster.set('alpha', '1')
await cluster.get('alpha') // '1'

await cluster.quit()
```

### `createInMemoryClient` — our own socketless client

If you don't need to look like any particular client library,
`createInMemoryClient()` returns an in-process client with its **own** keyspace
that drives the command pipeline directly — no TCP loopback, no RESP encoding —
and resolves to native JS replies (throwing `RedisCommandError` on `-ERR`).
Standalone only.

```typescript
import { createInMemoryClient } from 'js-redis-server'

const client = await createInMemoryClient({
  // databaseCount?, database?, returnBuffers?, seed?
})

await client.command('SET', 'k', 'v')
await client.command('GET', 'k') // 'v'
await client.command('INCR', 'n') // 1 (number)
await client.command('HGETALL', 'h') // { field: 'value', ... }

client.close() // tears down its keyspace
```

It takes the same `seed` array as `createRedisMock().seed()` to pre-populate its
keyspace before the first command. Need to drive an existing `createRedisMock()`'s
keyspace instead of an independent one? Construct `InMemoryRedisClient` directly
with that mock's `state` and an executor from `js-redis-server/core`.
