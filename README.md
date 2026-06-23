# js-redis-server

[![CI](https://github.com/fatal10110/js-redis-server/actions/workflows/ci.yml/badge.svg)](https://github.com/fatal10110/js-redis-server/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/js-redis-server.svg)](https://www.npmjs.com/package/js-redis-server)
[![npm downloads](https://img.shields.io/npm/dm/js-redis-server.svg)](https://www.npmjs.com/package/js-redis-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js Version](https://img.shields.io/node/v/js-redis-server.svg)](https://nodejs.org)

In-memory Redis-compatible server implementation in JavaScript. Useful for testing and development without requiring a real Redis instance.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [As a Test Mock (recommended)](#as-a-test-mock-recommended)
  - [As a Standalone Server](#as-a-standalone-server)
  - [As a CLI](#as-a-cli)
  - [CLI Options](#cli-options)
- [Connecting Clients](#connecting-clients)
  - [Protocol Version (RESP2 / RESP3)](#protocol-version-resp2--resp3)
  - [Connecting with ioredis](#connecting-with-ioredis)
  - [Connecting with node-redis](#connecting-with-node-redis)
  - [Using in Unit Tests](#using-in-unit-tests)
- [Cluster Mode](#cluster-mode)
- [API Reference](#api-reference)
- [Supported Commands](docs/COMMANDS.md)
- [Requirements](#requirements)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Features

- **RESP2 and RESP3 protocols** - Per-session version negotiation via `HELLO`
- **Standalone and Cluster modes** - Run a single server or a full cluster
- **Lua scripting support** - Execute Redis Lua scripts via WebAssembly
- **No external dependencies** - Pure JavaScript, no Redis installation needed
- **TypeScript support** - Ships with full type definitions

## Installation

```bash
npm install js-redis-server
```

## Quick Start

Picking an entry point comes down to **one** question — _are you driving it from test code, or running a server something else connects to?_ Both handle standalone **and** cluster via the same `cluster` option, so that's the only axis you choose:

| Your situation                                                                                 | Use                    | Standalone                                                | Cluster                                          |
| :--------------------------------------------------------------------------------------------- | :--------------------- | :-------------------------------------------------------- | :----------------------------------------------- |
| Writing tests / want an in-memory Redis you control from code (seed, reset, in-process client) | `createRedisMock`      | `createRedisMock()`                                       | `createRedisMock({ cluster: { masters: 3 } })`   |
| Running a real, listening server a separate process connects to (CLI, dev tool)                | `createRedisServer`    | `createRedisServer()`                                     | `createRedisServer({ cluster: { masters: 3 } })` |
| Assembling the pipeline by hand (custom commands/policies/transport)                           | `js-redis-server/core` | see [Advanced](#advanced-assembling-the-pipeline-by-hand) | —                                                |

**Most users want `createRedisMock`.** It wraps `createRedisServer` and adds the test ergonomics. Reach for `createRedisServer` only when you need a long-running server without the test helpers.

### As a Test Mock (recommended)

For test suites use the `createRedisMock()` facade. It spins up a standalone
server (16 databases, random free port) or a whole cluster, seeds data, and
resets between tests — no manual wiring:

```typescript
import { createRedisMock } from 'js-redis-server'
import { Redis } from 'ioredis'

const mock = await createRedisMock()
// or: await createRedisMock({ cluster: { masters: 3, replicas: 1 } })

await mock.seed([
  { key: 'user:1', type: 'string', value: 'alice' },
  { key: 'counter', type: 'string', value: 42 },
  { key: 'h:1', type: 'hash', value: { name: 'bob' }, ttlMs: 5000 },
  { key: 'l:1', type: 'list', value: ['a', 'b'] },
  { key: 's:1', type: 'set', value: ['x', 'y'] },
  { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
])

const client = new Redis(mock.connectionOptions())
// cluster: new Redis.Cluster(mock.clusterNodes())

await mock.flush() // reset between tests
await mock.close()
```

See [Use as a Redis mock in tests](#use-as-a-redis-mock-in-tests) for `beforeEach`/`afterEach` recipes.

### As a Standalone Server

To run a real server you connect to (not a mock), use `createRedisServer()` —
it wires the state, executor, and `Resp2Server` for you and starts listening:

```typescript
import { createRedisServer } from 'js-redis-server'

const { host, port, close } = await createRedisServer({ port: 6379 })
console.log(`Redis server listening at ${host}:${port}`)

// Cleanup
await close()
```

### As a CLI

```bash
# Run a single server on default port 6379
npx js-redis-server

# Run on a specific port
npx js-redis-server --port 6380

# Run a cluster with 3 masters
npx js-redis-server --cluster --masters 3

# Run a cluster with replicas
npx js-redis-server --cluster --masters 3 --slaves 1
```

### CLI Options

```
Modes:
  --single               Run a single Redis server (default)
  --cluster              Run a Redis cluster
  --mode <single|cluster>

Single server options:
  --port <number>        Port to listen on (default 6379)

Cluster options:
  --masters <number>     Number of masters (default 3)
  --slaves <number>      Number of replicas per master (default 0)
  --base-port <number>   Starting port for cluster nodes (default 30000)

General:
  -d, --debug            Enable debug logging
  -h, --help             Show help
```

## Cluster Mode

Pass `cluster` to `createRedisServer` (or `createRedisMock` for tests) — it
builds **and** starts the cluster, returning a live handle:

```typescript
import { createRedisServer } from 'js-redis-server'

const cluster = await createRedisServer({
  cluster: { masters: 3, replicas: 1 },
  basePort: 30000,
})

// Get all node addresses
console.log(cluster.nodes.map(n => `${n.host}:${n.port}`))

// Cleanup
await cluster.close()
```

> Need control over _when_ the cluster starts listening? The lower-level
> `createRedisCluster()` builder returns an un-started `RedisCluster` you call
> `.listen()` on yourself. (`buildRedisCluster` is a deprecated alias of it.)

## Connecting Clients

You can connect to `js-redis-server` using any standard Redis client.

### Protocol Version (RESP2 / RESP3)

Each connection starts on RESP2 and can switch to RESP3 by sending `HELLO 3`
(handled per-session, no server restart needed). Clients that support RESP3
negotiate this automatically:

```typescript
import { createClient } from 'redis'

// node-redis negotiates RESP3 via the `RESP` option
const client = createClient({
  url: 'redis://127.0.0.1:6379',
  RESP: 3,
})
await client.connect()
```

### Connecting with ioredis

```typescript
import Redis from 'ioredis'

// Single Node
const redis = new Redis(6379, '127.0.0.1')

// Cluster Node
const cluster = new Redis.Cluster([
  { host: '127.0.0.1', port: 30000 },
  { host: '127.0.0.1', port: 30001 },
  { host: '127.0.0.1', port: 30002 },
])
```

### Connecting with node-redis

```typescript
import { createClient, createCluster } from 'redis'

// Single Node
const client = createClient({
  url: 'redis://127.0.0.1:6379',
})
await client.connect()

// Cluster Node
const cluster = createCluster({
  rootNodes: [
    { url: 'redis://127.0.0.1:30000' },
    { url: 'redis://127.0.0.1:30001' },
    { url: 'redis://127.0.0.1:30002' },
  ],
})
await cluster.connect()
```

## Use as a Redis mock in tests

`js-redis-server` starts instantly with no external Redis, so it makes a clean
drop-in mock for test suites. The `createRedisMock()` facade owns the server
lifecycle, seeding, and reset-between-tests; you just connect your real client
library to it.

`RedisMock` surface:

| Member                  | Description                                                                 |
| :---------------------- | :-------------------------------------------------------------------------- |
| `host` / `port` / `url` | Connection coordinates of the (first) node.                                 |
| `connectionOptions()`   | ioredis-shaped `{ host, port }` for a single client.                        |
| `clusterNodes()`        | Seed-node list for `Redis.Cluster` (one entry for standalone).              |
| `seed(entries)`         | Preload data (see [Seeding](#seeding)).                                     |
| `client(opts?)`         | Socketless in-process client (see [Socketless client](#socketless-client)). |
| `flush()` / `reset()`   | Clear all keyspace data between tests.                                      |
| `close()`               | Shut down the server / cluster.                                             |
| `state` / `nodes`       | Escape hatches to the underlying `RedisServerState` / node handles.         |

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
  client = new Redis(mock.connectionOptions())
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
  client = new Redis(mock.connectionOptions())
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

```typescript
const mock = await createRedisMock({ cluster: { masters: 3, replicas: 1 } })
const cluster = new Redis.Cluster(mock.clusterNodes())
```

### Socketless client

If you don't need a real client library, `mock.client()` returns an in-process
client that drives the **same** command pipeline directly — no TCP loopback, no
RESP encoding — and resolves to native JS replies (throwing `RedisCommandError`
on `-ERR`). Standalone mocks only; cluster mocks throw.

```typescript
const mock = await createRedisMock()
const client = mock.client() // { database?, returnBuffers? }

await client.command('SET', 'k', 'v')
await client.command('GET', 'k') // 'v'
await client.command('INCR', 'n') // 1 (number)
await client.command('HGETALL', 'h') // { field: 'value', ... }

// clients are closed automatically when the mock closes
await mock.close()
```

For a mock with **no network listener at all**, pass `transport: 'memory'`. Only
`client()` works; the network accessors (`host`/`port`/`url`/`connectionOptions`/
`clusterNodes`) throw.

```typescript
const mock = await createRedisMock({ transport: 'memory' })
const client = mock.client()
await client.command('PING') // 'PONG'
```

### Drop-in ioredis mock

`createIoredisMock()` returns a **real** `ioredis` client wired to the in-memory
pipeline over a fake `net.Socket` — no TCP port, no loopback. Because it's the
genuine client speaking real RESP, typed methods, pipelines, `multi`, pub/sub,
and `scanStream` all work unchanged. `ioredis` is an optional peer dependency,
imported lazily, so the core stays dependency-free — install `ioredis` yourself
to use this.

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

const client = mock.client()
await client.command('GET', 'user:1') // 'alice'
await client.command('HGETALL', 'h:1') // { name: 'bob', age: '30' }
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

### Package entry points

The package ships dual ESM + CJS builds with two import surfaces:

**Root (`js-redis-server`)** — the curated consumer surface: the `create*`
builders, seeding, the socketless client, and the client-visible error classes.
This is all most users ever need.

```typescript
import {
  createRedisMock,
  createRedisCluster,
  RedisCommandError,
} from 'js-redis-server'
```

**Core (`js-redis-server/core`)** — the building blocks for assembling the
pipeline by hand: `Resp2Server`, `RedisServerState`, `createRedisCommandExecutor`,
`defineCommand`, the `t` schema builder, execution policies, transports, the Lua
runtime, data-type helpers, etc. These are **not** exported from the root.

```typescript
import {
  Resp2Server,
  RedisServerState,
  createRedisCommandExecutor,
  defineCommand,
  t,
} from 'js-redis-server/core'
```

### Advanced: assembling the pipeline by hand

`createRedisServer()` is the supported way to run a standalone server. If you
need full control — a custom command registry, extra execution policies, a
non-default transport — wire the pieces yourself from `js-redis-server/core`:

```typescript
import {
  RedisServerState,
  createRedisCommandExecutor,
  Resp2Server,
} from 'js-redis-server/core'

const state = new RedisServerState()
const executor = createRedisCommandExecutor()
const server = new Resp2Server({ server: state, executor })

await server.listen(6379)
console.log(`Redis server listening at ${server.getAddress()}`)

await server.close()
```

## API Reference

### `Resp2Server`

Creates a TCP server running the RESP2 protocol. Most users should prefer
[`createRedisServer`](#createredisserver), which wires this up for you.

> Imported from the `js-redis-server/core` subpath, not the package root.

```typescript
import { Resp2Server } from 'js-redis-server/core'

new Resp2Server(options: Resp2ServerOptions)
```

**Options (`Resp2ServerOptions`)**:

| Parameter  | Type                    | Required | Description                                                    |
| :--------- | :---------------------- | :------- | :------------------------------------------------------------- |
| `server`   | `RedisServerState`      | Yes      | The database server state containing database instances.       |
| `executor` | `CommandExecutor`       | Yes      | The command executor handling pipeline and execution policies. |
| `logger`   | `Pick<Logger, 'error'>` | No       | Optional logger for error logging.                             |
| `encoder`  | `RespEncodeOptions`     | No       | Custom RESP protocol encoding options.                         |

---

### `RedisServerState`

Holds the database and keyspace state for the server.

> Imported from the `js-redis-server/core` subpath, not the package root.

```typescript
import { RedisServerState } from 'js-redis-server/core'

new RedisServerState(options?: RedisServerStateOptions)
```

**Options (`RedisServerStateOptions`)**:

| Parameter         | Type                   | Default     | Description                                 |
| :---------------- | :--------------------- | :---------- | :------------------------------------------ |
| `databaseCount`   | `number`               | `1`         | Number of databases to initialize.          |
| `clusterTopology` | `RedisClusterTopology` | `undefined` | Optional cluster topology for slot routing. |
| `pubsubBroker`    | `RedisPubSubBroker`    | `undefined` | Optional broker for pub/sub operations.     |
| `scriptCache`     | `RedisScriptCache`     | `undefined` | Optional cache for Lua scripts.             |

---

### `createRedisCluster`

Low-level builder that returns an **un-started** `RedisCluster` — call
`.listen()` yourself.

> **Prefer [`createRedisServer`](#createredisserver) with the `cluster` option**,
> which builds and starts the cluster in one call. Use this builder only when you
> need control over when `listen()` runs. (`buildRedisCluster` is a deprecated
> alias of this function.)

```typescript
createRedisCluster(options: RedisClusterOptions): RedisCluster
```

**Options (`RedisClusterOptions`)**:

| Parameter           | Type                    | Default       | Description                                            |
| :------------------ | :---------------------- | :------------ | :----------------------------------------------------- |
| `masters`           | `number`                | **Required**  | Number of master nodes in the cluster.                 |
| `replicasPerMaster` | `number`                | `0`           | Replicas per master node.                              |
| `basePort`          | `number`                | **Required**  | Base port range. If `0`, random OS ports are assigned. |
| `host`              | `string`                | `'127.0.0.1'` | Host address to bind to.                               |
| `databasesPerNode`  | `number`                | `1`           | Number of databases per cluster node.                  |
| `logger`            | `Pick<Logger, 'error'>` | `undefined`   | Optional logger.                                       |

---

### `createRedisMock`

High-level test-mock facade. Returns a `RedisMock` wrapping a standalone server
(default) or a cluster.

```typescript
createRedisMock(options?: CreateRedisMockOptions): Promise<RedisMock>
```

**Options (`CreateRedisMockOptions`)**:

| Parameter       | Type                                     | Default     | Description                                                  |
| :-------------- | :--------------------------------------- | :---------- | :----------------------------------------------------------- |
| `cluster`       | `{ masters: number; replicas?: number }` | `undefined` | When set, builds a cluster mock instead of a standalone one. |
| `databaseCount` | `number`                                 | `16`        | Standalone-only: logical database count.                     |
| `port`          | `number`                                 | `0`         | Standalone bind port (`0` = OS-assigned).                    |
| `basePort`      | `number`                                 | `0`         | Cluster base port (`0` = each node OS-assigned).             |
| `logger`        | `Pick<Logger, 'error'>`                  | `undefined` | Optional logger.                                             |

---

### `createRedisServer`

Runs a real, **listening** server. Standalone by default — wires
`RedisServerState` (16 databases), the command executor, and a `Resp2Server`,
then listens, returning a `{ host, port, state, server, close() }` handle. Pass
`cluster` to build and start a whole cluster instead; that overload resolves to
a live `RedisCluster` (same shape `createRedisCluster` returns, already
listening).

```typescript
// standalone
createRedisServer(options?: CreateRedisServerOptions): Promise<RedisServerHandle>
// cluster
createRedisServer(options: CreateRedisServerClusterOptions): Promise<RedisCluster>
```

**Standalone options (`CreateRedisServerOptions`)**:

| Parameter       | Type                    | Default     | Description                                  |
| :-------------- | :---------------------- | :---------- | :------------------------------------------- |
| `port`          | `number`                | `0`         | Bind port (`0` = OS-assigned).               |
| `databaseCount` | `number`                | `16`        | Logical database count (matches real Redis). |
| `logger`        | `Pick<Logger, 'error'>` | `undefined` | Optional logger.                             |

**Cluster options (`CreateRedisServerClusterOptions`)**:

| Parameter          | Type                                     | Default      | Description                                      |
| :----------------- | :--------------------------------------- | :----------- | :----------------------------------------------- |
| `cluster`          | `{ masters: number; replicas?: number }` | **Required** | Builds a cluster instead of a standalone server. |
| `basePort`         | `number`                                 | `0`          | Cluster base port (`0` = each node OS-assigned). |
| `databasesPerNode` | `number`                                 | `1`          | Databases per cluster node.                      |
| `logger`           | `Pick<Logger, 'error'>`                  | `undefined`  | Optional logger.                                 |

## Requirements

- Node.js >= 24

## Development

```bash
# Install dependencies
npm install

# Build
npm run build

# Run tests
npm test

# Lint and format
npm run lint
npm run format

# Run integration tests (mock backend)
npm run test:integration:mock

# Run integration tests (real Redis)
# Requires a Redis cluster — start one with docker-compose.test.yml first:
#   docker compose -f docker-compose.test.yml up -d --wait
npm run test:integration:real

# Run all tests
npm run test:all
```

> **CI** runs four jobs on every push and pull request: lint + format check,
> unit tests, mock-backend integration tests, and real-backend integration
> tests against a Redis cluster spun up via `docker-compose.test.yml`.

## Further Documentation

For more details on the architecture, testing infrastructure, and command support, see:

- [Architecture](docs/ARCHITECTURE.md) — layers, command pipeline, execution policies, cluster routing, RESP2/RESP3, and diagrams
- [Detailed Command Implementation Status](docs/COMMANDS.md)
- [Integration Testing Infrastructure](docs/TEST-INTEGRATION.md)

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) before submitting a pull request.

## License

MIT - see [LICENSE](LICENSE) for details.
