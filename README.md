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
  - [As a Library](#as-a-library)
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

### As a Library

```typescript
import {
  RedisServerState,
  createRedisCommandExecutor,
  Resp2Server,
} from 'js-redis-server'

const logger = {
  info: console.log,
  error: console.error,
  debug: console.debug,
}

const state = new RedisServerState()
const executor = createRedisCommandExecutor()
const server = new Resp2Server({ server: state, executor, logger })

await server.listen(6379)
console.log(`Redis server listening at ${server.getAddress()}`)

// Cleanup
await server.close()
```

### As a Test Mock (recommended)

For test suites, skip the manual wiring above and use the `createRedisMock()`
facade. It spins up a standalone server (16 databases, random free port) or a
whole cluster, seeds data, and resets between tests:

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

```typescript
import { buildRedisCluster } from 'js-redis-server'

const logger = {
  info: console.log,
  error: console.error,
  debug: console.debug,
}

const cluster = buildRedisCluster({
  masters: 3,
  replicasPerMaster: 1,
  basePort: 30000,
  logger,
})

await cluster.listen()

// Get all node addresses
console.log(cluster.nodes.map(n => `${n.host}:${n.port}`))

// Cleanup
await cluster.close()
```

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
  url: 'redis://127.0.0.1:6379'
})
await client.connect()

// Cluster Node
const cluster = createCluster({
  rootNodes: [
    { url: 'redis://127.0.0.1:30000' },
    { url: 'redis://127.0.0.1:30001' },
    { url: 'redis://127.0.0.1:30002' },
  ]
})
await cluster.connect()
```

## Use as a Redis mock in tests

`js-redis-server` starts instantly with no external Redis, so it makes a clean
drop-in mock for test suites. The `createRedisMock()` facade owns the server
lifecycle, seeding, and reset-between-tests; you just connect your real client
library to it.

`RedisMock` surface:

| Member | Description |
| :--- | :--- |
| `host` / `port` / `url` | Connection coordinates of the (first) node. |
| `connectionOptions()` | ioredis-shaped `{ host, port }` for a single client. |
| `clusterNodes()` | Seed-node list for `Redis.Cluster` (one entry for standalone). |
| `seed(entries)` | Preload data (see [Seeding](#seeding)). |
| `flush()` / `reset()` | Clear all keyspace data between tests. |
| `close()` | Shut down the server / cluster. |
| `state` / `nodes` | Escape hatches to the underlying `RedisServerState` / node handles. |

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

### Seeding

`seed()` takes an explicit entries array — you supply keys, types, values, and
optional `ttlMs` / `db`; the mock owns placement (including cluster slot
routing) and the internal value conversion.

```typescript
type SeedEntry =
  | { key: string; type: 'string'; value: string | number; ttlMs?: number; db?: number }
  | { key: string; type: 'hash';   value: Record<string, string | number>; ttlMs?: number; db?: number }
  | { key: string; type: 'list';   value: (string | number)[]; ttlMs?: number; db?: number }
  | { key: string; type: 'set';    value: (string | number)[]; ttlMs?: number; db?: number }
  | { key: string; type: 'zset';   value: Record<string, number>; ttlMs?: number; db?: number }
```

`db` selects the logical database (standalone mocks). Streams are not seedable
yet. For anything beyond these shapes, drive your client directly or reach for
the `mock.state` escape hatch.

## API Reference

### `Resp2Server`

Creates a TCP server running the RESP2 protocol.

```typescript
new Resp2Server(options: Resp2ServerOptions)
```

**Options (`Resp2ServerOptions`)**:

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `server` | `RedisServerState` | Yes | The database server state containing database instances. |
| `executor` | `CommandExecutor` | Yes | The command executor handling pipeline and execution policies. |
| `logger` | `Pick<Logger, 'error'>` | No | Optional logger for error logging. |
| `encoder` | `RespEncodeOptions` | No | Custom RESP protocol encoding options. |

---

### `RedisServerState`

Holds the database and keyspace state for the server.

```typescript
new RedisServerState(options?: RedisServerStateOptions)
```

**Options (`RedisServerStateOptions`)**:

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `databaseCount` | `number` | `1` | Number of databases to initialize. |
| `clusterTopology` | `RedisClusterTopology` | `undefined` | Optional cluster topology for slot routing. |
| `pubsubBroker` | `RedisPubSubBroker` | `undefined` | Optional broker for pub/sub operations. |
| `scriptCache` | `RedisScriptCache` | `undefined` | Optional cache for Lua scripts. |

---

### `buildRedisCluster`

Utility function to build and configure a complete Redis cluster in-memory.

```typescript
buildRedisCluster(options: RedisClusterOptions): RedisCluster
```

**Options (`RedisClusterOptions`)**:

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `masters` | `number` | **Required** | Number of master nodes in the cluster. |
| `replicasPerMaster` | `number` | `0` | Replicas per master node. |
| `basePort` | `number` | **Required** | Base port range. If `0`, random OS ports are assigned. |
| `host` | `string` | `'127.0.0.1'` | Host address to bind to. |
| `databasesPerNode` | `number` | `1` | Number of databases per cluster node. |
| `logger` | `Pick<Logger, 'error'>` | `undefined` | Optional logger. |

---

### `createRedisMock`

High-level test-mock facade. Returns a `RedisMock` wrapping a standalone server
(default) or a cluster.

```typescript
createRedisMock(options?: CreateRedisMockOptions): Promise<RedisMock>
```

**Options (`CreateRedisMockOptions`)**:

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `cluster` | `{ masters: number; replicas?: number }` | `undefined` | When set, builds a cluster mock instead of a standalone one. |
| `databaseCount` | `number` | `16` | Standalone-only: logical database count. |
| `port` | `number` | `0` | Standalone bind port (`0` = OS-assigned). |
| `basePort` | `number` | `0` | Cluster base port (`0` = each node OS-assigned). |
| `logger` | `Pick<Logger, 'error'>` | `undefined` | Optional logger. |

---

### `createRedisServer`

Standalone analog to `buildRedisCluster`: wires `RedisServerState` (16 databases
by default), the command executor, and a `Resp2Server`, then listens. Returns a
`{ host, port, state, server, close() }` handle.

```typescript
createRedisServer(options?: CreateRedisServerOptions): Promise<RedisServerHandle>
```

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `port` | `number` | `0` | Bind port (`0` = OS-assigned). |
| `databaseCount` | `number` | `16` | Logical database count (matches real Redis). |
| `logger` | `Pick<Logger, 'error'>` | `undefined` | Optional logger. |


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
