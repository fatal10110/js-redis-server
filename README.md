# valkey-server

[![CI](https://github.com/fatal10110/valkey-server/actions/workflows/ci.yml/badge.svg)](https://github.com/fatal10110/valkey-server/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/valkey-server.svg)](https://www.npmjs.com/package/valkey-server)
[![npm downloads](https://img.shields.io/npm/dm/valkey-server.svg)](https://www.npmjs.com/package/valkey-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js Version](https://img.shields.io/node/v/valkey-server.svg)](https://nodejs.org)

▶ **[Try the interactive browser demo](https://fatal10110.github.io/valkey-server/)** —
the whole server runs in your browser (no install, no network): type Redis
commands in an xterm REPL, run Lua `EVAL`, toggle single/cluster mode and watch
`MOVED` routing, and open multiple tabs that share one keyspace so `MONITOR` /
`SUBSCRIBE` / `BLPOP` observe each other.

A real, in-memory **Valkey**/Redis-compatible server in pure JavaScript. It starts
instantly with no Valkey or Redis installation, so **the main use case is testing** — point
your normal Valkey/Redis client at it instead of a real server, and your tests run fast,
isolated, and reproducible.

```typescript
import { createRedisMock, createValkeyMock } from 'valkey-server' // createValkeyMock === createRedisMock
import { Redis } from 'ioredis'

const mock = await createRedisMock()
const redis = new Redis(mock.addresses()[0])

await redis.set('foo', 'bar')
await redis.get('foo') // 'bar'

redis.disconnect()
await mock.close()
```

That's the recommended path: a real server + your real client over a real
socket, so your client's own encoding and parsing are exercised exactly as in
production. Jump to [Use as a Redis mock in tests](#use-as-a-redis-mock-in-tests).

## Table of Contents

- [Why](#why)
- [Features](#features)
- [Installation](#installation)
- [Migrating from `js-redis-server`](#migrating-from-js-redis-server)
- [Use as a Redis mock in tests](#use-as-a-redis-mock-in-tests)
  - [Connecting your client](#connecting-your-client)
  - [node:test](#nodetest)
  - [vitest / jest](#vitest--jest)
  - [Cluster mocks](#cluster-mocks)
  - [Compatibility profiles](#compatibility-profiles)
  - [Seeding](#seeding)
  - [`createRedisMock` options](#createredismock-options)
- [Experimental: socketless client mocks](#experimental-socketless-client-mocks)
  - [`createIoredisMock` — ioredis-mock replacement](#createioredismock--ioredis-mock-replacement)
  - [`createNodeRedisMock` — node-redis in-memory mock](#createnoderedismock--node-redis-in-memory-mock)
  - [`createInMemoryClient` — our own socketless client](#createinmemoryclient--our-own-socketless-client)
- [Running a server (not a test mock)](#running-a-server-not-a-test-mock)
- [Supported Commands](docs/COMMANDS.md)
- [Requirements](#requirements)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Why

- **No real Redis to install, start, or clean up** — it's in-memory and starts in milliseconds.
- **Isolated and reproducible** — a fresh keyspace per test, reset between tests.
- **High fidelity** — your real client talks RESP over a real socket, so client-side encoding/parsing is part of the test.
- **Standalone and cluster** — same API, just pass a `cluster` option.

## Features

- **RESP2 and RESP3 protocols** - Per-session version negotiation via `HELLO`
- **Standalone and Cluster modes** - Run a single server or a full cluster
- **Redis / Valkey compatibility profiles** - Pin implemented command behavior to
  older Redis or Valkey versions
- **Lua scripting support** - Execute Redis Lua scripts via WebAssembly
- **No external dependencies** - Pure JavaScript, no Redis installation needed
- **TypeScript support** - Ships with full type definitions

## Installation

```bash
npm install valkey-server
```

## Migrating from `js-redis-server`

This package was renamed from [`js-redis-server`](https://www.npmjs.com/package/js-redis-server) to `valkey-server` (Valkey-first). The API is the same:

```bash
npm uninstall js-redis-server
npm install valkey-server
```

```diff
- import { createRedisMock } from 'js-redis-server'
+ import { createRedisMock, createValkeyMock } from 'valkey-server'
```

`createRedisMock` remains the stable API. Prefer the `createValkeyMock` alias for new code — it is the same function.

> **Note:** The official Valkey binary is also named `valkey-server`. That name collision is intentional for this npm package / CLI.

## Use as a Redis mock in tests

`createRedisMock()` owns the whole lifecycle: it spins up a standalone server
(16 databases, random free port) or a whole cluster, seeds data, and resets
between tests — you just connect your real client library to it.

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
(ioredis sends `HELLO 3`; node-redis takes a `RESP: 3` option) — negotiated
per-connection, no special setup.

### node:test

```typescript
import { test, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { createRedisMock, type RedisMock } from 'valkey-server'

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
import { createRedisMock, type RedisMock } from 'valkey-server'

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
[Compatibility Profiles](docs/API.md#compatibility-profiles).

Supported presets: `redis-6.2`, `redis-7.0`, `redis-7.2`, `redis-7.4`,
`redis-8.0`, `valkey-8.0`, and `valkey-9.0`.

Valkey profiles model the Redis 7.0-era gates as enabled:

| Profile | Redis 7.0 command/subcommand/option gates | Valkey-only modeled gate |
| --- | --- | --- |
| `valkey-8.0` | enabled | cluster multi-DB disabled |
| `valkey-9.0` | enabled | cluster multi-DB enabled |

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
```

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

> ⚠️ **Not recommended.** Prefer [`createRedisMock`](#use-as-a-redis-mock-in-tests) + a real client unless you have a specific reason not to.

| Helper                 | Looks like      | How                                                        |
| :--------------------- | :-------------- | :--------------------------------------------------------- |
| `createIoredisMock`    | `ioredis`       | the **real** ioredis client over a fake `net.Socket`       |
| `createNodeRedisMock`  | `node-redis`    | a hand-written facade mirroring node-redis' public surface |
| `createInMemoryClient` | our own bespoke | a thin client that returns native JS replies, no RESP      |

```typescript
import { createIoredisMock, createNodeRedisMock, createInMemoryClient } from 'valkey-server'
```

Need to drive an existing `createRedisMock()` keyspace? Construct `InMemoryRedisClient` directly with that mock's `state` and an executor from `valkey-server/core`.

## Running a server (not a test mock)

Need a real, **listening** server a separate process connects to (a CLI, a dev
tool), or want to assemble the pipeline by hand with custom commands/policies?
That lives in the **[Server & Low-Level API](docs/API.md)** doc:
`createRedisServer`, `createRedisCluster`, the CLI, `Resp2Server`,
`RedisServerState`, and package entry points.

## Requirements

- Node.js >= 24

## Development

```bash
npm install
npm run build
npm test
npm run lint
npm run format
npm run test:integration:mock
npm run test:integration:real
npm run test:all
```

## Further Documentation

- [Server & Low-Level API](docs/API.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Detailed Command Implementation Status](docs/COMMANDS.md)
- [Integration Testing Infrastructure](docs/TEST-INTEGRATION.md)

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) before submitting a pull request.

## License

MIT - see [LICENSE](LICENSE) for details.
