# Server & Low-Level API

The [README](../README.md) covers the primary use case — an in-memory Redis
mock for tests. This document covers the rest: running a **real, listening**
server, building a cluster, and assembling the pipeline by hand.

> Most users never need anything here. If you're writing tests, use
> [`createRedisMock`](../README.md#use-as-a-redis-mock-in-tests).

## Table of Contents

- [Running a Standalone Server](#running-a-standalone-server)
- [Running a Cluster](#running-a-cluster)
- [CLI](#cli)
- [Compatibility Profiles](#compatibility-profiles)
- [Package Entry Points](#package-entry-points)
- [Advanced: Assembling the Pipeline by Hand](#advanced-assembling-the-pipeline-by-hand)
- [API Reference](#api-reference)
  - [`createRedisServer`](#createredisserver)
  - [`createRedisCluster`](#createrediscluster)
  - [`Resp2Server`](#resp2server)
  - [`RedisServerState`](#redisserverstate)

## Running a Standalone Server

To run a real server you connect to (not a test mock), use `createRedisServer()`
— it wires the state, executor, and `Resp2Server` for you and starts listening:

```typescript
import { createRedisServer } from 'js-redis-server'

const { host, port, close } = await createRedisServer({ port: 6379 })
console.log(`Redis server listening at ${host}:${port}`)

// Cleanup
await close()
```

## Running a Cluster

Pass `cluster` to `createRedisServer` — it builds **and** starts the cluster,
returning a live handle:

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
> [`createRedisCluster()`](#createrediscluster) builder returns an un-started
> `RedisCluster` you call `.listen()` on yourself. (`buildRedisCluster` is a
> deprecated alias of it.)

## CLI

```bash
# Run a single server on default port 6379
npx js-redis-server

# Run on a specific port
npx js-redis-server --port 6380

# Run a cluster with 3 masters
npx js-redis-server --cluster --masters 3

# Run a cluster with replicas
npx js-redis-server --cluster --masters 3 --slaves 1

# Run as Redis 6.2 compatibility
npx js-redis-server --compat redis-6.2
```

CLI options:

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
  --compat <preset>       Compatibility profile (or REDIS_COMPAT env)
  -d, --debug            Enable debug logging
  -h, --help             Show help
```

## Compatibility Profiles

Pass `compatibility` to pin implemented command behavior to a Redis or Valkey
target:

```typescript
await createRedisServer({ compatibility: 'redis-6.2' })

const cluster = createRedisCluster({
  masters: 3,
  basePort: 30000,
  compatibility: 'valkey-9.0',
  databasesPerNode: 16,
})
```

Supported presets are `redis-6.2`, `redis-7.0`, `redis-7.2`, `redis-7.4`,
`redis-8.0`, `valkey-8.0`, and `valkey-9.0`. The default is `redis-8.0`.

Profiles gate implemented commands, subcommands, options, and known behavioral
differences. Unsupported commands are still unsupported regardless of profile.

## Package Entry Points

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

## Advanced: Assembling the Pipeline by Hand

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

| Parameter       | Type                    | Default       | Description                                  |
| :-------------- | :---------------------- | :------------ | :------------------------------------------- |
| `port`          | `number`                | `0`           | Bind port (`0` = OS-assigned).               |
| `databaseCount` | `number`                | `16`          | Logical database count (matches real Redis). |
| `compatibility` | `CompatibilitySpec`     | `'redis-8.0'` | Redis / Valkey compatibility profile.        |
| `logger`        | `Pick<Logger, 'error'>` | `undefined`   | Optional logger.                             |

**Cluster options (`CreateRedisServerClusterOptions`)**:

| Parameter          | Type                                     | Default       | Description                                      |
| :----------------- | :--------------------------------------- | :------------ | :----------------------------------------------- |
| `cluster`          | `{ masters: number; replicas?: number }` | **Required**  | Builds a cluster instead of a standalone server. |
| `basePort`         | `number`                                 | `0`           | Cluster base port (`0` = each node OS-assigned). |
| `databasesPerNode` | `number`                                 | `1`           | Databases per cluster node.                      |
| `compatibility`    | `CompatibilitySpec`                      | `'redis-8.0'` | Redis / Valkey compatibility profile.            |
| `logger`           | `Pick<Logger, 'error'>`                  | `undefined`   | Optional logger.                                 |

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
| `compatibility`     | `CompatibilitySpec`     | `'redis-8.0'` | Redis / Valkey compatibility profile.                  |
| `logger`            | `Pick<Logger, 'error'>` | `undefined`   | Optional logger.                                       |

---

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

| Parameter         | Type                   | Default       | Description                                 |
| :---------------- | :--------------------- | :------------ | :------------------------------------------ |
| `databaseCount`   | `number`               | `1`           | Number of databases to initialize.          |
| `compatibility`   | `CompatibilitySpec`    | `'redis-8.0'` | Redis / Valkey compatibility profile.       |
| `clusterTopology` | `RedisClusterTopology` | `undefined`   | Optional cluster topology for slot routing. |
| `pubsubBroker`    | `RedisPubSubBroker`    | `undefined`   | Optional broker for pub/sub operations.     |
| `scriptCache`     | `RedisScriptCache`     | `undefined`   | Optional cache for Lua scripts.             |
