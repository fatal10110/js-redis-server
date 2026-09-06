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
import { createRedisServer } from 'valkey-server'

const { host, port, close } = await createRedisServer({ port: 6379 })
console.log(`Redis server listening at ${host}:${port}`)

await close()
```

## Running a Cluster

Pass `cluster` to `createRedisServer` — it builds **and** starts the cluster,
returning a live handle:

```typescript
import { createRedisServer } from 'valkey-server'

const cluster = await createRedisServer({
  cluster: { masters: 3, replicas: 1 },
  basePort: 30000,
})

console.log(cluster.nodes.map(n => `${n.host}:${n.port}`))

await cluster.close()
```

> Need control over _when_ the cluster starts listening? The lower-level
> [`createRedisCluster()`](#createrediscluster) builder returns an un-started
> `RedisCluster` you call `.listen()` on yourself. (`buildRedisCluster` is a
> deprecated alias of it.)

## CLI

```bash
npx valkey-server
npx valkey-server --port 6380
npx valkey-server --cluster --masters 3
npx valkey-server --cluster --masters 3 --slaves 1
npx valkey-server --compat redis-6.2
```

## Compatibility Profiles

Pass `compatibility` to pin implemented command behavior to a Redis or Valkey
target. Supported presets: `redis-6.2`, `redis-7.0`, `redis-7.2`, `redis-7.4`,
`redis-8.0`, `valkey-8.0`, and `valkey-9.0`. The default is `redis-8.0`.

See the README for the full gate matrix.

## Package Entry Points

**Root (`valkey-server`)** — curated consumer surface:

```typescript
import {
  createRedisMock,
  createValkeyMock,
  createRedisCluster,
  RedisCommandError,
} from 'valkey-server'
```

**Core (`valkey-server/core`)** — building blocks for assembling the pipeline by hand:

```typescript
import {
  Resp2Server,
  RedisServerState,
  createRedisCommandExecutor,
  defineCommand,
  t,
} from 'valkey-server/core'
```

## Advanced: Assembling the Pipeline by Hand

```typescript
import {
  RedisServerState,
  createRedisCommandExecutor,
  Resp2Server,
} from 'valkey-server/core'

const state = new RedisServerState()
const executor = createRedisCommandExecutor()
const server = new Resp2Server({ server: state, executor })

await server.listen(6379)
await server.close()
```

## API Reference

### `createRedisServer`

```typescript
createRedisServer(options?: CreateRedisServerOptions): Promise<RedisServerHandle>
createRedisServer(options: CreateRedisServerClusterOptions): Promise<RedisCluster>
```

### `createRedisCluster`

```typescript
createRedisCluster(options: RedisClusterOptions): RedisCluster
```

### `Resp2Server`

> Imported from `valkey-server/core`, not the package root.

```typescript
import { Resp2Server } from 'valkey-server/core'
```

### `RedisServerState`

> Imported from `valkey-server/core`, not the package root.

```typescript
import { RedisServerState } from 'valkey-server/core'
```
