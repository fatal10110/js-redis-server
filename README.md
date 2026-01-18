# js-redis-server

[![CI](https://github.com/fatal10110/js-redis-server/actions/workflows/ci.yml/badge.svg)](https://github.com/fatal10110/js-redis-server/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/js-redis-server.svg)](https://www.npmjs.com/package/js-redis-server)
[![npm downloads](https://img.shields.io/npm/dm/js-redis-server.svg)](https://www.npmjs.com/package/js-redis-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js Version](https://img.shields.io/node/v/js-redis-server.svg)](https://nodejs.org)

In-memory Redis-compatible server implementation in JavaScript. Useful for testing and development without requiring a real Redis instance.

## Features

- **Redis-compatible protocol** - Works with any Redis client (ioredis, node-redis, etc.)
- **Standalone and Cluster modes** - Run a single server or a full cluster
- **Lua scripting support** - Execute Redis Lua scripts via WebAssembly
- **No external dependencies** - Pure JavaScript, no Redis installation needed
- **Dual package** - Supports both ESM and CommonJS

## Installation

```bash
npm install js-redis-server
```

## Quick Start

### As a Library

```typescript
import { createCustomCommander, Resp2Transport } from 'js-redis-server'

const logger = {
  info: console.log,
  error: console.error,
}

const factory = await createCustomCommander(logger)
const transport = new Resp2Transport(logger, factory.createCommander())

await transport.listen(6379)
console.log('Redis server listening on port 6379')

// Cleanup
await transport.close()
await factory.shutdown()
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
  -h, --help             Show help
```

## Cluster Mode

```typescript
import { ClusterNetwork } from 'js-redis-server'

const logger = { info: console.log, error: console.error }
const cluster = new ClusterNetwork(logger)

await cluster.init({
  masters: 3,
  slaves: 1,
  basePort: 30000,
})

// Get all node addresses
const nodes = cluster.getAll()
console.log(nodes.map(n => `${n.host}:${n.port}`))

// Cleanup
await cluster.shutdown()
```

## Supported Commands

### Strings
`GET`, `SET`, `MGET`, `MSET`, `MSETNX`, `APPEND`, `STRLEN`, `GETSET`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `DECR`, `DECRBY`

### Hashes
`HGET`, `HSET`, `HMGET`, `HMSET`, `HDEL`, `HEXISTS`, `HGETALL`, `HINCRBY`, `HINCRBYFLOAT`, `HKEYS`, `HLEN`, `HVALS`

### Lists
`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LTRIM`

### Sets
`SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SPOP`, `SRANDMEMBER`, `SMOVE`, `SDIFF`, `SINTER`, `SUNION`

### Sorted Sets
`ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZCARD`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREMRANGEBYSCORE`, `ZINCRBY`

### Keys
`DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `TTL`, `PTTL`, `TYPE`

### Server
`PING`, `QUIT`, `INFO`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`

### Cluster
`CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER SLOTS`, `CLUSTER SHARDS`

### Scripting
`EVAL`, `EVALSHA`, `SCRIPT LOAD`

### Client
`CLIENT SETNAME`

## Requirements

- Node.js >= 22

## Development

```bash
# Install dependencies
npm install

# Build
npm run build

# Run tests
npm test

# Run integration tests (mock backend)
npm run test:integration:mock

# Run integration tests (real Redis)
npm run test:integration:real

# Run all tests
npm run test:all
```

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) before submitting a pull request.

## License

MIT - see [LICENSE](LICENSE) for details.
