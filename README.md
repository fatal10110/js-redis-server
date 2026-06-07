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

## Supported Commands

### Strings
`APPEND`, `DECR`, `DECRBY`, `GET`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `MGET`, `MSET`, `MSETNX`, `PSETEX`, `SET`, `SETEX`, `SETNX`, `SETRANGE`, `STRLEN`

### Hashes
`HDEL`, `HEXISTS`, `HGET`, `HGETALL`, `HINCRBY`, `HINCRBYFLOAT`, `HKEYS`, `HLEN`, `HMGET`, `HMSET`, `HSCAN`, `HSET`, `HSETNX`, `HSTRLEN`, `HVALS`

### Lists
`LINDEX`, `LLEN`, `LPOP`, `LPUSH`, `LPUSHX`, `LRANGE`, `LREM`, `LSET`, `LTRIM`, `RPOP`, `RPOPLPUSH`, `RPUSH`, `RPUSHX`

### Sets
`SADD`, `SCARD`, `SDIFF`, `SDIFFSTORE`, `SINTER`, `SINTERSTORE`, `SISMEMBER`, `SMEMBERS`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SREM`, `SSCAN`, `SUNION`, `SUNIONSTORE`

### Sorted Sets
`ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZPOPMAX`, `ZPOPMIN`, `ZRANGE`, `ZRANGEBYSCORE`, `ZRANK`, `ZREM`, `ZREMRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANK`, `ZSCAN`, `ZSCORE`

### Keys
`DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `KEYS`, `PERSIST`, `PEXPIRE`, `PEXPIREAT`, `PTTL`, `RENAME`, `RENAMENX`, `SCAN`, `TTL`, `TYPE`

### Server
`DBSIZE`, `FLUSHALL`, `FLUSHDB`, `INFO`, `PING`, `QUIT`

### Transactions
`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

### Scripting
`EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`

### Cluster
`CLUSTER INFO`, `CLUSTER MYID`, `CLUSTER NODES`, `CLUSTER SHARDS`, `CLUSTER SLOTS`

### Connection
`AUTH`, `CLIENT`, `HELLO`, `RESET`, `SELECT`

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

## Contributing

Contributions are welcome! Please read the [contributing guidelines](CONTRIBUTING.md) before submitting a pull request.

## License

MIT - see [LICENSE](LICENSE) for details.
