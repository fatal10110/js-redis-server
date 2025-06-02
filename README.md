# JS Redis Server

A lightweight, in-memory Redis-compatible server implementation in TypeScript, designed for testing and development purposes. This server provides a mock Redis environment that can be used with popular Redis clients like ioredis and node-redis.

## 🚀 Features

- **Redis-compatible RESP protocol** - Full RESP (REdis Serialization Protocol) implementation
- **In-memory data storage** - Fast, memory-based data operations
- **Cluster support** - Master-slave replication with configurable cluster topology
- **All major data types** - Strings, Lists, Sets, Sorted Sets, and Hashes
- **60+ Redis commands implemented** - Comprehensive command set for testing
- **TypeScript implementation** - Type-safe codebase with modern Node.js features
- **Perfect for testing** - Drop-in replacement for Redis in test environments
- **Zero dependencies** - Lightweight implementation focused on testing needs

## 📦 Installation

```bash
npm install js-redis-server
```

## 🚀 Quick Start

### Basic Server Setup

```typescript
import { ClusterNetwork } from 'js-redis-server'

async function startRedisServer() {
  const cluster = new ClusterNetwork(console)

  // Initialize cluster with 3 masters and no slaves
  await cluster.init({ masters: 3, slaves: 0 })

  // Get cluster information
  console.log(
    'Cluster nodes:',
    Array.from(cluster.getAll()).map(node => `${node.host}:${node.port}`),
  )

  // Graceful shutdown
  process.on('SIGINT', () => cluster.shutdown())
  process.on('SIGTERM', () => cluster.shutdown())
}

startRedisServer().catch(console.error)
```

### Using with Redis Clients

#### IORedis Client

```typescript
import Redis from 'ioredis'

const redis = new Redis({
  port: 6379,
  host: 'localhost',
  retryDelayOnFailover: 100,
  maxRetriesPerRequest: 3,
})

// String operations
await redis.set('user:1', 'John Doe')
const user = await redis.get('user:1')

// Hash operations
await redis.hset('user:1:profile', 'name', 'John', 'age', '30')
const profile = await redis.hgetall('user:1:profile')

// List operations
await redis.lpush('tasks', 'task1', 'task2', 'task3')
const tasks = await redis.lrange('tasks', 0, -1)

// Set operations
await redis.sadd('tags', 'redis', 'nodejs', 'typescript')
const tags = await redis.smembers('tags')

// Sorted set operations
await redis.zadd('leaderboard', 100, 'player1', 200, 'player2')
const leaders = await redis.zrange('leaderboard', 0, -1, 'WITHSCORES')
```

#### Node-Redis Client

```typescript
import { createClient } from 'redis'

const client = createClient({
  url: 'redis://localhost:6379',
})

await client.connect()

// String operations
await client.set('key', 'value')
const value = await client.get('key')

// Hash operations
await client.hSet('hash', { field1: 'value1', field2: 'value2' })
const hash = await client.hGetAll('hash')

await client.disconnect()
```

## ✅ Implemented Commands

### Connection Commands

- **PING** - Test connection
- **QUIT** - Close client connection
- **INFO** - Get server information

### String Commands

- **SET** - Set string value with options (EX, PX, NX, XX, GET)
- **GET** - Get string value
- **MGET** - Get multiple string values
- **MSET** - Set multiple string values
- **MSETNX** - Set multiple strings only if none exist
- **APPEND** - Append to string
- **STRLEN** - Get string length
- **INCR/DECR** - Increment/decrement integer by 1
- **INCRBY/DECRBY** - Increment/decrement integer by amount
- **INCRBYFLOAT** - Increment float by amount
- **GETSET** - Set new value and return old value

### Key Commands

- **DEL** - Delete keys
- **EXISTS** - Check if keys exist
- **TYPE** - Get key type
- **TTL** - Get time to live in seconds
- **EXPIRE** - Set key expiration in seconds
- **EXPIREAT** - Set key expiration at timestamp
- **FLUSHDB** - Remove all keys from current database
- **FLUSHALL** - Remove all keys from all databases

### Hash Commands

- **HSET** - Set hash field
- **HGET** - Get hash field value
- **HMSET** - Set multiple hash fields
- **HMGET** - Get multiple hash field values
- **HGETALL** - Get all hash fields and values
- **HDEL** - Delete hash fields
- **HEXISTS** - Check if hash field exists
- **HKEYS** - Get all hash field names
- **HVALS** - Get all hash values
- **HLEN** - Get hash field count
- **HINCRBY** - Increment hash field by integer
- **HINCRBYFLOAT** - Increment hash field by float

### List Commands

- **LPUSH/RPUSH** - Push elements to list head/tail
- **LPOP/RPOP** - Pop element from list head/tail
- **LLEN** - Get list length
- **LRANGE** - Get list elements by range
- **LINDEX** - Get list element by index
- **LSET** - Set list element by index
- **LREM** - Remove list elements
- **LTRIM** - Trim list to range

### Set Commands

- **SADD** - Add members to set
- **SREM** - Remove members from set
- **SMEMBERS** - Get all set members
- **SISMEMBER** - Check if member exists in set
- **SCARD** - Get set member count
- **SPOP** - Remove and return random member
- **SRANDMEMBER** - Get random member(s)
- **SINTER** - Intersect multiple sets
- **SUNION** - Union multiple sets
- **SDIFF** - Difference of multiple sets
- **SMOVE** - Move member between sets

### Sorted Set Commands

- **ZADD** - Add members with scores
- **ZREM** - Remove members
- **ZRANGE** - Get members by rank range
- **ZREVRANGE** - Get members by rank range (reversed)
- **ZRANK/ZREVRANK** - Get member rank
- **ZSCORE** - Get member score
- **ZCARD** - Get sorted set member count
- **ZINCRBY** - Increment member score

## 🔧 Development

### Prerequisites

- Node.js v22 or higher
- npm or yarn

### Setup

```bash
git clone https://github.com/fatal10110/js-redis-server.git
cd js-redis-server
npm install
```

### Available Scripts

- `npm test` - Run comprehensive test suite (221 tests)
- `npm run build` - Build TypeScript to JavaScript
- `npm start` - Start the Redis server
- `npm run lint` - Run ESLint code analysis
- `npm run format` - Format code with Prettier

### Testing

The project includes extensive test coverage with 221 passing tests covering:

- String commands (22KB+ of tests)
- Hash commands (13KB+ of tests)
- Sorted set tests (15KB+ of tests)
- Set commands (12KB+ of tests)
- List commands (10KB+ of tests)
- Key operations
- Integration tests for all data types

All tests use Node.js built-in test runner and assertion library following modern testing practices.

## 🏗️ Architecture

The project follows a modular architecture with:

- **Core cluster management** - Network topology and node management
- **Command processing** - RESP protocol parsing and command execution
- **Data structures** - In-memory implementations of Redis data types
- **Type safety** - Full TypeScript implementation with strict types
- **Error handling** - Comprehensive error types matching Redis behavior

## 📈 Performance

JS Redis Server is optimized for performance with:

- In-memory data structures optimized for speed
- Efficient RESP protocol implementation
- Minimal object allocation in hot paths
- Early returns and linear code flow
- Comprehensive benchmarking in tests

## 🗺️ Roadmap

### Planned Features

- **Lua scripting** - EVAL and EVALSHA commands
- **Transaction support** - MULTI/EXEC transaction blocks
- **Pub/Sub messaging** - PUBLISH/SUBSCRIBE commands
- **Persistence** - RDB snapshots and AOF logging
- **Advanced data structures** - Streams, HyperLogLog, Geospatial
- **Enhanced cluster features** - Full cluster management commands
- **Monitoring tools** - MONITOR, SLOWLOG, and metrics

For detailed implementation status, see [COMMANDS.md](COMMANDS.md).

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues.

### Contribution Guidelines

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Follow existing code style and patterns
4. Add comprehensive tests for new functionality
5. Ensure all tests pass (`npm test`)
6. Update documentation as needed
7. Submit a pull request

### Code Style

- Use early returns to avoid nested conditions
- Prefer `for...of` with `Object.entries()` over `for...in`
- Use strict TypeScript types
- Follow the established error handling patterns
- Write comprehensive tests using Node.js built-in test runner

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with TypeScript for type safety and developer experience
- Uses the RESP protocol for Redis compatibility
- Inspired by the official Redis implementation
- Community-driven development and testing
