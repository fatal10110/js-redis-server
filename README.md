# JS Redis Server

A lightweight, in-memory Redis-compatible server implementation in TypeScript, designed for testing and development purposes. This server provides a mock Redis environment that can be used with popular Redis clients like ioredis and node-redis.

## Features

- 🚀 **Redis-compatible RESP protocol** - Full RESP (REdis Serialization Protocol) implementation
- 🗄️ **In-memory data storage** - Fast, memory-based data operations
- 🔗 **Cluster support** - Master-slave replication with configurable cluster topology
- 📜 **Lua scripting** - Execute Lua scripts with EVAL and EVALSHA commands
- 📊 **All major data types** - Strings, Lists, Sets, Sorted Sets, and Hashes
- 🔒 **Transaction support** - MULTI/EXEC transaction blocks
- 📝 **Comprehensive command set** - 60+ Redis commands implemented
- 🧪 **Perfect for testing** - Drop-in replacement for Redis in test environments

## Installation

```bash
npm install js-redis-server
```

## Quick Start

### Basic Server Setup

```typescript
import { ClusterNetwork } from 'js-redis-server'

async function startRedisServer() {
  const cluster = new ClusterNetwork(console)

  // Initialize cluster with 3 masters and 2 slaves
  await cluster.init({ masters: 3, slaves: 2 })

  // Get cluster information
  console.log(
    'Cluster nodes:',
    Array.from(cluster.getAll()).map(node => ({
      address: `${node.host}:${node.port}`,
      slots: node.slotRange,
    })),
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

### Lua Scripting

```typescript
import Redis from 'ioredis'

const redis = new Redis()

// Execute Lua script
const script = `
  local key = KEYS[1]
  local increment = ARGV[1]
  local current = redis.call('GET', key) or 0
  local result = current + increment
  redis.call('SET', key, result)
  return result
`

const result = await redis.eval(script, 1, 'counter', 5)
console.log('Counter value:', result)
```

## Implemented Commands

### Connection Commands

- ✅ **PING** - Test connection with optional message
- ✅ **QUIT** - Close client connection
- ✅ **INFO** - Get server information and statistics

### String Commands

- ✅ **SET** - Set string value with options (EX, PX, NX, XX, KEEPTTL, GET)
- ✅ **GET** - Get string value
- ✅ **MGET** - Get multiple string values
- ✅ **MSET** - Set multiple string values
- ✅ **MSETNX** - Set multiple strings only if none exist
- ✅ **APPEND** - Append to string
- ✅ **STRLEN** - Get string length
- ✅ **INCR** - Increment integer by 1
- ✅ **DECR** - Decrement integer by 1
- ✅ **INCRBY** - Increment integer by amount
- ✅ **DECRBY** - Decrement integer by amount
- ✅ **INCRBYFLOAT** - Increment float by amount
- ✅ **GETSET** - Set new value and return old value

### Key Commands

- ✅ **DEL** - Delete keys
- ✅ **EXISTS** - Check if keys exist
- ✅ **TYPE** - Get key type
- ✅ **TTL** - Get time to live in seconds
- ✅ **PTTL** - Get time to live in milliseconds

### Hash Commands

- ✅ **HSET** - Set hash field
- ✅ **HGET** - Get hash field value
- ✅ **HMSET** - Set multiple hash fields
- ✅ **HMGET** - Get multiple hash field values
- ✅ **HGETALL** - Get all hash fields and values
- ✅ **HDEL** - Delete hash fields
- ✅ **HEXISTS** - Check if hash field exists
- ✅ **HKEYS** - Get all hash field names
- ✅ **HVALS** - Get all hash values
- ✅ **HLEN** - Get hash field count
- ✅ **HINCRBY** - Increment hash field by integer
- ✅ **HINCRBYFLOAT** - Increment hash field by float

### List Commands

- ✅ **LPUSH** - Push elements to list head
- ✅ **RPUSH** - Push elements to list tail
- ✅ **LPOP** - Pop element from list head
- ✅ **RPOP** - Pop element from list tail
- ✅ **LLEN** - Get list length
- ✅ **LRANGE** - Get list elements by range
- ✅ **LINDEX** - Get list element by index
- ✅ **LSET** - Set list element by index
- ✅ **LREM** - Remove list elements
- ✅ **LTRIM** - Trim list to range

### Set Commands

- ✅ **SADD** - Add members to set
- ✅ **SREM** - Remove members from set
- ✅ **SMEMBERS** - Get all set members
- ✅ **SISMEMBER** - Check if member exists in set
- ✅ **SCARD** - Get set member count
- ✅ **SPOP** - Remove and return random member
- ✅ **SRANDMEMBER** - Get random member(s)
- ✅ **SINTER** - Intersect multiple sets
- ✅ **SUNION** - Union multiple sets
- ✅ **SDIFF** - Difference of multiple sets
- ✅ **SMOVE** - Move member between sets

### Sorted Set Commands

- ✅ **ZADD** - Add members with scores
- ✅ **ZREM** - Remove members
- ✅ **ZRANGE** - Get members by rank range
- ✅ **ZREVRANGE** - Get members by rank range (reversed)
- ✅ **ZRANGEBYSCORE** - Get members by score range
- ✅ **ZREMRANGEBYSCORE** - Remove members by score range
- ✅ **ZRANK** - Get member rank
- ✅ **ZREVRANK** - Get member rank (reversed)
- ✅ **ZSCORE** - Get member score
- ✅ **ZCARD** - Get sorted set member count
- ✅ **ZINCRBY** - Increment member score

### Scripting Commands

- ✅ **EVAL** - Execute Lua script
- ✅ **EVALSHA** - Execute Lua script by SHA

### Transaction Commands

- ✅ **MULTI** - Start transaction block

## Development

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

- `npm test` - Run comprehensive test suite
- `npm run build` - Build TypeScript to JavaScript
- `npm start` - Start the Redis server
- `npm run lint` - Run ESLint code analysis
- `npm run format` - Format code with Prettier

### Testing

The project includes extensive test coverage with both unit and integration tests:

- String command tests (22KB+ of tests)
- Hash command tests (13KB+ of tests)
- Sorted set tests (15KB+ of tests)
- Set command tests (12KB+ of tests)
- List command tests (10KB+ of tests)
- Key operation tests
- Integration tests for all data types

## TODO: Future Enhancements

### 🔐 Authentication & Security

- [ ] **AUTH** command - User authentication
- [ ] **ACL** commands - Access control lists
- [ ] **CONFIG** commands - Runtime configuration
- [ ] SSL/TLS support
- [ ] Security audit logging

### 🏪 Persistence & Durability

- [ ] **SAVE** / **BGSAVE** - RDB snapshots
- [ ] **AOF** (Append Only File) persistence
- [ ] **LASTSAVE** - Last save timestamp
- [ ] **SHUTDOWN** - Graceful server shutdown
- [ ] Point-in-time recovery
- [ ] Automatic persistence scheduling

### 📡 Pub/Sub Messaging

- [ ] **PUBLISH** / **SUBSCRIBE** - Channel messaging
- [ ] **PSUBSCRIBE** / **PUNSUBSCRIBE** - Pattern subscriptions
- [ ] **PUBSUB** - Introspection commands
- [ ] Message routing and filtering
- [ ] Persistent message queues

### 🗄️ Database Management

- [ ] **SELECT** - Multiple database support
- [ ] **FLUSHDB** / **FLUSHALL** - Database clearing
- [ ] **DBSIZE** - Database key counting
- [ ] **RANDOMKEY** - Random key selection
- [ ] **KEYS** / **SCAN** - Key enumeration with patterns
- [ ] **MIGRATE** - Key migration between instances

### ⏰ Advanced Key Operations

- [ ] **EXPIRE** / **EXPIREAT** - Key expiration with flags (NX, XX, GT, LT)
- [ ] **PERSIST** - Remove key expiration
- [ ] **RENAME** / **RENAMENX** - Key renaming
- [ ] **DUMP** / **RESTORE** - Key serialization
- [ ] **OBJECT** - Key introspection
- [ ] **MEMORY** commands - Memory analysis

### 🔗 Cluster & Replication

- [ ] **CLUSTER** commands - Full cluster management
  - [ ] **CLUSTER INFO** - Cluster state information
  - [ ] **CLUSTER NODES** - Node configuration
  - [ ] **CLUSTER SLOTS** - Slot distribution
  - [ ] **CLUSTER MEET** - Node discovery
  - [ ] **CLUSTER REPLICATE** - Replica configuration
- [ ] **REPLICATION** commands
  - [ ] **SLAVEOF** / **REPLICAOF** - Master-slave setup
  - [ ] **ROLE** - Node role information
- [ ] Automatic failover
- [ ] Split-brain detection and resolution

### 📊 Monitoring & Analytics

- [ ] **MONITOR** - Real-time command monitoring
- [ ] **SLOWLOG** - Slow query logging
- [ ] **LATENCY** commands - Latency monitoring
- [ ] **CLIENT** commands - Client connection management
- [ ] **COMMAND** introspection - Command documentation
- [ ] Metrics collection and export (Prometheus)
- [ ] Performance profiling tools

### 🧮 Advanced Data Structures

- [ ] **Streams** - Redis 5.0+ streams
  - [ ] **XADD** / **XREAD** / **XRANGE**
  - [ ] **XGROUP** - Consumer groups
  - [ ] **XPENDING** - Pending message tracking
- [ ] **HyperLogLog** - Probabilistic counting
  - [ ] **PFADD** / **PFCOUNT** / **PFMERGE**
- [ ] **Geospatial** - Location-based operations
  - [ ] **GEOADD** / **GEOPOS** / **GEODIST**
  - [ ] **GEORADIUS** / **GEOHASH**
- [ ] **Bitmaps** - Bit operations
  - [ ] **SETBIT** / **GETBIT** / **BITCOUNT**
  - [ ] **BITOP** - Bitwise operations

### 🔧 Advanced Scripting

- [ ] **SCRIPT** commands - Script cache management
  - [ ] **SCRIPT LOAD** - Preload scripts
  - [ ] **SCRIPT EXISTS** - Check script existence
  - [ ] **SCRIPT FLUSH** - Clear script cache
  - [ ] **SCRIPT KILL** - Terminate running scripts
- [ ] Enhanced Lua environment
- [ ] Script debugging capabilities
- [ ] Custom function libraries

### 🚀 Performance & Scalability

- [ ] Memory optimization algorithms
- [ ] Lazy expiration and eviction policies
- [ ] Connection pooling and multiplexing
- [ ] Pipelining optimization
- [ ] Horizontal scaling support
- [ ] Load balancing integration

### 🔄 Advanced Transactions

- [ ] **EXEC** - Execute transaction
- [ ] **DISCARD** - Cancel transaction
- [ ] **WATCH** / **UNWATCH** - Optimistic locking
- [ ] Conditional transactions
- [ ] Transaction rollback mechanisms

### 🌐 Protocol & Compatibility

- [ ] RESP3 protocol support
- [ ] Redis modules compatibility layer
- [ ] Backward compatibility modes
- [ ] Protocol versioning
- [ ] Custom protocol extensions

### 🛠️ Developer Experience

- [ ] Configuration file support
- [ ] Environment variable configuration
- [ ] Docker containerization
- [ ] Kubernetes deployment manifests
- [ ] CLI administration tools
- [ ] Web-based management interface
- [ ] IDE plugins and extensions

### 📈 Enterprise Features

- [ ] High availability clustering
- [ ] Automatic backup scheduling
- [ ] Disaster recovery procedures
- [ ] Multi-datacenter replication
- [ ] Enterprise monitoring integration
- [ ] Compliance and audit trails

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

### Contribution Guidelines

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Follow the existing code style and patterns
4. Add comprehensive tests for new functionality
5. Ensure all tests pass (`npm test`)
6. Update documentation as needed
7. Submit a pull request

## Performance

JS Redis Server is designed for performance with:

- In-memory data structures optimized for speed
- Efficient RESP protocol implementation
- Minimal object allocation in hot paths
- Comprehensive benchmarking and profiling

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with TypeScript for type safety and developer experience
- Uses the RESP protocol for Redis compatibility
- Inspired by the official Redis implementation
- Community-driven development and testing
