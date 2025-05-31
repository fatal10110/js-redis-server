# JS Redis Server

A lightweight, in-memory Redis-compatible server implementation in TypeScript, designed for testing and development purposes. This server provides a mock Redis environment that can be used with popular Redis clients like ioredis and node-redis.

## Features

- Redis-compatible RESP (REdis Serialization Protocol) implementation
- In-memory data storage
- Cluster support with master-slave replication
- Pub/Sub functionality
- Scripting support
- Multi/Transaction support
- Data persistence capabilities

## Installation

```bash
npm install js-redis-server
```

## Usage

### Basic Server Setup

```typescript
import { ClusterNetwork } from 'js-redis-server'

async function run() {
  const cluster = new ClusterNetwork(console)

  // Initialize cluster with 3 masters and 2 slaves
  await cluster.init({ masters: 3, slaves: 2 })

  // Get cluster information
  console.log(
    Array.from(cluster.getAll()).map(n => ({
      port: n.getAddress().port,
      slots: n.slotRange,
    })),
  )
}

run().catch(console.error)
```

### Using with Redis Clients

```typescript
import Redis from 'ioredis'

// Connect to the server
const redis = new Redis({
  port: 6379, // Default port
  host: 'localhost',
})

// Use Redis commands
await redis.set('key', 'value')
const value = await redis.get('key')
```

## Development

### Prerequisites

- Node.js (v22 or higher)
- npm

### Setup

1. Clone the repository:

```bash
git clone https://github.com/fatal10110/js-redis-server.git
cd js-redis-server
```

2. Install dependencies:

```bash
npm install
```

### Available Scripts

- `npm test` - Run tests
- `npm run build` - Build the project
- `npm start` - Start the server
- `npm run lint` - Run ESLint
- `npm run format` - Format code with Prettier

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Redis Commands and Functionalities

### Connection Commands

- [x] `PING` - Check server connection
- [x] `QUIT` - Close the connection
- [ ] `AUTH` - Authenticate to the server
- [ ] `SELECT` - Change the selected database

### Server Commands

- [x] `INFO` - Get server information
- [ ] `CONFIG` - Configure server parameters
- [ ] `DBSIZE` - Return the number of keys in the selected database
- [ ] `FLUSHDB` - Remove all keys from the current database
- [ ] `FLUSHALL` - Remove all keys from all databases
- [ ] `TIME` - Return the current server time

### Scripting Commands

- [x] `EVAL` - Execute a Lua script
- [x] `EVALSHA` - Execute a Lua script by its SHA1 digest
- [ ] `SCRIPT LOAD` - Load a script into the script cache
- [ ] `SCRIPT EXISTS` - Check existence of scripts in the script cache
- [ ] `SCRIPT FLUSH` - Remove all scripts from the script cache
- [ ] `SCRIPT KILL` - Kill the script currently in execution

### Transaction Commands

- [x] `MULTI` - Mark the start of a transaction block
- [ ] `EXEC` - Execute all commands issued after MULTI
- [ ] `DISCARD` - Discard all commands issued after MULTI
- [ ] `WATCH` - Watch the given keys to determine execution of the MULTI/EXEC block
- [ ] `UNWATCH` - Forget about all watched keys

### Cluster Commands

- [ ] `CLUSTER INFO` - Provides info about Redis Cluster node state
- [ ] `CLUSTER NODES` - Get Cluster config for the node
- [ ] `CLUSTER MEET` - Force a node cluster to handshake with another node
- [ ] `CLUSTER FORGET` - Remove a node from the nodes table
- [ ] `CLUSTER REPLICATE` - Reconfigure a node as a replica of the specified master node
- [ ] `CLUSTER SAVECONFIG` - Force the node to save cluster state on disk
- [ ] `CLUSTER ADDSLOTS` - Assign new hash slots to receiving node
- [ ] `CLUSTER DELSLOTS` - Set hash slots as unbound in receiving node
- [ ] `CLUSTER FLUSHSLOTS` - Delete own slots information
- [ ] `CLUSTER SETSLOT` - Bind a hash slot to a specific node
- [ ] `CLUSTER KEYSLOT` - Returns the hash slot of the specified key
- [ ] `CLUSTER COUNTKEYSINSLOT` - Return the number of local keys in the specified hash slot
- [ ] `CLUSTER GETKEYSINSLOT` - Return local key names in the specified hash slot

### Data Types and Commands

#### Strings

- [ ] `SET` - Set the string value of a key
- [ ] `GET` - Get the value of a key
- [ ] `DEL` - Delete a key
- [ ] `EXISTS` - Determine if a key exists
- [ ] `EXPIRE` - Set a key's time to live in seconds
- [ ] `TTL` - Get the time to live for a key
- [ ] `INCR` - Increment the integer value of a key by one
- [ ] `DECR` - Decrement the integer value of a key by one
- [ ] `APPEND` - Append a value to a key
- [ ] `STRLEN` - Get the length of the value stored in a key

#### Lists

- [ ] `LPUSH` - Prepend one or multiple values to a list
- [ ] `RPUSH` - Append one or multiple values to a list
- [ ] `LPOP` - Remove and get the first element in a list
- [ ] `RPOP` - Remove and get the last element in a list
- [ ] `LLEN` - Get the length of a list
- [ ] `LRANGE` - Get a range of elements from a list
- [ ] `LINDEX` - Get an element from a list by its index

#### Sets

- [ ] `SADD` - Add one or more members to a set
- [ ] `SREM` - Remove one or more members from a set
- [ ] `SMEMBERS` - Get all the members in a set
- [ ] `SISMEMBER` - Determine if a given value is a member of a set
- [ ] `SCARD` - Get the number of members in a set
- [ ] `SINTER` - Intersect multiple sets
- [ ] `SUNION` - Add multiple sets
- [ ] `SDIFF` - Subtract multiple sets

#### Hashes

- [ ] `HSET` - Set the string value of a hash field
- [ ] `HGET` - Get the value of a hash field
- [ ] `HDEL` - Delete one or more hash fields
- [ ] `HEXISTS` - Determine if a hash field exists
- [ ] `HGETALL` - Get all the fields and values in a hash
- [ ] `HKEYS` - Get all the fields in a hash
- [ ] `HLEN` - Get the number of fields in a hash

#### Sorted Sets

- [ ] `ZADD` - Add one or more members to a sorted set
- [ ] `ZREM` - Remove one or more members from a sorted set
- [ ] `ZRANGE` - Return a range of members in a sorted set
- [ ] `ZRANK` - Determine the index of a member in a sorted set
- [ ] `ZSCORE` - Get the score associated with the given member in a sorted set
- [ ] `ZCARD` - Get the number of members in a sorted set

### Pub/Sub Commands

- [ ] `PUBLISH` - Post a message to a channel
- [ ] `SUBSCRIBE` - Listen for messages published to the given channels
- [ ] `UNSUBSCRIBE` - Stop listening for messages posted to the given channels
- [ ] `PSUBSCRIBE` - Listen for messages published to channels matching the given patterns
- [ ] `PUNSUBSCRIBE` - Stop listening for messages posted to channels matching the given patterns

### Persistence Commands

- [ ] `SAVE` - Synchronously save the dataset to disk
- [ ] `BGSAVE` - Asynchronously save the dataset to disk
- [ ] `LASTSAVE` - Get the UNIX timestamp of the last successful save to disk
- [ ] `SHUTDOWN` - Synchronously save the dataset to disk and then shut down the server

Note: [x] indicates implemented commands, [ ] indicates planned or in-progress commands.
