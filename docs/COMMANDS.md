# Redis Commands Implementation Status

This document provides a detailed overview of Redis commands and their implementation status in JS Redis Server.

`[x]` = implemented, `[ ]` = not implemented. Where the implementation accepts
only a subset of the real Redis syntax, the supported syntax is shown and a
**Note** lists the gaps.

## 1. Connection Commands

- [x] `PING [message]` - Return PONG, or echo `message`
- [x] `QUIT` - Close the connection
- [x] `SELECT index` - Change the selected database
- [x] `RESET` - Reset connection state (auth, MULTI/WATCH, RESP version, db, cluster read-only flag, client name) to defaults
- [x] `HELLO [protover [AUTH username password] [SETNAME clientname]]` - Switch protocol (RESP2/3), optionally authenticate and set the connection name in one round trip; reports the current cluster node role

#### AUTH

- [x] `AUTH password` - Authenticate to the server
- [x] `AUTH username password` - Authenticate to the server with username and password

> With no `requirepass` configured the default user is `nopass`: single-arg
> `AUTH` returns the "no password configured" error, while `AUTH default <pass>`
> is a no-op `OK` (any other username is `WRONGPASS`). When `requirepass` is set,
> connections start unauthenticated and every command except `AUTH`/`HELLO`/
> `RESET`/`QUIT` is rejected with `NOAUTH Authentication required.` until a
> correct `AUTH` (or `HELLO ... AUTH`) succeeds. `AUTH` and `HELLO` are flagged
> `noscript` (rejected from Lua), matching real Redis.

#### CLIENT

- [x] `CLIENT GETNAME` - Get the connection name
- [x] `CLIENT SETNAME connection-name` - Set the connection name
- [x] `CLIENT SETINFO LIB-NAME|LIB-VER value` - Record client library metadata
- [x] `CLIENT ID` - Return the connection's ID
- [x] `CLIENT INFO` / `CLIENT LIST` - Return a single `key=value` line for the current connection
- [x] `CLIENT HELP` - Return subcommand help
- [ ] `CLIENT KILL`, `CLIENT PAUSE`/`UNPAUSE`, `CLIENT NO-EVICT`, `CLIENT NO-TOUCH`, `CLIENT REPLY`, `CLIENT TRACKING` - not implemented

## 2. Server Commands

#### INFO

- [x] `INFO [section]` - Get information and statistics about the server
  - [x] `server`, `clients`, `memory`, `persistence`, `stats`, `replication`, `cpu`, `cluster`, `keyspace` - populated with static/zeroed placeholder values (sufficient for client-library handshakes, not real telemetry)
  - [x] `commandstats`, `latencystats`, `errorstats`, `modules`, `sentinel` - return empty sections
  - [x] `default` / `all` - returns the default section set
  - [ ] Real per-command/error/latency stats are not tracked

#### COMMAND

- [x] `COMMAND` - Return details for commands in the active registry
- [x] `COMMAND COUNT` - Return the command count for the active registry
- [x] `COMMAND LIST [FILTERBY PATTERN pattern|MODULE module]` - Return command names
- [x] `COMMAND INFO [command-name ...]` - Return command metadata
- [x] `COMMAND DOCS [command-name ...]` - Return command documentation (RESP3 maps / RESP2 flat arrays)
- [x] `COMMAND GETKEYS command [arg ...]` - Extract keys through the command definition
- [x] `COMMAND GETKEYSANDFLAGS command [arg ...]` - Extract keys with access flags
- [x] `COMMAND HELP` - Return command help

`COMMAND` is generated from registered command definitions and their
introspection metadata. It should not carry a separate hardcoded list of Redis
commands, so alternate Redis-version command registries can expose their own
surface.

#### CONFIG

- [x] `CONFIG GET parameter [parameter ...]` - Get configuration parameters (glob-matched against a fixed set of plausible defaults; RESP3 map / RESP2 flat array)
- [x] `CONFIG SET parameter value [parameter value ...]` - Set configuration parameters (rejects unknown parameter names with the real Redis error, matching CONFIG SET's "all-or-nothing" validation)
- [x] `CONFIG HELP`
- [ ] `CONFIG RESETSTAT` - Reset the stats returned by INFO
- [ ] `CONFIG REWRITE` - Rewrite the configuration file

> There is no real configuration subsystem behind this - values are an
> in-memory per-server store seeded with plausible defaults (`maxmemory`,
> `appendonly`, `save`, listpack thresholds, etc.), enough to satisfy client
> library initialization. `CONFIG SET` does not change server behavior.

#### DBSIZE

- [x] `DBSIZE` - Return the number of keys in the selected database

#### FLUSHDB / FLUSHALL

- [x] `FLUSHDB` - Remove all keys from the current database
- [x] `FLUSHALL` - Remove all keys from all databases
- [ ] `ASYNC`/`SYNC` modifiers - not accepted (any extra argument is a wrong-arity error)

> `FLUSHALL`/`FLUSHDB` clear keyspace data but **not** the Lua script cache -
> only `SCRIPT FLUSH` does.

## 3. Generic Key Commands

- [x] `DEL key [key ...]` - Delete one or more keys
- [x] `UNLINK key [key ...]` - Delete one or more keys without blocking (same as `DEL` in this mock)
- [x] `EXISTS key [key ...]` - Determine how many of the given keys exist
- [x] `TYPE key` - Return the type of the value stored at key
- [x] `RENAME key newkey` - Rename a key
- [x] `RENAMENX key newkey` - Rename a key only if the new key does not exist
- [x] `KEYS pattern` - Find all keys matching a glob pattern
- [x] `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` - Incrementally iterate the keyspace (see [Scan Family](#10-scan-family))

#### Expiration

- [x] `TTL key` - Get the time to live for a key in seconds
- [x] `PTTL key` - Get the time to live for a key in milliseconds
- [x] `PERSIST key` - Remove the existing timeout on a key
- [x] `EXPIRE key seconds` - Set a key's time to live in seconds
- [x] `PEXPIRE key milliseconds` - Set a key's time to live in milliseconds
- [x] `EXPIREAT key unix-time-seconds` - Set the expiration as a UNIX timestamp
- [x] `PEXPIREAT key unix-time-milliseconds` - Set the expiration as a UNIX timestamp in milliseconds
- [ ] `NX | XX | GT | LT` conditional flags on `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` - not implemented (any extra argument is a wrong-arity error)

#### Not implemented

- [ ] `OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ`
- [ ] `RANDOMKEY`
- [ ] `COPY` / `MOVE`
- [ ] `DUMP` / `RESTORE`
- [ ] `TOUCH`
- [ ] `SORT` / `SORT_RO`
- [ ] `WAIT`
- [ ] `MIGRATE`

## 4. String Commands

- [x] `GET key` - Get the value of a key
- [x] `SET key value [NX | XX] [GET] [KEEPTTL | EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds]` - Set the string value of a key
- [x] `SETNX key value` - Set the value of a key, only if it does not exist
- [x] `SETEX key seconds value` - Set the value and expiration in seconds
- [x] `PSETEX key milliseconds value` - Set the value and expiration in milliseconds
- [x] `MGET key [key ...]` - Get the values of multiple keys
- [x] `MSET key value [key value ...]` - Set multiple keys to multiple values
- [x] `MSETNX key value [key value ...]` - Set multiple keys, only if none of them exist
- [x] `GETSET key value` - Set a key's value and return its old value
- [x] `GETDEL key` - Get the value of a key and delete it
- [x] `GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]` - Get the value and optionally manage its TTL
- [x] `APPEND key value` - Append a value to a key
- [x] `STRLEN key` - Get the length of the value stored at key
- [x] `GETRANGE key start end` - Get a substring of the value
- [x] `SUBSTR key start end` - Alias for `GETRANGE` (deprecated)
- [x] `SETRANGE key offset value` - Overwrite part of a string at the given offset
- [x] `INCR key` / `DECR key` - Increment/decrement the integer value of a key by one
- [x] `INCRBY key increment` / `DECRBY key decrement` - Increment/decrement by the given integer
- [x] `INCRBYFLOAT key increment` - Increment the float value of a key

#### Not implemented

- [ ] `LCS` (longest common subsequence)

## 5. Hash Commands

- [x] `HSET key field value [field value ...]` - Set the value of one or more hash fields
- [x] `HSETNX key field value` - Set the value of a hash field, only if the field does not exist
- [x] `HGET key field` - Get the value of a hash field
- [x] `HMSET key field value [field value ...]` - Set multiple hash fields (deprecated, use HSET)
- [x] `HMGET key field [field ...]` - Get the values of multiple hash fields
- [x] `HGETALL key` - Get all fields and values in a hash (RESP3 map / RESP2 flat array)
- [x] `HDEL key field [field ...]` - Delete one or more hash fields
- [x] `HEXISTS key field` - Determine if a hash field exists
- [x] `HKEYS key` - Get all fields in a hash
- [x] `HVALS key` - Get all values in a hash
- [x] `HLEN key` - Get the number of fields in a hash
- [x] `HSTRLEN key field` - Get the length of the value of a hash field
- [x] `HINCRBY key field increment` - Increment the integer value of a hash field
- [x] `HINCRBYFLOAT key field increment` - Increment the float value of a hash field
- [x] `HSCAN key cursor [MATCH pattern] [COUNT count]` - Incrementally iterate hash fields and values (see [Scan Family](#10-scan-family))

#### Not implemented

- [ ] `HRANDFIELD key [count [WITHVALUES]]`
- [ ] `HEXPIRE` / `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT` / `HPERSIST` / `HTTL` / `HPTTL` / `HGETDEL` / `HGETEX` (Redis 7.4 hash-field-TTL family)

## 6. List Commands

- [x] `LPUSH key element [element ...]` - Prepend one or more elements
- [x] `RPUSH key element [element ...]` - Append one or more elements
- [x] `LPUSHX key element [element ...]` - Prepend, only if the key already exists
- [x] `RPUSHX key element [element ...]` - Append, only if the key already exists
- [x] `LPOP key [count]` - Remove and return the first element, or up to `count` elements
- [x] `RPOP key [count]` - Remove and return the last element, or up to `count` elements
- [x] `LLEN key` - Return the length of the list
- [x] `LRANGE key start stop` - Get a range of elements
- [x] `LINDEX key index` - Get an element by index
- [x] `LSET key index element` - Set the value of an element by index
- [x] `LREM key count element` - Remove elements matching a value
- [x] `LTRIM key start stop` - Trim a list to the specified range
- [x] `RPOPLPUSH source destination` - Pop from one list and push to another
- [x] `BLPOP key [key ...] timeout` - Blocking left pop
- [x] `BRPOP key [key ...] timeout` - Blocking right pop

#### Not implemented

- [ ] `LPOS`
- [ ] `LINSERT`
- [ ] `LMOVE` / `BLMOVE`
- [ ] `LMPOP` / `BLMPOP`

## 7. Set Commands

- [x] `SADD key member [member ...]` - Add one or more members to a set
- [x] `SREM key member [member ...]` - Remove one or more members from a set
- [x] `SCARD key` - Get the number of members in a set
- [x] `SMEMBERS key` - Get all members in a set
- [x] `SISMEMBER key member` - Determine if a value is a member of a set
- [x] `SPOP key [count]` - Remove and return one or more random members
- [x] `SRANDMEMBER key [count]` - Get one or more random members without removing them
- [x] `SMOVE source destination member` - Move a member between sets
- [x] `SDIFF key [key ...]` / `SDIFFSTORE destination key [key ...]` - Set difference
- [x] `SINTER key [key ...]` / `SINTERSTORE destination key [key ...]` - Set intersection
- [x] `SUNION key [key ...]` / `SUNIONSTORE destination key [key ...]` - Set union
- [x] `SSCAN key cursor [MATCH pattern] [COUNT count]` - Incrementally iterate set members (see [Scan Family](#10-scan-family))

#### Not implemented

- [ ] `SMISMEMBER key member [member ...]`
- [ ] `SINTERCARD`

## 8. Sorted Set Commands

- [x] `ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]` - Add or update members with scores
- [x] `ZREM key member [member ...]` - Remove one or more members
- [x] `ZCARD key` - Get the number of members
- [x] `ZSCORE key member` - Get the score of a member
- [x] `ZINCRBY key increment member` - Increment the score of a member
- [x] `ZRANK key member` - Get the index of a member, lowest score first
- [x] `ZREVRANK key member` - Get the index of a member, highest score first
- [x] `ZRANGE key start stop [WITHSCORES]` - Return a range of members by index
- [x] `ZREVRANGE key start stop [WITHSCORES]` - Return a range of members by index, high to low
- [x] `ZRANGEBYSCORE key min max` - Return members with scores between min and max (`-inf`, `+inf`, and exclusive `(score` bounds supported)
- [x] `ZREMRANGEBYSCORE key min max` - Remove members with scores between min and max (`-inf`, `+inf`, and exclusive `(score` bounds supported)
- [x] `ZCOUNT key min max` - Count members with scores between min and max (`-inf`, `+inf`, and exclusive `(score` bounds supported)
- [x] `ZPOPMIN key [count]` - Remove and return members with the lowest scores
- [x] `ZPOPMAX key [count]` - Remove and return members with the highest scores
- [x] `ZSCAN key cursor [MATCH pattern] [COUNT count]` - Incrementally iterate members and scores (see [Scan Family](#10-scan-family))

#### Notes / gaps vs. real Redis

- [ ] `ZRANGE`/`ZREVRANGE` do not support the Redis 6.2+ unified syntax (`BYSCORE`, `BYLEX`, `REV`, `LIMIT offset count`) - `start`/`stop` are always treated as indexes
- [ ] `ZRANGEBYSCORE` does not support `WITHSCORES` or `LIMIT offset count`
- [ ] `ZRANK`/`ZREVRANK` do not support the Redis 7.2 `WITHSCORE` option

#### Not implemented

- [ ] `ZRANGEBYLEX` / `ZREVRANGEBYSCORE` / `ZREVRANGEBYLEX` / `ZLEXCOUNT` / `ZREMRANGEBYRANK` / `ZREMRANGEBYLEX`
- [ ] `ZUNIONSTORE` / `ZINTERSTORE` / `ZDIFFSTORE` / `ZUNION` / `ZINTER` / `ZDIFF` / `ZINTERCARD`
- [ ] `ZMSCORE` / `ZRANDMEMBER` / `ZRANGESTORE` / `ZMPOP` / `BZPOPMIN` / `BZPOPMAX` / `BZMPOP`

## 9. Stream Commands

- [x] `XADD key [NOMKSTREAM] [MAXLEN|MINID [~] threshold] <ID|*> field value [field value ...]` - Append an entry to a stream
- [x] `XLEN key` - Return the number of entries
- [x] `XRANGE key start end [COUNT count]` - Return entries in ascending ID order
- [x] `XREVRANGE key end start [COUNT count]` - Return entries in descending ID order
- [x] `XDEL key ID [ID ...]` - Remove entries by ID
- [x] `XTRIM key MAXLEN|MINID [~] threshold` - Trim a stream to a size or minimum ID
- [x] `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]` - Read entries, optionally blocking for new ones (RESP3 map / RESP2 array of stream-entry pairs)

#### Notes / gaps vs. real Redis

- [ ] `XADD`/`XTRIM` trim specs do not support `LIMIT count`
- [ ] `XREAD` does not support the `GROUP` consumer-group form

#### Not implemented

- [ ] Consumer groups: `XGROUP`, `XREADGROUP`, `XACK`, `XCLAIM`, `XAUTOCLAIM`, `XPENDING`
- [ ] `XINFO STREAM|GROUPS|CONSUMERS`
- [ ] `XSETID`
- [ ] `XMSET` / `XCOPY`

> `RedisStreamData` is currently a minimal stub - entries are stored, but
> there is no consumer-group state.

## 10. Scan Family

- [x] `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` - Iterate the keyspace
- [x] `HSCAN key cursor [MATCH pattern] [COUNT count]` - Iterate hash fields/values
- [x] `SSCAN key cursor [MATCH pattern] [COUNT count]` - Iterate set members
- [x] `ZSCAN key cursor [MATCH pattern] [COUNT count]` - Iterate sorted set members/scores
- [x] `KEYS pattern` - Return all keys matching a glob pattern

`TYPE` is only accepted by `SCAN` (matching real Redis); `HSCAN`/`SSCAN`/`ZSCAN`
reject it.

- [ ] `HSCAN ... NOVALUES` (Redis 7.4)

## 11. Transaction Commands

- [x] `MULTI` - Mark the start of a transaction block
- [x] `EXEC` - Execute all commands issued after MULTI
- [x] `DISCARD` - Discard all commands issued after MULTI
- [x] `WATCH key [key ...]` - Watch keys for conditional execution of a MULTI/EXEC block
- [x] `UNWATCH` - Forget all watched keys

Parsing and key extraction (and therefore early `CROSSSLOT`/`MOVED` errors in
cluster mode) happen at queue time, not at `EXEC` time. `EXECABORT` is
returned if the queue is dirty (e.g. an unknown command was queued).

## 12. Scripting Commands

- [x] `EVAL script numkeys [key ...] [arg ...]` - Execute a Lua script
- [x] `EVALSHA sha1 numkeys [key ...] [arg ...]` - Execute a cached Lua script by its SHA1
- [x] `SCRIPT LOAD script` - Load a script into the script cache
- [x] `SCRIPT EXISTS sha1 [sha1 ...]` - Check existence of scripts in the cache
- [x] `SCRIPT FLUSH [ASYNC|SYNC]` - Remove all scripts from the cache
- [x] `SCRIPT KILL` - Report no script running (no script can run long enough to need killing)
- [x] `SCRIPT DEBUG YES|SYNC|NO` - Validates the mode and returns `OK` (debug mode itself is a no-op)
- [x] `SCRIPT HELP` - Return subcommand help

`EVAL`/`EVALSHA` run via `executePlanSync` against the same command registry
and policies as normal commands, so every command's `noscript`/`readonly`
flags are enforced inside `redis.call`/`redis.pcall`.

## 13. Cluster Commands

- [x] `CLUSTER INFO` - Provides info about Redis Cluster node state
- [x] `CLUSTER NODES` - Get the cluster config for the node
- [x] `CLUSTER SLOTS` - Get array of slot ranges with assigned nodes
- [x] `CLUSTER SHARDS` - Get array of shards with their slot ranges and nodes (RESP3 maps / RESP2 flat arrays)
- [x] `CLUSTER MYID` - Return the node's own ID
- [x] `READONLY` - Allow read-only commands against a direct replica connection for slots served by its master
- [x] `READWRITE` - Disable replica read mode on the current connection

> Cluster topology is fixed at startup (config-driven), so dynamic
> reconfiguration commands are out of scope by design:

- [ ] `CLUSTER MEET` / `FORGET` / `REPLICATE` / `SAVECONFIG`
- [ ] `CLUSTER ADDSLOTS` / `DELSLOTS` / `FLUSHSLOTS` / `SETSLOT`
- [ ] `CLUSTER KEYSLOT` / `COUNTKEYSINSLOT` / `GETKEYSINSLOT`
- [ ] `CLUSTER BUMPEPOCH` / `RESET` / `FAILOVER`

## 14. Pub/Sub Commands

- [ ] `PUBLISH channel message`
- [ ] `SUBSCRIBE` / `UNSUBSCRIBE` / `PSUBSCRIBE` / `PUNSUBSCRIBE` / `SSUBSCRIBE` / `SUNSUBSCRIBE` / `SPUBLISH`
- [ ] `PUBSUB CHANNELS|NUMSUB|NUMPAT`

> The mutation-event bus used internally for `WATCH` is unrelated to client
> pub/sub; no pub/sub command module exists yet.

## 15. Persistence Commands

- [ ] `SAVE` / `BGSAVE` / `BGREWRITEAOF`
- [ ] `LASTSAVE`
- [ ] `SHUTDOWN [NOSAVE|SAVE]`

## Implementation Notes

- `[x]` indicates implemented commands; `[ ]` indicates planned or
  unimplemented commands (or unimplemented options of an otherwise-implemented
  command)
- This document reflects the current state of `src/commands/`; when adding or
  changing a command, update the relevant section here
