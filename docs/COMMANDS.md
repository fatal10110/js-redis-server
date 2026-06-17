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
- [x] `CLIENT INFO` - Return a single `key=value` line for the current connection
- [x] `CLIENT LIST` - Return one `key=value` line per active client connected to the current server node
- [x] `CLIENT HELP` - Return subcommand help
- [ ] `CLIENT KILL`, `CLIENT PAUSE`/`UNPAUSE`, `CLIENT NO-EVICT`, `CLIENT NO-TOUCH`, `CLIENT REPLY`, `CLIENT TRACKING` - not implemented

## 2. Server Commands

#### INFO

- [x] `INFO [section]` - Get information and statistics about the server
  - [x] `server`, `clients`, `memory`, `persistence`, `stats`, `replication`, `cpu`, `cluster`, `keyspace` - populated with static/zeroed placeholder values (sufficient for client-library handshakes, not real telemetry)
  - [x] `commandstats`, `latencystats`, `errorstats`, `modules`, `sentinel` - return empty sections
  - [x] `default` / `all` - returns the default section set
  - [ ] Real per-command/error/latency stats are not tracked

#### MONITOR

- [x] `MONITOR` - Return `OK` and stream Redis-style command event lines as simple string replies for commands from other connections

`MONITOR` is implemented as a long-lived `ResponseStream` backed by a
server-level command event feed. Monitor lines include an epoch timestamp, the
selected DB, the client address/identity when available, and quoted command
arguments. Unknown commands and arity/syntax failures are not emitted; commands
that parse successfully but return execution errors are emitted, matching Redis.
Cluster redirects and pre-execution cluster errors are not emitted because the
command is not executed on that node. Commands with monitor skip metadata are
skipped, authentication credentials are redacted, commands replayed by `EXEC` are
emitted once when the transaction runs, and Lua `redis.call` / `redis.pcall`
commands are emitted with the `lua` source. `MONITOR` is flagged `noscript` and
is rejected from Lua.

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
> library initialization. Most parameters are inert: `CONFIG SET` stores the
> value but does not change server behavior. The exception is
> `notify-keyspace-events`, which is a real, behavior-driving setting — see
> [Keyspace notifications](#14-pubsub-commands). Its value is validated and
> normalized exactly like Redis (e.g. `CONFIG SET ... KEA` reads back as `AKE`;
> an unknown class character is rejected).

#### DBSIZE

- [x] `DBSIZE` - Return the number of keys in the selected database

#### FLUSHDB / FLUSHALL

- [x] `FLUSHDB [ASYNC|SYNC]` - Remove all keys from the current database
- [x] `FLUSHALL [ASYNC|SYNC]` - Remove all keys from all databases
- The optional `ASYNC` / `SYNC` modifier is accepted (case-insensitive) and ignored — the in-memory keyspace is always flushed synchronously. Any other token, or more than one modifier, returns `ERR syntax error`.

> `FLUSHALL`/`FLUSHDB` clear keyspace data but **not** the Lua script cache -
> only `SCRIPT FLUSH` does.

#### TIME

- [x] `TIME` - Return the current server time as a two-element array `[unix-time-seconds, microseconds]`

#### MEMORY

- [ ] `MEMORY USAGE key [SAMPLES count]` - Estimate memory consumption of a key in bytes
- [ ] `MEMORY DOCTOR` - Return a memory health report
- [ ] `MEMORY MALLOC-STATS` - Return allocator internal stats
- [ ] `MEMORY PURGE` - Attempt to free unused memory
- [ ] `MEMORY STATS` - Return a breakdown of memory allocation
- [ ] `MEMORY HELP`

#### SLOWLOG

- [ ] `SLOWLOG GET [count]` - Return the slow-log entries
- [ ] `SLOWLOG LEN` - Return the number of entries in the slow log
- [ ] `SLOWLOG RESET` - Clear the slow log
- [ ] `SLOWLOG HELP`

#### LATENCY

- [ ] `LATENCY LATEST` - Return the latest samples for all event types
- [ ] `LATENCY HISTORY event` - Return latency history for a given event
- [ ] `LATENCY RESET [event ...]` - Reset latency data

#### DEBUG

- [ ] `DEBUG SLEEP seconds` - Block the server for the given number of seconds (useful for timeout testing)
- [ ] `DEBUG RELOAD` - Save and reload the RDB in-process
- [ ] `DEBUG OBJECT key` - Return internal encoding and refcount metadata for a key
- [ ] `DEBUG JMAP` - Force a GC cycle

#### LOLWUT

- [ ] `LOLWUT [VERSION version]` - Return a version-specific ASCII art rendering

## 3. Generic Key Commands

- [x] `DEL key [key ...]` - Delete one or more keys
- [x] `UNLINK key [key ...]` - Delete one or more keys without blocking (same as `DEL` in this mock)
- [x] `EXISTS key [key ...]` - Determine how many of the given keys exist
- [x] `TOUCH key [key ...]` - Count the given keys that exist without changing key values
- [x] `TYPE key` - Return the type of the value stored at key
- [x] `RANDOMKEY` - Return a random key from the selected database, or nil when empty
- [x] `RENAME key newkey` - Rename a key
- [x] `RENAMENX key newkey` - Rename a key only if the new key does not exist
- [x] `COPY source destination [DB destination-db] [REPLACE]` - Copy a key's value (and TTL) to a destination, optionally into another database; returns `0` if the destination exists without `REPLACE`
- [x] `KEYS pattern` - Find all keys matching a glob pattern
- [x] `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` - Incrementally iterate the keyspace (see [Scan Family](#10-scan-family))

#### Expiration

- [x] `TTL key` - Get the time to live for a key in seconds
- [x] `PTTL key` - Get the time to live for a key in milliseconds
- [x] `PERSIST key` - Remove the existing timeout on a key
- [x] `EXPIRE key seconds [NX | XX | GT | LT]` - Set a key's time to live in seconds, optionally only when the current expiry state matches the condition
- [x] `PEXPIRE key milliseconds [NX | XX | GT | LT]` - Set a key's time to live in milliseconds, optionally only when the current expiry state matches the condition
- [x] `EXPIREAT key unix-time-seconds [NX | XX | GT | LT]` - Set the expiration as a UNIX timestamp, optionally only when the current expiry state matches the condition
- [x] `PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]` - Set the expiration as a UNIX timestamp in milliseconds, optionally only when the current expiry state matches the condition
- [x] `EXPIRETIME key` - Get the absolute Unix expiration time in seconds
- [x] `PEXPIRETIME key` - Get the absolute Unix expiration time in milliseconds

`NX`, `GT`, and `LT` follow Redis' mutual-exclusion rules; `XX` can be combined
with `GT` or `LT`.

#### Not implemented

- [ ] `OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ|HELP`
- [ ] `MOVE`
- [ ] `DUMP` / `RESTORE`
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
- [x] `HRANDFIELD key [count [WITHVALUES]]` - Return one or more random hash fields, optionally with values
- [x] `HSCAN key cursor [MATCH pattern] [COUNT count]` - Incrementally iterate hash fields and values (see [Scan Family](#10-scan-family))

#### Not implemented

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
- [x] `LINSERT key BEFORE | AFTER pivot element` - Insert an element before or after the first matching pivot
- [x] `LSET key index element` - Set the value of an element by index
- [x] `LREM key count element` - Remove elements matching a value
- [x] `LTRIM key start stop` - Trim a list to the specified range
- [x] `RPOPLPUSH source destination` - Pop from one list and push to another
- [x] `LMOVE source destination LEFT | RIGHT LEFT | RIGHT` - Atomically pop from one end of a list and push to either end of another
- [x] `LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]` - Return the index(es) of matching elements
- [x] `BLPOP key [key ...] timeout` - Blocking left pop
- [x] `BRPOP key [key ...] timeout` - Blocking right pop
- [x] `BLMOVE source destination LEFT | RIGHT LEFT | RIGHT timeout` - Blocking variant of `LMOVE`
- [x] `LMPOP numkeys key [key ...] LEFT | RIGHT [COUNT count]` - Pop from the first non-empty list
- [x] `BLMPOP timeout numkeys key [key ...] LEFT | RIGHT [COUNT count]` - Blocking variant of `LMPOP`

## 7. Set Commands

- [x] `SADD key member [member ...]` - Add one or more members to a set
- [x] `SREM key member [member ...]` - Remove one or more members from a set
- [x] `SCARD key` - Get the number of members in a set
- [x] `SMEMBERS key` - Get all members in a set
- [x] `SISMEMBER key member` - Determine if a value is a member of a set
- [x] `SMISMEMBER key member [member ...]` - Determine membership for multiple values
- [x] `SPOP key [count]` - Remove and return one or more random members
- [x] `SRANDMEMBER key [count]` - Get one or more random members without removing them
- [x] `SMOVE source destination member` - Move a member between sets
- [x] `SDIFF key [key ...]` / `SDIFFSTORE destination key [key ...]` - Set difference
- [x] `SINTER key [key ...]` / `SINTERSTORE destination key [key ...]` - Set intersection
- [x] `SINTERCARD numkeys key [key ...] [LIMIT limit]` - Count set intersection members without materializing them
- [x] `SUNION key [key ...]` / `SUNIONSTORE destination key [key ...]` - Set union
- [x] `SSCAN key cursor [MATCH pattern] [COUNT count]` - Incrementally iterate set members (see [Scan Family](#10-scan-family))

## 8. Sorted Set Commands

- [x] `ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]` - Add or update members with scores (`inf`, `+inf`, and `-inf` score tokens supported)
- [x] `ZREM key member [member ...]` - Remove one or more members
- [x] `ZCARD key` - Get the number of members
- [x] `ZSCORE key member` - Get the score of a member
- [x] `ZINCRBY key increment member` - Increment the score of a member (`inf`, `+inf`, and `-inf` increments supported; NaN results rejected)
- [x] `ZRANK key member` - Get the index of a member, lowest score first
- [x] `ZREVRANK key member` - Get the index of a member, highest score first
- [x] `ZRANGE key min max [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]` - Return a range of members by index, score, or lexicographic range, optionally reversed
- [x] `ZREVRANGE key start stop [WITHSCORES]` - Return a range of members by index, high to low
- [x] `ZMSCORE key member [member ...]` - Get the scores of multiple members (nil per missing member)
- [x] `ZRANDMEMBER key [count [WITHSCORES]]` - Get one or more random members, optionally with their scores
- [x] `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]` - Return members with scores between min and max (`-inf`, `+inf`, and exclusive `(score` bounds supported)
- [x] `ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]` - Return members with scores between max and min, high to low (`-inf`, `+inf`, and exclusive `(score` bounds supported)
- [x] `ZREMRANGEBYRANK key start stop` - Remove members by rank range (negative ranks count from highest)
- [x] `ZREMRANGEBYSCORE key min max` - Remove members with scores between min and max (`-inf`, `+inf`, and exclusive `(score` bounds supported)
- [x] `ZCOUNT key min max` - Count members with scores between min and max (`-inf`, `+inf`, and exclusive `(score` bounds supported)
- [x] `ZRANGEBYLEX key min max [LIMIT offset count]` - Return members within a lexicographic range (`-`, `+`, and inclusive `[member`/exclusive `(member` bounds supported)
- [x] `ZREVRANGEBYLEX key max min [LIMIT offset count]` - Return members within a lexicographic range, high to low
- [x] `ZLEXCOUNT key min max` - Count members within a lexicographic range
- [x] `ZREMRANGEBYLEX key min max` - Remove members within a lexicographic range
- [x] `ZPOPMIN key [count]` - Remove and return members with the lowest scores
- [x] `ZPOPMAX key [count]` - Remove and return members with the highest scores
- [x] `ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]` - Pop one or more members from the first non-empty sorted set
- [x] `BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]` - Blocking multi-key sorted-set pop; returns null on timeout
- [x] `BZPOPMIN key [key ...] timeout` - Blocking pop of the lowest-score member from the first non-empty sorted set; returns `[key, member, score]` or null on timeout
- [x] `BZPOPMAX key [key ...] timeout` - Blocking pop of the highest-score member from the first non-empty sorted set; returns `[key, member, score]` or null on timeout
- [x] `ZSCAN key cursor [MATCH pattern] [COUNT count]` - Incrementally iterate members and scores (see [Scan Family](#10-scan-family))
- [x] `ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]` - Union of sorted sets (or plain sets, scored 1) into destination; empty result deletes destination
- [x] `ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]` - Intersection of sorted sets into destination; empty result deletes destination
- [x] `ZDIFFSTORE destination numkeys key [key ...]` - Difference of sorted sets into destination; empty result deletes destination
- [x] `ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]` - Union of sorted sets without storing the result
- [x] `ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]` - Intersection of sorted sets without storing the result
- [x] `ZDIFF numkeys key [key ...] [WITHSCORES]` - Difference of sorted sets without storing the result
- [x] `ZINTERCARD numkeys key [key ...] [LIMIT limit]` - Count members in the intersection of sorted sets (LIMIT 0 means no cap)

#### Notes / gaps vs. real Redis

- Score replies from `ZSCORE`, `ZINCRBY`, `ZRANGE WITHSCORES`, `ZREVRANGE WITHSCORES`, `ZRANGEBYSCORE WITHSCORES`, `ZREVRANGEBYSCORE WITHSCORES`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`, `BZMPOP`, `BZPOPMIN`, `BZPOPMAX`, and `ZSCAN` serialize infinite scores as `inf` / `-inf`.
- [ ] `ZREVRANGE` does not support the Redis 6.2+ unified syntax (`BYSCORE`, `BYLEX`, `REV`, `LIMIT offset count`) - `start`/`stop` are always treated as indexes; use `ZRANGE ... REV` instead
- [ ] `ZRANK`/`ZREVRANK` do not support the Redis 7.2 `WITHSCORE` option

#### Not implemented

- [ ] `ZRANGESTORE`

## 9. Stream Commands

- [x] `XADD key [NOMKSTREAM] [MAXLEN|MINID [~] threshold [LIMIT count]] <ID|*> field value [field value ...]` - Append an entry to a stream
- [x] `XLEN key` - Return the number of entries
- [x] `XRANGE key start end [COUNT count]` - Return entries in ascending ID order
- [x] `XREVRANGE key end start [COUNT count]` - Return entries in descending ID order
- [x] `XDEL key ID [ID ...]` - Remove entries by ID
- [x] `XTRIM key MAXLEN|MINID [~] threshold [LIMIT count]` - Trim a stream to a size or minimum ID
- [x] `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]` - Read entries, optionally blocking for new ones (RESP3 map / RESP2 array of stream-entry pairs)
- [x] `XGROUP CREATE|SETID|DESTROY|CREATECONSUMER|DELCONSUMER ...` - Manage stream consumer groups and consumers
- [x] `XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]` - Read entries through a consumer group and track pending delivery
- [x] `XACK key group ID [ID ...]` - Acknowledge pending stream entries
- [x] `XPENDING key group [[IDLE min-idle-time] start end count [consumer]]` - Inspect pending stream entries
- [x] `XCLAIM key group consumer min-idle-time ID [ID ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID id]` - Claim pending entries for another consumer
- [x] `XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]` - Claim idle pending entries automatically
- [x] `XINFO STREAM key [FULL [COUNT count]]` - Inspect stream metadata, entries, groups, and PEL details
- [x] `XINFO GROUPS key` - List stream consumer groups
- [x] `XINFO CONSUMERS key group` - List consumers in a group

#### Notes / gaps vs. real Redis

- Approximate (`~`) `XADD`/`XTRIM` trim specs use a simplified mock heuristic that may leave one extra eligible entry; `LIMIT count` is accepted for Redis-compatible parsing and treated as a trimming hint.
- [ ] Stream radix-tree statistics in `XINFO STREAM` are approximated for mock compatibility rather than mirroring Redis internals

#### Not implemented

- [ ] `XSETID key last-id [ENTRIESADDED entries-added]` - Set the last-delivered ID and optionally the entries-added counter for a stream

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

All five transaction-control commands are flagged `noscript` (rejected from
Lua with the standard script error), matching real Redis — a script cannot
flip its session into transaction mode or register a `WATCH`.

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

#### Not implemented

- [ ] `FCALL function numkeys [key ...] [arg ...]` - Call a Redis Function (Redis 7.0+)
- [ ] `FCALL_RO function numkeys [key ...] [arg ...]` - Read-only variant of `FCALL`
- [ ] `FUNCTION LOAD [REPLACE] function-code` - Load a library of functions
- [ ] `FUNCTION DELETE library-name` - Delete a function library
- [ ] `FUNCTION LIST [LIBRARYNAME name] [WITHCODE]` - List function libraries
- [ ] `FUNCTION STATS` - Return runtime stats for the running script and loaded libraries
- [ ] `FUNCTION DUMP` - Dump all function libraries to a binary payload
- [ ] `FUNCTION RESTORE serialized-value [FLUSH|APPEND|REPLACE]` - Restore libraries from a `FUNCTION DUMP` payload
- [ ] `FUNCTION FLUSH [ASYNC|SYNC]` - Delete all function libraries
- [ ] `FUNCTION HELP`

> Redis Functions (Redis 7.0) are a named, persistent alternative to ad-hoc `EVAL` Lua scripts.
> They are stored in named libraries and survive flushes/restarts, unlike `EVAL` scripts.
> `FCALL` / `FCALL_RO` are the call sites; the `FUNCTION` family manages the library registry.

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

- [x] `PUBLISH channel message`
- [x] `SUBSCRIBE` / `UNSUBSCRIBE`
- [x] `PSUBSCRIBE` / `PUNSUBSCRIBE`
- [x] `PUBSUB CHANNELS|NUMSUB|NUMPAT`
- [x] `PUBSUB SHARDCHANNELS|SHARDNUMSUB` - Present for Redis 7 command compatibility; returns empty shard pub/sub state because sharded Pub/Sub subscriptions are not implemented yet.

- [ ] `SSUBSCRIBE` / `SUNSUBSCRIBE` / `SPUBLISH`

Subscribed connections enforce Redis' restricted command mode: only subscribe,
unsubscribe, `PING`, `RESET`, and `QUIT` are accepted until all subscriptions
are removed. Pub/Sub state is process-local to the `RedisServerState` instance;
cluster-wide fan-out between separate mock cluster nodes is not implemented.

#### Keyspace notifications

Key mutations are published to the standard `__keyspace@<db>__:<key>` (event in
the message) and `__keyevent@<db>__:<event>` (key in the message) channels when
enabled via `CONFIG SET notify-keyspace-events <flags>`. The flag string uses
Redis' class characters (`K`, `E`, `A`, `g`, `$`, `l`, `s`, `h`, `z`, `x`, `e`,
`t`, `m`, `n`, `d`); it is validated and normalized like real Redis.

- [x] Lifecycle events derived from the keyspace itself: `del`, `expire`,
      `persist`, and `expired` (fired when a key is lazily evicted on access).
- [x] Write events named after the originating command, matching real Redis:
      `set` (and `setnx`/`setex`/`getset`/`mset` → `set`), `incrby`
      (`incr`/`decr`/`decrby` → `incrby`), `append`, `setrange`, `lpush`/`rpush`
      (`lpushx`/`rpushx` too), `lpop`, `lset`, `linsert`, `hset`
      (`hmset`/`hsetnx` → `hset`), `hdel`, `sadd`, `srem`, `spop`, `zadd`,
      `zincr` (from `ZINCRBY`), `zrem`, `xadd`, etc.
- [x] `RENAME`/`RENAMENX` emit `rename_from` + `rename_to`; `COPY` emits
      `copy_to`.

> Known gaps: `SET ... EX`/`SETEX` emit only `set` (real Redis also emits a
> secondary `expire`); `FLUSHDB`/`FLUSHALL` emit no per-key events; cross-DB
> `COPY` does not name the destination event. Notifications are process-local to
> the `RedisServerState`, so they are not delivered across mock cluster nodes.

## 15. Persistence Commands

- [ ] `SAVE` - Synchronously save the dataset to disk
- [ ] `BGSAVE [SCHEDULE]` - Asynchronously save the dataset to disk in the background
- [ ] `BGREWRITEAOF` - Asynchronously rewrite the append-only file
- [x] `LASTSAVE` - Return the Unix timestamp of the last successful save (returns process start time, since there is no persistence)
- [ ] `SHUTDOWN [NOSAVE|SAVE|ABORT]` - Synchronously save and shut down the server

## 16. ACL Commands

All `ACL` subcommands are unimplemented. The auth system (`requirepass` / `AUTH`) is functional;
the full multi-user ACL layer (Redis 6.0+) is not.

- [ ] `ACL WHOAMI` - Return the username of the current connection
- [ ] `ACL LIST` - List all user accounts in the ACL config format
- [ ] `ACL USERS` - Return a list of all usernames
- [ ] `ACL GETUSER username` - Return the full ACL definition for a user
- [ ] `ACL SETUSER username [rule ...]` - Create or modify a user and set rules
- [ ] `ACL DELUSER username [username ...]` - Delete one or more users
- [ ] `ACL CAT [category]` - List ACL categories or the commands in a given category
- [ ] `ACL LOG [count | RESET]` - Return or clear the ACL security-event log
- [ ] `ACL SAVE` - Save the current ACL rules to the `aclfile`
- [ ] `ACL LOAD` - Reload ACL rules from the `aclfile`
- [ ] `ACL HELP`

## Implementation Notes

- `[x]` indicates implemented commands; `[ ]` indicates planned or
  unimplemented commands (or unimplemented options of an otherwise-implemented
  command)
- This document reflects the current state of `src/commands/`; when adding or
  changing a command, update the relevant section here
