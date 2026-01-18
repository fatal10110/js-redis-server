# Redis Commands Implementation Status

This document provides a detailed overview of Redis commands and their implementation status in JS Redis Server.

## Command Categories

### 1. Connection Commands

#### PING

- [x] `PING` - Return PONG
- [ ] `PING [message]` - Return a copy of the argument

#### QUIT

- [x] QUIT - Closes the connection

#### AUTH

- [ ] `AUTH password` - Authenticate to the server
- [ ] `AUTH username password` - Authenticate to the server with username and password

#### SELECT

- [ ] `SELECT index` - Change the selected database

### 2. Server Commands

#### INFO

- [x] Basic implementation
- `INFO [section]` - Get information and statistics about the server
  - [ ] `server` - General information about the Redis server
  - [ ] `clients` - Client connections section
  - [ ] `memory` - Memory consumption related information
  - [ ] `persistence` - RDB and AOF related information
  - [ ] `stats` - General statistics
  - [ ] `replication` - Master/replica replication information
  - [ ] `cpu` - CPU consumption statistics
  - [ ] `commandstats` - Redis command statistics
  - [ ] `cluster` - Redis Cluster section
  - [ ] `keyspace` - Database related statistics

#### CONFIG

- [ ] `CONFIG GET parameter` - Get the value of a configuration parameter
- [ ] `CONFIG SET parameter value` - Set a configuration parameter
- [ ] `CONFIG RESETSTAT` - Reset the stats returned by INFO
- [ ] `CONFIG REWRITE` - Rewrite the configuration file

#### DBSIZE

- [x] `DBSIZE` - Return the number of keys in the selected database

#### FLUSHDB

- [ ] `FLUSHDB [ASYNC]` - Remove all keys from the current database
- [ ] `FLUSHDB ASYNC` - Remove all keys asynchronously

#### FLUSHALL

- [ ] `FLUSHALL [ASYNC]` - Remove all keys from all databases
- [ ] `FLUSHALL ASYNC` - Remove all keys asynchronously

### 3. Scripting Commands

#### EVAL

- [ ] `EVAL script numkeys [key [key ...]] [arg [arg ...]]` - Execute a Lua script
- [ ] `EVAL script 0` - Execute a Lua script with no keys
- [ ] `EVAL script 1 key1` - Execute a Lua script with one key
- [ ] `EVAL script 2 key1 key2` - Execute a Lua script with two keys

#### EVALSHA

- [ ] `EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]` - Execute a Lua script by its SHA1 digest

#### SCRIPT

- [ ] `SCRIPT LOAD script` - Load a script into the script cache
- [ ] `SCRIPT EXISTS sha1 [sha1 ...]` - Check existence of scripts in the script cache
- [ ] `SCRIPT FLUSH [ASYNC]` - Remove all scripts from the script cache
- [ ] `SCRIPT KILL` - Kill the script currently in execution

### 4. Transaction Commands

#### MULTI

- [ ] `MULTI` - Mark the start of a transaction block
  - [ ] `EXEC` - Execute all commands issued after MULTI
  - [ ] `DISCARD` - Discard all commands issued after MULTI

#### WATCH

- [ ] `WATCH key [key ...]` - Watch the given keys to determine execution of the MULTI/EXEC block
  - [ ] `UNWATCH` - Forget about all watched keys

### 5. Cluster Commands

#### CLUSTER

- [ ] `CLUSTER INFO` - Provides info about Redis Cluster node state
- [ ] `CLUSTER NODES` - Get Cluster config for the node
- [ ] `CLUSTER MEET ip port` - Force a node cluster to handshake with another node
- [ ] `CLUSTER FORGET node-id` - Remove a node from the nodes table
- [ ] `CLUSTER REPLICATE node-id` - Reconfigure a node as a replica of the specified master node
- [ ] `CLUSTER SAVECONFIG` - Force the node to save cluster state on disk
- [ ] `CLUSTER ADDSLOTS slot [slot ...]` - Assign new hash slots to receiving node
- [ ] `CLUSTER DELSLOTS slot [slot ...]` - Set hash slots as unbound in receiving node
- [ ] `CLUSTER FLUSHSLOTS` - Delete own slots information
- [ ] `CLUSTER SETSLOT slot IMPORTING|MIGRATING|STABLE|NODE node-id` - Bind a hash slot to a specific node
- [ ] `CLUSTER KEYSLOT key` - Returns the hash slot of the specified key
- [ ] `CLUSTER COUNTKEYSINSLOT slot` - Return the number of local keys in the specified hash slot
- [ ] `CLUSTER GETKEYSINSLOT slot count` - Return local key names in the specified hash slot

### 6. Data Types and Commands

#### Strings

##### SET

- [ ] `SET key value` - Set the string value of a key
- [ ] `SET key value EX seconds` - Set the string value of a key with expiration in seconds
- [ ] `SET key value PX milliseconds` - Set the string value of a key with expiration in milliseconds
- [ ] `SET key value NX` - Set the string value of a key only if the key does not exist
- [ ] `SET key value XX` - Set the string value of a key only if the key already exists
- [ ] `SET key value KEEPTTL` - Set the string value of a key and retain the time to live
- [ ] `SET key value GET` - Set the string value of a key and return its old value

##### GET

- [ ] `GET key` - Get the value of a key

##### DEL

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `DEL key [key ...]` - Delete one or more keys

##### EXISTS

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `EXISTS key [key ...]` - Determine if one or more keys exist

##### EXPIRE

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `EXPIRE key seconds` - Set a key's time to live in seconds
  - [ ] `EXPIRE key seconds NX` - Set a key's time to live in seconds only if the key does not exist
  - [ ] `EXPIRE key seconds XX` - Set a key's time to live in seconds only if the key already exists
  - [ ] `EXPIRE key seconds GT` - Set a key's time to live in seconds only if the new TTL is greater than current
  - [ ] `EXPIRE key seconds LT` - Set a key's time to live in seconds only if the new TTL is less than current

##### TTL

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `TTL key` - Get the time to live for a key in seconds
  - [ ] `PTTL key` - Get the time to live for a key in milliseconds

##### INCR/DECR

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `INCR key` - Increment the integer value of a key by one
  - [ ] `DECR key` - Decrement the integer value of a key by one
  - [ ] `INCRBY key increment` - Increment the integer value of a key by the given amount
  - [ ] `DECRBY key decrement` - Decrement the integer value of a key by the given amount
  - [ ] `INCRBYFLOAT key increment` - Increment the float value of a key by the given amount

### 7. Pub/Sub Commands

#### PUBLISH

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `PUBLISH channel message` - Post a message to a channel

#### SUBSCRIBE

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `SUBSCRIBE channel [channel ...]` - Listen for messages published to the given channels

#### UNSUBSCRIBE

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `UNSUBSCRIBE [channel [channel ...]]` - Stop listening for messages posted to the given channels

#### PSUBSCRIBE

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `PSUBSCRIBE pattern [pattern ...]` - Listen for messages published to channels matching the given patterns

#### PUNSUBSCRIBE

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `PUNSUBSCRIBE [pattern [pattern ...]]` - Stop listening for messages posted to channels matching the given patterns

### 8. Persistence Commands

#### SAVE

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `SAVE` - Synchronously save the dataset to disk

#### BGSAVE

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `BGSAVE [SCHEDULE]` - Asynchronously save the dataset to disk
  - [ ] `BGSAVE SCHEDULE` - Schedule a background save if no save is in progress

#### LASTSAVE

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `LASTSAVE` - Get the UNIX timestamp of the last successful save to disk

#### SHUTDOWN

- [ ] Basic implementation
- [ ] Flags:
  - [ ] `SHUTDOWN [SAVE|NOSAVE]` - Synchronously save the dataset to disk and then shut down the server
  - [ ] `SHUTDOWN SAVE` - Save the dataset to disk and then shut down the server
  - [ ] `SHUTDOWN NOSAVE` - Shut down the server without saving the dataset to disk

## Implementation Notes

- [x] indicates implemented commands
- [ ] indicates planned or in-progress commands
- Each command's flags are listed separately to track their implementation status
- Some commands may have additional flags not listed here
- Implementation status is based on the current state of the codebase
