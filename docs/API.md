# Server & Low-Level API

The [README](../README.md) introduces CLI, TCP and socketless usage. This
reference covers listening TCP servers, clusters, and low-level APIs. All
modes use the JavaScript implementation, not a native Redis binary.

For test lifecycle helpers, seeding and socketless APIs, see the
[testing guide](TESTING.md).

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
  - [`createVirtualConnection`](#createvirtualconnection)

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
> `RedisCluster` you call `.listen()` on yourself.

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

A `{ flavor, version }` object instead of a preset pins any release, down to
the patch: `{ flavor: 'valkey', version: '9.0.5' }`. The presets are pinned
to the versions above, so behavior that only later patch releases have (the
rows marked "patch" below) needs a custom version.

Each gate is a `VersionGate` (exported, with `gateSatisfied` and
`FEATURE_GATES`) naming, per flavor, the first version that has the behavior:
`{ redis: '7.0.0', valkey: '7.2.0' }` means Redis 7.0.0 and later, and Valkey
7.2.0 and later. A change backported to patch releases lists one minimum per
`major.minor` branch: `{ redis: ['7.2.16', '7.4.11', '8.2.9'] }` is met by
7.2.16+ on 7.2, 7.4.11+ on 7.4 and 8.2.9+ on 8.2, by every version of a branch
newer than the last entry (8.4.0 and later), and by no version of an older
branch that is not listed (7.0.x, 8.0.x). A flavor the gate does not name
never has the behavior.

Valkey profiles model the Redis 7.0-era gates as enabled:

| Profile | Redis 7.0 command/subcommand/option gates | Valkey-only modeled gate |
| --- | --- | --- |
| `valkey-8.0` | enabled | cluster multi-DB disabled |
| `valkey-9.0` | enabled | cluster multi-DB enabled |

Current profile gates:

| Surface | Redis profiles | Valkey profiles |
| --- | --- | --- |
| Redis 6.2 root commands: `BLMOVE`, `COPY`, `GETDEL`, `GETEX`, `HRANDFIELD`, `LMOVE`, `RESET`, `SMISMEMBER`, `XAUTOCLAIM`, `ZMSCORE` | `redis-6.2+` | `valkey-8.0+` |
| Redis 7.0 root commands: `BLMPOP`, `BZMPOP`, `EXPIRETIME`, `LMPOP`, `PEXPIRETIME`, `SINTERCARD`, `SORT_RO`, `SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE`, `ZINTERCARD`, `ZMPOP` | `redis-7.0+` | `valkey-8.0+` |
| Redis 7.4 hash-field expiration commands: `HEXPIRE`, `HEXPIREAT`, `HEXPIRETIME`, `HPERSIST`, `HPEXPIRE`, `HPEXPIREAT`, `HPEXPIRETIME`, `HPTTL`, `HTTL` | `redis-7.4+` | `valkey-9.0+` |
| `HSCAN ... NOVALUES` | `redis-7.4+` | `valkey-8.0+` |
| `XREAD ... +` latest-entry stream ID | `redis-7.4+` | unsupported |
| `XREAD` / `XREADGROUP` `Unbalanced '<cmd>' list of streams` wording per command (6.2 / 7.0: `Unbalanced XREAD list of streams` for both) | `redis-7.2+` | `valkey-8.0+` |
| `'+'` in XREAD's `Unbalanced` error (`an ID, '+', or '$' must be specified`; 7.4 accepts `+` but does not list it) | `redis-8.0+` | never |
| `COMMAND DOCS` summaries in the 7.2 wording (`A container for stream introspection commands.`, with the period) | `redis-7.2+` | `valkey-8.0+` |
| `COMMAND INFO` flags, ACL categories, tips and key specs: the real command table of the profile's version (captured from redis-server 6.2.24, 7.0.15, 7.2.4, 7.4.4, 8.0.6 and valkey 8.0.11 / 9.0.6; Valkey 7.2 reads Redis 7.2's), for example `variable_flags` on the `GEORADIUS` `STORE` / `STOREDIST` key specs. A preset versioned below its captured patch answers as that patch: `redis-6.2` (6.2.14) reports 6.2.24's `denyoom` on `SUBSCRIBE` / `PSUBSCRIBE`, which 6.2.14 lacks | per version | per version |
| `incomplete` key specs, with `notes`, on `XREAD` / `XREADGROUP` (STREAMS) and `GEORADIUS` / `GEORADIUSBYMEMBER` (`STORE`, `STOREDIST`) in `COMMAND INFO`; `COMMAND GETKEYS` / `GETKEYSANDFLAGS` then use the getkeys procedure, so `XREAD STREAMS a b 0` answers `Invalid arguments specified for command` and the last of repeated `STORE` / `STOREDIST` options names the destination | patch: 7.2.16+, 7.4.11+, 8.2.9+, 8.4.6+, 8.6.6+, 8.8.2+, 8.10+ (never 7.0 or 8.0) | never |
| `CLIENT KILL MAXAGE` | `redis-7.4+` | `valkey-8.0+` |
| `CLIENT KILL ID` `client-id should be greater than 0` (6.2: any integer, `ID 0` is no filter) | `redis-7.0+` | all |
| `CLIENT KILL TYPE primary` | never | `valkey-8.0+` |
| `CLIENT KILL ID id [id ...]` (several ids after one `ID`) | never | `valkey-9.0+` |
| Redis 8.0 hash-field read commands: `HGETDEL`, `HGETEX` | `redis-8.0+` | `HGETEX` in `valkey-9.0`; `HGETDEL` is not modeled for Valkey |
| Redis 8.0 hash-field write command: `HSETEX` | `redis-8.0+` | `valkey-9.0+` |
| `COMMAND DOCS` and `COMMAND GETKEYSANDFLAGS` | `redis-7.0+` | `valkey-8.0+` |
| `COMMAND INFO` entries with tips, key specs and subcommands (10 fields; 6.2 has 7, ending with the ACL categories) | `redis-7.0+` | `valkey-8.0+` |
| `COMMAND LIST`, a bare `COMMAND INFO` listing every command (6.2: an empty array) and per-subcommand arity errors (6.2 dispatches on the argument count and answers `Unknown subcommand or wrong number of arguments for '<sub>'. Try COMMAND HELP.`) | `redis-7.0+` | `valkey-8.0+` |
| A `QUIT` command-table entry (`COMMAND INFO quit`, `COMMAND GETKEYS QUIT`) | `redis-7.0+` | `valkey-8.0+` |
| `COMMAND HELP` without "Redis" (`Return details about all commands.`) | never | `valkey-8.0+` |
| An empty array, not an empty set, for a `COMMAND INFO` entry without subcommands (RESP3 `*0` rather than `~0`), as Valkey 7.2.14, 8.0.11 and 9.0.6 send it; Valkey 8.0.0, 9.0.0 and 9.0.1 still send `~0`, so the `valkey-8.0` / `valkey-9.0` presets (8.0.0 / 9.0.0) differ from those first releases | never | `valkey-8.0+` |
| `CLIENT NO-EVICT` and multi-section `INFO` | `redis-7.0+` | `valkey-8.0+` |
| `CLIENT NO-TOUCH`, and `timeout is out of range` for a `CLIENT PAUSE` end time past `LLONG_MAX` (before: `+OK`, nothing paused) | `redis-7.2+` | `valkey-8.0+` |
| `CLIENT PAUSE` accepts arguments after the mode, reading the mode only when it is the last argument (`CLIENT PAUSE 10 WRITE x` pauses ALL) | never | `valkey-8.1+` (not a preset; `valkey-9.0`) |
| `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` `NX`, `XX`, `GT`, `LT` options (before them: arity 3, and any extra token is `wrong number of arguments`) | `redis-7.0+` | `valkey-8.0+` |
| `SET GET`, `SET EXAT`, `SET PXAT` | `redis-6.2+` | `valkey-8.0+` |
| `SET` with a deadline already past writes nothing and deletes an existing key (Redis writes the key with the past TTL and publishes `set` / `expire`) | never | `valkey-8.0+` |
| A key deleted by a deadline already past (`SET`, `GETEX EXAT\|PXAT`, the `EXPIRE` family) publishes `expired` (class `x`), not `del` | never | `valkey-9.0+` |
| `ZRANK`/`ZREVRANK` `WITHSCORE` (before it: arity 3, and a trailing token is `wrong number of arguments`) | `redis-7.2+` | `valkey-8.0+` |
| `XSETID` `ENTRIESADDED`/`MAXDELETEDID` (before it: arity 3, and a trailing token is `wrong number of arguments`) | `redis-7.0+` | `valkey-8.0+` |
| `COMMAND GETKEYS`/`GETKEYSANDFLAGS` need an argument after the target command (arity -4; `COMMAND GETKEYS GET` is a `command\|getkeys` arity error) | `redis-7.0` only | never |
| `GEOSEARCH`, `GEOSEARCHSTORE` | `redis-6.2+` | `valkey-8.0+` |
| `SET NX GET` | `redis-7.0+` | `valkey-8.0+` |
| `CLIENT SETINFO` | `redis-7.2+` | `valkey-8.0+` |
| `ssub`, `multi-mem`, `rbs`, `rbp` and `resp` in `CLIENT LIST` / `CLIENT INFO`, and `cmd` naming `container\|subcommand` (6.2: the container); `lib-name` / `lib-ver` follow `CLIENT SETINFO` | `redis-7.0+` | `valkey-8.0+` |
| `watch` in `CLIENT LIST` / `CLIENT INFO` | `redis-7.4+` | `valkey-8.0+` |
| `io-thread` in `CLIENT LIST` / `CLIENT INFO` | `redis-8.0+` | never |
| `tot-net-in`, `tot-net-out`, `tot-cmds` in `CLIENT LIST` / `CLIENT INFO` | never | `valkey-8.0+` |
| `capa` in `CLIENT LIST` / `CLIENT INFO` | never | `valkey-9.0+` |
| A CLIENT TRACKING client's own sync `FLUSHDB` / `FLUSHALL` (no `ASYNC`, outside MULTI and scripts) writes its null invalidation before the `+OK` (a blocking async flush); before: after it | `redis-7.4+` | never |
| A readonly command run from a script makes a CLIENT TRACKING client remember that command's keys; before: the keys the script declared | `redis-7.2+` | `valkey-7.2+` |
| `subexpiry` (hashes with field TTLs) on the `INFO keyspace` line | `redis-7.4+` | never |
| `keys_with_volatile_items` (the same count) on the `INFO keyspace` line | never | `valkey-9.0+` |
| Sharded Pub/Sub: `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH`, `PUBSUB SHARDCHANNELS`, `PUBSUB SHARDNUMSUB` | `redis-7.0+` | `valkey-8.0+` |
| RESP3 subscribed `PUBLISH` self-reply before pushed message | `redis-7.2+` | `valkey-8.0+` |
| `XAUTOCLAIM` deleted-entry ID reply shape, `XCLAIM` / `XAUTOCLAIM` dropping a deleted entry from the PEL (6.2 claims it and replies nil for it), and `XAUTOCLAIM COUNT` capped at `LONG_MAX / 16` (6.2: `LONG_MAX`) | `redis-7.0+` | `valkey-8.0+` |
| Consumer group lag tracking: `XGROUP CREATE` / `SETID` `ENTRIESREAD`, `entries-read` / `lag` in `XINFO GROUPS` and `XINFO STREAM FULL`, and `max-deleted-entry-id` / `entries-added` / `recorded-first-entry-id` in `XINFO STREAM` (6.2's `XGROUP CREATE` takes exactly `MKSTREAM`, checked before the key, and `SETID` no option) | `redis-7.0+` | `valkey-8.0+` |
| Consumer active time: `inactive` in `XINFO CONSUMERS`, and `XREADGROUP` / `XCLAIM` / `XAUTOCLAIM` creating the consumer before looking for anything to deliver or claim (6.2 / 7.0 create it only once something is, or on any `XREADGROUP` history read) | `redis-7.2+` | `valkey-8.0+` |
| `BITCOUNT`/`BITPOS` `BYTE`\|`BIT` range modifier | `redis-7.0+` | `valkey-8.0+` |
| `Protocol error: invalid CRLF in request` (then the connection closes) for a header-line CR not followed by LF, or a bulk not followed by CRLF (Redis and Valkey x.0.0 skip those bytes unread: `*1\r\n$4\r\nPINGXX` runs `PING`) | never | patch: 7.2.14+, 8.0.8+, 8.1.8+, 9.0.5+, 9.1+ |
| RESP multibulk element count bounded at `INT_MAX` (6.2 refuses more than `1024*1024` with `Protocol error: invalid multibulk length`) | `redis-7.0+` | `valkey-8.0+` |
| Unknown-subcommand error wording for every container command (`unknown subcommand '<name>'. Try <CMD> HELP.` with the echoed name cut at 128 bytes, vs. 6.2's untruncated `Unknown subcommand or wrong number of arguments for '<name>'.`) | `redis-7.0+` | `valkey-8.0+` |
| Container-subcommand resolution at command-lookup time, before anything else sees the command: `MULTI` refuses to queue an unknown subcommand and `EXEC` answers `EXECABORT`, `XGROUP`/`XINFO` never look their key up first, a script's `redis.call`/`redis.pcall` gets `Unknown Redis command called from script`, and `COMMAND GETKEYS`/`GETKEYSANDFLAGS` answer `Invalid command specified` and `ACL DRYRUN` `Command '<name>' not found` (6.2 rejects the subcommand only when it runs, and `XGROUP`/`XINFO` check the key first) | `redis-7.0+` | `valkey-8.0+` |
| `XINFO HELP` / `XGROUP HELP` as keyless arity-2 subcommands (`xinfo\|help`/`xgroup\|help` arity error with an argument; 6.2 answers `XINFO HELP <anything>` and treats `XGROUP HELP <args>` as an unknown subcommand, key first) and the `XGROUP HELP` text documenting `ENTRIESREAD` | `redis-7.0+` | `valkey-8.0+` |
| `Print this help.` as the last line of `XINFO HELP` / `XGROUP HELP` (6.2 / 7.0: `Prints this help.`; the other containers' HELP still says `Prints` on every profile) | `redis-7.2+` | `valkey-8.0+` |
| Valkey's wording for the scripting layer's own errors, without `Redis`: `Unknown command called from script`, `Wrong number of args calling command from script`, `Please specify at least one argument for this call`, `Command arguments must be strings or integers` | never | `valkey-8.0+` |
| `This Valkey command is not allowed from script` for a `noscript` command called from a script | never | `valkey-9.0+` |
| `CONFIG SET` failure wording (`CONFIG SET failed (possibly related to argument '<name>')` vs. 6.2's `Invalid argument '<value>' for CONFIG SET '<name>'`, which echoes the name as sent and has no detail suffix for `notify-keyspace-events`), and unknown-parameter wording (`Unknown option or number of arguments for CONFIG SET - '<name>'` vs. 6.2's `Unsupported CONFIG parameter: <name>`) | `redis-7.0+` | `valkey-8.0+` |
| `CONFIG SET` with several parameter/value pairs, its `config\|set` arity / `syntax error` split, and `duplicate parameter` detection (6.2 accepts exactly one pair and answers any other shape with `Unknown subcommand or wrong number of arguments for 'SET'.`) | `redis-7.0+` | `valkey-8.0+` |
| `n` (new-key) class in `notify-keyspace-events` (6.2 rejects it as an invalid flag character) | `redis-7.0+` | `valkey-8.0+` |
| Unknown-command error wording (`unknown command '<name>', with args beginning with: '<arg>' ...` with the name cut at 128 bytes, vs. 6.2's backtick-quoted, `, `-separated form with the whole name; both echo args against a 128-byte budget, so 6.2 echoes fewer) | `redis-7.0+` | `valkey-8.0+` |
| Subscribed-mode refusal naming the looked-up table entry and listing sharded Pub/Sub (`Can't execute 'config\|get': only (P\|S)SUBSCRIBE / (P\|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`, vs. 6.2's `Can't execute 'config': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / ...`) | `redis-7.0+` | `valkey-8.0+` |
| `MSET` / `MSETNX` / `XADD` odd field/value-count error in the standard arity wording (`wrong number of arguments for 'mset' command`, vs. 6.2's `wrong number of arguments for MSET` for both MSET forms and `... for XADD`) | `redis-7.0+` | `valkey-8.0+` |
| Script-abort error decoration (`<error> script: <sha>, on @user_script:<line>.`, keeping a failing `redis.call`'s own error code, vs. 6.2's `-ERR Error running script (call to f_<sha>): @user_script:<line>: <error>`). The same gate picks the wording of script-level rejections (unknown / not-allowed command, wrong arity, no command): 6.2's `... from Lua script` forms carry no error code and, from `redis.call`, an inner `@user_script: <line>: ` position, which a `redis.pcall` rejection carries too (`=[C]: -1: ` when called through the global `pcall`) | `redis-7.0+` | `valkey-8.0+` |
| `CONFIG SET` rejecting a memory value above the parameter's maximum (6.2 saturates to the maximum instead) | `redis-7.0+` | `valkey-8.0+` |
| Cluster `SELECT` for non-zero databases | unsupported | `valkey-9.0` |
| `SORT`/`SORT_RO` cluster `BY`/`GET` patterns compared by slot (and the longer `...may be in different slots.` error wording) instead of refused outright | `redis-7.4+` | `valkey-8.0+` |
| `SORT`/`SORT_RO` cluster `GET '#'` exempt from that slot comparison | `redis-7.4+` (7.4.2) | `valkey-9.0+` (8.0.2) |
| GEO coordinate text (`GEOPOS`, `WITHCOORD`) spelled by `d2string()` (`13.361389338970184`) instead of `%.17Lf` trimmed (`13.36138933897018433`); a `,` double on RESP3 on every profile | `redis-8.0+` | unsupported (every Valkey keeps `%.17Lf`) |
| Double reply text (`ZSCORE`, `ZINCRBY`, `ZMSCORE`, `WITHSCORES`, RESP3 `,` doubles, scores read by `redis.call`, `ZSCAN` of a listpack; a skiplist's `ZSCAN` is `%.17Lg` on every profile) spelled by `d2string()` / `fpconv_dtoa` (`0.1`, `1.23e-5`, `4611686018427387904`) instead of 6.2 / 7.0's `%.17g` (`0.10000000000000001`, `1.2300000000000001e-05`, `4.6116860184273879e+18`) | `redis-7.2+` | `valkey-8.0+` |
| Lua scripts resolve a `noscript` container's (`CLIENT`, `ACL`, `SCRIPT`, `CONFIG`, `FUNCTION`) subcommand before refusing it: `<container> HELP` runs, and an unknown subcommand fails lookup (see the container-subcommand row). 6.2 refuses every subcommand. (The mock has no `CONFIG HELP` yet, so that one call still errors.) | `redis-7.0+` | `valkey-8.0+` |
| `QUIT` from a Lua script is refused as `noscript` (6.2 has no `QUIT` table entry: unknown command) | `redis-7.0+` | `valkey-8.0+` |
| `BLPOP` / `BRPOP` / `BLMOVE` / `BZPOPMIN` / `BZPOPMAX` callable from a Lua script, without blocking: with nothing to pop they answer the timeout reply at once, even with timeout 0 (6.2 / 7.0 refuse them as `noscript`; `BLMPOP` / `BZMPOP` run this way from 7.0) | `redis-7.2+` | `valkey-8.0+` |
| Listpack sets' encoding rules: `SADD` creates an intset only when its member count fits `set-max-intset-entries` (so with the limit at 2, `SADD s 3 1 3` keeps `3 1`; 6.2 / 7.0 sort it), and `SPOP`'s rebuild of a large pop keeps a non-intset set out of the intset encoding (6.2 / 7.0 turn all-integer survivors into a sorted intset) | `redis-7.2+` | `valkey-8.0+` |
| Sorted-set `ZADD` size hint: a `ZADD` (or `GEOADD`) of more pairs than `zset-max-listpack-entries` makes a skiplist up front, duplicate members included, so `ZADD k -0 m` repeated 129 times keeps `-0` (7.0 stores the listpack's `0`) | `redis-7.2+` | `valkey-8.0+` |
| Sorted-set `ZADD` longest-member hint: a `ZADD` member longer than `zset-max-listpack-value` makes (or converts to) a skiplist up front, even one `XX` skips; Redis reads only the first member's length, on creation | unsupported | `valkey-8.0+` |
| A non-`STORE` `SUNION` / `SDIFF` (and `SPOP` with a count that covers the set) with a non-intset source builds its result as a hashtable, so a listpack's integers keep their order (`SADD u x 3 1`, `SREM u x`, then `SUNION u` replies `3 1`; 7.4 and earlier sort it to `1 3`) | `redis-8.0+` | `valkey-8.0+` |
| `redis.REDIS_VERSION` / `redis.REDIS_VERSION_NUM` in scripts (Valkey reports `7.2.4` there and adds `SERVER_NAME`, `VALKEY_VERSION` and `VALKEY_VERSION_NUM`; 6.2 has none of them) | `redis-7.0+` | `valkey-8.0+` |
| A script's returned `{big_number=...}` / `{verbatim_string=...}` table converts to a big number / verbatim string (a bulk string on RESP2), at any depth; 6.2 sends it as an empty array (`*0`) | `redis-7.0+` | `valkey-8.0+` |
| `#!lua [flags=...]` shebang scripts: `EVAL` / `EVALSHA` / `EVAL_RO` / `SCRIPT LOAD` check the shebang (`Unexpected flag in script shebang: ...`, `Unknown lua shebang option: ...`, `Invalid script shebang`, ...) and run the body with the shebang line blanked, so line numbers count it; `no-writes`, `no-cluster` and, on a cluster node, the one-slot rule `allow-cross-slot-keys` lifts are enforced (6.2 compiles the shebang as Lua: `unexpected symbol near '#'`) | `redis-7.0+` | `valkey-8.0+` |
| Shebang engine looked up by name, ignoring case, after the options (`#!LUA` runs; `#!notlua` is `Could not find scripting engine 'notlua'`, vs. `Unexpected engine in script shebang: #!notlua`) | never | `valkey-9.0+` (8.1) |
| `EVALSHA` miss worded `-NOSCRIPT No matching script.`, without ` Please use EVAL.` | never | `valkey-8.0+` |

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
> need control over when `listen()` runs.

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
| `nodeRole` | `RedisClusterNodeRole`  | No       | Cluster role; a replica redirects keyed commands to master.    |

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

---

### `createVirtualConnection`

Creates an in-process connection to a server pipeline without opening a socket:
the returned `clientSocket` is a `net.Socket`-shaped stream you hand to a client
library, and the server side runs on the other end of a `stream.duplexPair()`.

> Imported from the `js-redis-server/core` subpath, not the package root.

```typescript
import { createVirtualConnection } from 'js-redis-server/core'

const { clientSocket, done, close } = createVirtualConnection(options)
```

`options` takes the `state` (`RedisServerState`) and `executor`
(`CommandExecutor`), and optionally `nodeRole`, `logger`, `remoteAddress` and
`remotePort`. `done` resolves once the server-side session has ended, and
`close()` ends it and half-closes the wire.

**Known limits.** The in-process transport (`SocketConnectionTransport`, which
also serves TCP sockets) differs from a real TCP connection in three ways. None
of them affects ioredis or node-redis, which always read; they matter only to
code that drives the stream directly. See
[Known limits of the in-process transport](ARCHITECTURE.md#known-limits-of-the-in-process-transport).

1. **Event order when the client ends first.** With a pipelined reply, the
   in-process wire emits `data, finish, end, close` on the client end; real TCP
   emits `finish, data, end, close`.
2. **A bounded `Duplex` can stay parked.** The client's EOF is only seen while
   the read loop is reading. A size-limited stream that is blocked writing an
   ordinary reply to a client that stopped reading stays parked until the
   stream closes. The same holds over TCP for a client that half-closes without
   reading.
3. **`readable.iterator({ destroyOnReturn })` is experimental in Node.** The
   TCP and virtual read paths depend on it, so a Node release that changes or
   removes it needs a change in `SocketConnectionTransport`.
