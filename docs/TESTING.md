# Testing guide

[Back to the quick start](../README.md)

Install the optional client package shown in each example (`ioredis@5` or
`redis`). TypeScript examples require your test runner's TypeScript setup.
See the [README](../README.md) for runnable JavaScript examples.

## Use as a Redis mock in tests

`createRedisMock()` owns the whole lifecycle: it starts a standalone server
(16 databases, random free port) or a cluster. Connect your real client, then
use `seed()` and `flush()` explicitly when needed; data is not reset automatically.

`RedisMock` surface:

| Member                  | Description                                                                         |
| :---------------------- | :---------------------------------------------------------------------------------- |
| `host` / `port` / `url` | Connection coordinates of the (first) node.                                         |
| `addresses()`           | `{ host, port }[]` — one entry standalone, every node for cluster. Client-agnostic. |
| `seed(entries)`         | Preload data (see [Seeding](#seeding)).                                             |
| `flush()` / `reset()`   | Clear all keyspace data between tests.                                              |
| `close()`               | Shut down the server / cluster.                                                     |
| `state` / `nodes`       | Escape hatches to the underlying `RedisServerState` / node handles.                 |

### Connecting your client

A mock is a real server on a random free port, so connect any standard client
to `mock.addresses()` / `mock.url`:

```typescript
// ioredis
import { Redis } from 'ioredis'
const redis = new Redis(mock.addresses()[0])

// node-redis
import { createClient } from 'redis'
const client = createClient({ url: mock.url })
await client.connect()
```

Connections start on RESP2 and upgrade to RESP3 when the client asks for it
(for example, node-redis supports a `RESP: 3` option). RESP3 support and
configuration depend on the client and version; the `ioredis@5` examples here
use RESP2.

### node:test

```typescript
import { test, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { createRedisMock, type RedisMock } from 'js-redis-server'

let mock: RedisMock
let client: Redis

beforeEach(async () => {
  mock = await createRedisMock()
  client = new Redis(mock.addresses()[0])
})

afterEach(async () => {
  client.disconnect()
  await mock.close()
})

test('basic set/get operations', async () => {
  await client.set('foo', 'bar')
  assert.strictEqual(await client.get('foo'), 'bar')
})
```

### vitest / jest

```typescript
import { beforeEach, afterEach, test, expect } from 'vitest' // or '@jest/globals'
import { Redis } from 'ioredis'
import { createRedisMock, type RedisMock } from 'js-redis-server'

let mock: RedisMock
let client: Redis

beforeEach(async () => {
  mock = await createRedisMock()
  await mock.seed([{ key: 'counter', type: 'string', value: 1 }])
  client = new Redis(mock.addresses()[0])
})

afterEach(async () => {
  client.disconnect()
  await mock.close()
})

test('increments a seeded counter', async () => {
  expect(await client.incr('counter')).toBe(2)
})
```

Prefer a fresh `createRedisMock()` per test for full isolation; to reuse one
instance across a file, call `await mock.flush()` in `afterEach` instead.

### Cluster mocks

Same facade — pass `cluster`, then point a cluster client at every node via
`mock.addresses()`:

```typescript
const mock = await createRedisMock({ cluster: { masters: 3, replicas: 1 } })
```

```typescript
// ioredis
const cluster = new Redis.Cluster(mock.addresses())
```

```typescript
// node-redis
import { createCluster } from 'redis'
const cluster = createCluster({
  rootNodes: mock
    .addresses()
    .map(n => ({ url: `redis://${n.host}:${n.port}` })),
})
await cluster.connect()
```

Slots are computed exactly as Redis computes them. That includes a key whose
first hash tag is empty, such as `{}{foo}`: Redis hashes the whole key (slot
2263). ioredis and node-redis route with the `cluster-key-slot` package, which
hashes `{foo` instead (slot 13308). When those two slots belong to different
masters, the client sends the key to the wrong one and gets `MOVED`, against
the mock and a real cluster alike. ioredis follows the redirect. node-redis
looks the slot up again, reaches the same master each time, and fails with
the `MOVED` error once its redirections run out. With node-redis, avoid keys
whose first `{...}` is empty.

### Compatibility profiles

By default the mock exposes the newest implemented Redis behavior. Pass
`compatibility` when a test needs to match an older Redis or Valkey target:

```typescript
const redis62 = await createRedisMock({ compatibility: 'redis-6.2' })

const valkeyCluster = await createRedisMock({
  cluster: { masters: 3 },
  compatibility: 'valkey-9.0',
})
```

Profiles gate implemented commands, subcommands, options, and known behavioral
differences. For example, `EXPIRETIME key` is unavailable under `redis-6.2` but
available under newer Redis profiles. Unsupported commands remain unsupported
regardless of profile. See the current gate matrix in
[Compatibility Profiles](API.md#compatibility-profiles).

Supported presets: `redis-6.2`, `redis-7.0`, `redis-7.2`, `redis-7.4`,
`redis-8.0`, `valkey-8.0`, and `valkey-9.0`.

Suites that pin version-dependent wording follow `REDIS_COMPAT` through
`activeProfile` and the shared wording helpers in
`tests-integration/profile-wording.ts` (script error decoration, `NOSCRIPT`,
unknown-subcommand and arity errors). `npm run test:integration:profiles:mock`
runs the script, `COMMAND`, subcommand-error and MONITOR suites once per preset
(the `profile-suites-mock` CI job); each profile's expectations are checked
against a real server of that version (`scripts/agent-tools/probe.py`).
`npm test` also type-checks `scripts/` and `tests/` with `tsconfig.check.json`.

Valkey profiles model the Redis 7.0-era gates as enabled:

| Profile      | Redis 7.0 command/subcommand/option gates | Valkey-only modeled gate  |
| ------------ | ----------------------------------------- | ------------------------- |
| `valkey-8.0` | enabled                                   | cluster multi-DB disabled |
| `valkey-9.0` | enabled                                   | cluster multi-DB enabled  |

### Seeding

`seed()` takes an explicit entries array — you supply keys, types, values, and
optional `ttlMs` / `db`; the mock owns placement (including cluster slot
routing) and the internal value conversion.

```typescript
const mock = await createRedisMock()

await mock.seed([
  { key: 'user:1', type: 'string', value: 'alice' },
  { key: 'counter', type: 'string', value: 42 },
  { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
  { key: 'l:1', type: 'list', value: ['a', 'b', 1] },
  { key: 's:1', type: 'set', value: ['x', 'y'] },
  { key: 'z:1', type: 'zset', value: { a: 1, b: 2 } },
  { key: 'ttl:1', type: 'string', value: 'temp', ttlMs: 50_000 },
  { key: 'in-db-3', type: 'string', value: 'scoped', db: 3 },
])

// any client connected to the mock now sees the seeded keys
// (e.g. new Redis(mock.addresses()[0]) — GET user:1 → 'alice')
```

Each entry's shape is checked against its `type`:

```typescript
type SeedEntry =
  | {
      key: string
      type: 'string'
      value: string | number
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'hash'
      value: Record<string, string | number>
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'list'
      value: (string | number)[]
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'set'
      value: (string | number)[]
      ttlMs?: number
      db?: number
    }
  | {
      key: string
      type: 'zset'
      value: Record<string, number>
      ttlMs?: number
      db?: number
    }
```

`db` selects the logical database (standalone mocks). Streams are not seedable
yet. For anything beyond these shapes, drive your client directly or reach for
the `mock.state` escape hatch.

### `createRedisMock` options

```typescript
createRedisMock(options?: CreateRedisMockOptions): Promise<RedisMock>
```

| Parameter       | Type                                     | Default       | Description                                                  |
| :-------------- | :--------------------------------------- | :------------ | :----------------------------------------------------------- |
| `cluster`       | `{ masters: number; replicas?: number }` | `undefined`   | When set, builds a cluster mock instead of a standalone one. |
| `databaseCount` | `number`                                 | `16`          | Standalone-only: logical database count.                     |
| `compatibility` | `CompatibilitySpec`                      | `'redis-8.0'` | Redis / Valkey compatibility profile.                        |
| `port`          | `number`                                 | `0`           | Standalone bind port (`0` = OS-assigned).                    |
| `basePort`      | `number`                                 | `0`           | Cluster base port (`0` = each node OS-assigned).             |
| `logger`        | `Pick<Logger, 'error'>`                  | `undefined`   | Optional logger.                                             |

## Experimental: socketless client mocks

> **Experimental.** These return a client object directly — no socket, no
> port — so they skip the real network round-trip and (in some cases) real RESP
> encoding. They need no `addresses()` wiring, but skip operating-system networking,
> and their surfaces are still evolving. Prefer [`createRedisMock`](#use-as-a-redis-mock-in-tests) + a real
> client unless you have a specific reason not to.

Three flavours, depending on which client you want to look like:

| Helper                 | Looks like      | How                                                        |
| :--------------------- | :-------------- | :--------------------------------------------------------- |
| `createIoredisMock`    | `ioredis`       | the **real** ioredis client over a fake `net.Socket`       |
| `createNodeRedisMock`  | `node-redis`    | a hand-written facade mirroring node-redis' public surface |
| `createInMemoryClient` | our own bespoke | a thin client that returns native JS replies, no RESP      |

How far each one goes is measured by this repo's integration suites, which
also run against the socketless clients (`npm run test:integration:socketless`).
`createIoredisMock` passes the whole ioredis suite. The node-redis facade covers
a much smaller surface. The methods, signatures and argument forms it does not
support yet are listed in
[`tests-integration/socketless/known-gaps.ts`](../tests-integration/socketless/known-gaps.ts).

`createNodeRedisMock()` starts on the protocol the installed node-redis
negotiates by default: RESP3 on v6 (and when `redis` is not installed), RESP2
on v4 and v5. Its `RESP: 2 | 3` option picks one explicitly, as node-redis'
own `RESP` client option does. `createInMemoryClient()` starts on RESP2. Both
follow a `HELLO` the way a real connection does, so every
reply whose shape the protocol decides changes with it — RESP2 has no map,
double, boolean, big-number or pair type, and these clients hand back what a
real client reads off the wire at each version:

| reply                      | RESP2                    | RESP3                      |
| :------------------------- | :----------------------- | :------------------------- |
| `ZRANGE … WITHSCORES`      | `['a', '1', 'b', '2']`   | `[['a', 1], ['b', 2]]`     |
| `HRANDFIELD … WITHVALUES`  | `['f1', 'v1']`           | `[['f1', 'v1']]`           |
| `HGETALL`, `CONFIG GET`    | `['f1', 'v1']`           | `{ f1: 'v1' }`             |
| `XREAD`                    | `[['s', […]]]`           | `{ s: […] }`               |
| `ZSCORE`, `ZINCRBY`        | `'2.5'`                  | `2.5`                      |

Only the pair *shape* is shared between the first two rows: a `WITHSCORES`
score is a double, so it is a string at RESP2 and a number at RESP3, while a
`WITHVALUES` hash value is a bulk string at both.

A Lua script can return two more of these kinds. A `{big_number=…}` table is a
digit string at RESP2 and a `bigint` at RESP3. A boolean is `1` / `0` at RESP2
and `true` / `false` at RESP3 — but only from a script that has called
`redis.setresp(3)`, since without it real Redis converts a Lua `true` to the
integer `1` and `false` to nil. The typed tables (`{double=…}`,
`{big_number=…}`, `{map=…}`, `{set=…}`, `{verbatim_string=…}`) convert
whether or not the script called it, except on the `redis-6.2` profile, where
`{big_number=…}` and `{verbatim_string=…}` are not typed yet and reply `[]`,
as Redis 6.2 does. After `redis.setresp(3)`, `redis.call` also
hands the script RESP3 replies — `HGETALL` as a `{map=…}` table, `ZSCORE` as a
`{double=…}` table — so returning one gives the client a map or a double.

After `redis.setresp(3)` a set reply (`SMEMBERS`, `SINTER`, `SUNION`,
`SDIFF`, `SPOP` with a count) reaches the script as a `{set=…}` table, as in
Redis. The script gaps that still need Lua engine work (6.2's error handler, `redis.call` number arguments on
6.2 / 7.0, `set_repl` / `replicate_commands`) are tracked in
[#540](https://github.com/fatal10110/js-redis-server/issues/540).

(`createIoredisMock` drives the real `ioredis@5`, which is RESP2-only, so it
only ever sees the left column.) The *curated* methods on the node-redis facade
are protocol-independent where node-redis' own `transformReply` is: `hGetAll()`
returns an object at RESP2 as well, because the real client builds that object
from the flat array itself. The facade's raw paths follow the table:
`sendCommand()`, `eval()` (real node-redis gives `EVAL` no `transformReply`),
and `multi().addCommand(…).exec()`.

### `createIoredisMock` — virtual-socket ioredis client

`createIoredisMock()` is an option for tests otherwise using
[`ioredis-mock`](https://www.npmjs.com/package/ioredis-mock), but is not a
constructor-compatible or universally drop-in replacement. It returns a **real**
`ioredis` client wired to the
in-memory pipeline over a fake `net.Socket` — no TCP port, no loopback. Because
it's the genuine client speaking RESP, its command encoding and reply parsing
remain involved. The server's implemented commands, virtual transport and
lifecycle still have compatibility limits; validate your application's tests. `ioredis` is an optional peer
dependency, imported lazily — install `ioredis` yourself to use this helper.

```typescript
import { createIoredisMock } from 'js-redis-server'
import type { Redis } from 'ioredis'

const redis = (await createIoredisMock()) as Redis // 16 logical DBs by default

await redis.set('k', 'v')
await redis.get('k') // 'v'
await redis.hset('h', 'f1', 'a', 'f2', 'b')
await redis.hgetall('h') // { f1: 'a', f2: 'b' }

await redis.quit() // tears down the in-memory state
```

Pass `cluster` for a real `Cluster` client; keyed commands follow `MOVED`
in-process across the synthetic nodes:

```typescript
import type { Cluster } from 'ioredis'

const cluster = (await createIoredisMock({
  cluster: { masters: 3, replicasPerMaster: 1 }, // replicasPerMaster optional
})) as Cluster

await cluster.set('alpha', '1') // routed to its owning master
await cluster.get('alpha') // '1'

await cluster.quit()
```

Preload data with a `seed` array (same [`SeedEntry`](#seeding) shapes as
`createRedisMock().seed()`). The keyspace is populated before the client
connects, so it's ready on the first command. In cluster mode each key is
routed to its slot-owning master:

```typescript
const redis = (await createIoredisMock({
  seed: [
    { key: 'user:1', type: 'string', value: 'alice' },
    { key: 'h:1', type: 'hash', value: { name: 'bob', age: 30 } },
    { key: 'temp', type: 'string', value: 'x', ttlMs: 50_000 },
  ],
})) as Redis

await redis.get('user:1') // 'alice'

// cluster: createIoredisMock({ cluster: { masters: 3 }, seed: [...] })
```

### `createNodeRedisMock` — node-redis in-memory mock

node-redis exposes no socket hook, so this can't drive the real client over a
virtual socket the way `createIoredisMock` does. Instead `createNodeRedisMock()`
returns a **facade** that mirrors node-redis' public surface and routes every
command through the same in-memory pipeline. Its command methods are node-redis'
own ([#490](https://github.com/fatal10110/js-redis-server/issues/490)): every
entry of the installed node-redis' (5 or 6) `@redis/client` command map, in both
spellings (`zScore`, `ZSCORE`), builds its arguments with that command's
`parseCommand` and shapes the reply with its `transformReply` for the client's
configured `RESP`, just as `RedisClient` does, so `zScore()` is a number,
`xRange()` a list of `{ id, message }` and `hScan()` `{ cursor, entries }` at
either protocol. A few dozen methods stay hand-written (`get`, `set`, `del`,
`mSet`, `hSet`, `zAdd`, `zRange`, `eval`, pub/sub, `watch`, `multi`, ...), as do
node-redis' client methods `select()` (it resolves `undefined` and is selected
again after a reconnect) and `close()`. `sendCommand()` is the generic escape
hatch, which decodes replies to native JS. With `redis` absent, or on node-redis
4, only the hand-written methods exist. The generated methods are typed loosely
(`(...args: any[]) => Promise<any>`); cast the client to `RedisClientType` for
node-redis' own signatures. `withTypeMapping()`, the scan iterators and sharded
pub/sub (`sSubscribe`) are not modelled.

```typescript
import { createNodeRedisMock } from 'js-redis-server'

const client = await createNodeRedisMock() // 16 logical DBs by default

await client.set('k', 'v')
await client.get('k') // 'v'
await client.zAdd('z', { score: 2.5, value: 'm' })
await client.zScore('z', 'm') // 2.5, node-redis' own ZSCORE
await client.multi().zScore('z', 'm').incr('n').exec() // [2.5, 1]
await client.sendCommand(['HSET', 'h', 'f1', 'a']) // escape hatch

await client.quit() // tears down the in-memory state
```

Like a real client, the facade opens at node-redis' default protocol (RESP3
on node-redis 6) unless given `RESP`. `duplicate()` copies that option, and
the facade's pub/sub session always runs at the client's current protocol,
following a `HELLO` sent before or while subscribed. Like node-redis,
`duplicate()` returns the new client synchronously; unlike node-redis it is
already open (`isOpen` is true), so its `connect()` resolves at once and both
`duplicate()` + `await connect()` and using it directly work:

```typescript
const resp2 = await createNodeRedisMock({ RESP: 2 })
await resp2.sendCommand(['ZSCORE', 'z', 'm']) // '2.5' (RESP3: 2.5)
```

The hand-written methods take node-redis' argument forms
([#490](https://github.com/fatal10110/js-redis-server/issues/490)) and send the
argument list node-redis would: an array for the variadic ones (`sAdd(key,
['a', 'b'])`, `del`, `exists`, `lPush`, `rPush`, `watch`, `subscribe`,
`pSubscribe`, `unsubscribe`, `pUnsubscribe`; the facade also still takes them
as rest arguments), an object, `Map` or tuples for `hSet(key, fields)` and
`mSet(toSet)`, `set(key, value, { expiration: { type: 'EX', value: 10 }, NX:
true, GET: true })`, `expire(key, seconds, 'NX')`, `zAdd(key, members, { XX,
GT, CH })` and weighted `zUnionStore(dest, [{ key, weight }], { AGGREGATE })`.
`unsubscribe(channel, listener)` removes just that listener, and unsubscribes
the channel only once it has none left.
Like node-redis, `mSet()` does not stringify numbers: a number key or value
rejects with the encoder's `"arguments[N]" must be of type "string | Buffer",
got number instead.` (`set()` and `hSet()` values do accept numbers). Inside
`multi()`, such a command is left out, the rest and EXEC still run, and
`exec()` rejects with that `TypeError`.

`zRange()` and `zRangeWithScores()` take node-redis' `{ BY, REV, LIMIT }`
options and send them the way node-redis does, so `zRange(key, 5, 2, { BY:
'SCORE', REV: true })` is a reverse score range, not an index range.

#### Close path

The **single client's** close path follows real node-redis v6:

| | behaviour |
| --- | --- |
| `quit()` | resolves `'OK'`; **graceful** — commands already issued still run; `'end'` fires just before it resolves |
| `disconnect()` | resolves `undefined`; an alias for `destroy()` (real node-redis' is `Promise.resolve(this.destroy())`) |
| `destroy()` | returns `undefined` synchronously; flushes every command not yet answered — queued **or executing** — with `DisconnectsClientError`; `'end'` fires before it returns |
| `'end'` | emitted exactly **once** per close |
| `'error'` | never emitted by a clean close |
| any call on a closed client | throws `ClientClosedError` (`'The client is closed'`) — a rejection from `quit()`, but **synchronous** from `destroy()` *and* `disconnect()` |
| pub/sub call on a closed client | `subscribe()` / `pSubscribe()` / `unsubscribe()` / `pUnsubscribe()` do **not** throw: they wait for the next `connect()` and run after the re-subscribe, as node-redis queues them; never re-opened, they stay pending (without holding the event loop). A listener added to an already-subscribed channel resolves at once |

That last row matters for teardown code: because a redundant `disconnect()`
throws before any promise exists, `client.disconnect().catch(...)` does **not**
catch it — against the real client or the facade. Use `try`/`catch`, exactly as
you would against the real client:

```typescript
import { ClientClosedError } from 'redis'

try {
  await client.quit()
} catch (err) {
  if (!(err instanceof ClientClosedError)) throw err
}
```

**Graceful, then forced.** While a `quit()` is still draining — say behind a
`BLPOP 0` that will never be answered — the client is *closing*: new commands and
another `quit()` get `ClientClosedError`, but `destroy()` or `disconnect()` can
still force it. As in real node-redis v6, that rejects the pending `quit()` and
every command still pending with `DisconnectsClientError` — and the forcing
`destroy()`/`disconnect()` call **itself still throws `ClientClosedError`**
synchronously, and no `'end'` is emitted. (Real node-redis flushes its queue
first, then reaches a socket the pending `quit()` already marked closed.) Real
node-redis also leaks the socket on that path, so the process never exits; the
facade deliberately does not reproduce that, and tears everything down.

That `instanceof` works because the facade throws the `redis` package's *own*
error classes (the same mechanism behind `WatchError` / `ErrorReply` /
`SimpleError` / `MultiErrorReply`). Server errors are `SimpleError` — a subclass
of `ErrorReply` — exactly as real node-redis v5+ decodes them, so both
`instanceof ErrorReply` and `instanceof SimpleError` hold; on redis 4.1–4.x,
which has no `SimpleError`, they are plain `ErrorReply`, again as that version
throws. The classes are resolved lazily, the first time a facade client is
constructed or an error is decoded — importing `js-redis-server` never loads
`redis`, so ioredis-only users don't pay for it.

They are resolved one at a time, and the facade follows the installed version:
a class it does not export costs only that class, never the ones that do exist.
Before redis 4.6.12 there is no `MultiErrorReply`, and — as on those releases —
`exec()` then resolves with a failed command's `ErrorReply` inline in the reply
array instead of throwing. A command that fails to *queue* (wrong arity, an
unknown command) is different on every version: `exec()` rejects with that first
command's own error — not `EXECABORT`, and not a `MultiErrorReply` — because
node-redis pipelines MULTI, the commands and EXEC and awaits them together. An
error nested in an array or map reply (`EVAL "return {1,
redis.error_reply('x')}" 0`) is returned in place as a `SimpleError`; only a
top-level error rejects. redis 4.0.x exports no `ErrorReply` at all, so server
errors there are a local look-alike that is not `instanceof` anything you can
import; match on `err.message` on 4.0.x. The same look-alikes (matching node-redis
v6 in message, `name` and `constructor.name`) are used if `redis` cannot be
required at all.

**Re-opening:** `connect()` on a closed client re-opens it, as real node-redis
does ([#440](https://github.com/fatal10110/js-redis-server/issues/440)): a new
connection to the same in-memory server (the keyspace survived the close) at the
configured `RESP` and database, `'connect'` and `'ready'` again, and a second
`'end'` on the next close. Like the real client it counts as open as soon as
`connect()` is called, so a command issued right after it runs on the new
connection; called while a `quit()` still drains, it waits for that close. A
`watch()` from before the reconnect fails the next `exec()` with
`WatchError('Client reconnected after WATCH')`, as on node-redis. The pub/sub
listeners survive the close, whether `quit()`, `destroy()` or `disconnect()`,
and the re-open subscribes every channel and pattern that still has one again
(one `SUBSCRIBE`, one `PSUBSCRIBE`) before `'ready'`, as node-redis' reconnect
does ([#600](https://github.com/fatal10110/js-redis-server/issues/600)); one
unsubscribed before the close stays unsubscribed. A pub/sub call made while
closed runs after that re-subscribe. Real node-redis 6 never settles a bare
`unsubscribe()` / `pUnsubscribe()` queued while closed when channels were
subscribed before the close; the facade resolves it after `connect()`.
`connect()` on an open client
stays a no-op (real node-redis rejects `Socket already opened`).

The **cluster** client closes on node-redis' own, quite different, cluster
terms: `quit()` / `disconnect()` / `destroy()` all resolve `undefined`, they
emit `'disconnect'` (never `'end'`) once per close *call* — after `quit()` and
`disconnect()` return but before their promise settles, and in-line from the
synchronous `destroy()` — and a redundant close is a silent no-op rather than a
throw. The one deliberate deviation is that a
command issued on a closed cluster throws `ClientClosedError`, where real
node-redis v6 crashes with an internal `TypeError` from its own reset slot map.

Pass `cluster` for a cluster facade; keyed commands route by slot in-process.
Routing keys come from `CommandExecutor.plan()` — the same extraction
`ClusterPolicy` uses — so multi-key commands (`MSET`, `RENAME`), numkeys-prefixed
ones (`EVAL`, `ZUNIONSTORE`) and STORE targets all reach the right node, and a
key set spanning slots is refused with `CROSSSLOT`. The facade routes by the
server's own slot, so unlike real node-redis it reaches the owner of a key
whose first hash tag is empty (see [Cluster mocks](#cluster-mocks)).

The cluster facade has node-redis' cluster surface too:
`sendCommand(firstKey, isReadonly, args)` (a keyed command runs on the owner
of its own keys, where a real client following `MOVED` ends up; a keyless one
on `firstKey`'s owner), `masters` (`{ id, host, port, address, readonly }`
nodes, at synthetic ports from 7000 that nothing listens on), `slots` (16384
`{ master }` shards; node-redis adds `replicas` only with `useReplicas`),
`isOpen`, `close()`, `multi(routing?)` (the transaction runs on the owner of
`routing`, else of the first queued command's key), and a synchronous
`duplicate()`: a second client on the same nodes
and keyspace with its own connection per node, whose close leaves the nodes to
the client that built them:

```typescript
const cluster = await createNodeRedisMock({
  cluster: { masters: 3, replicas: 1 },
})

await cluster.set('alpha', '1')
await cluster.get('alpha') // '1'
await cluster.sendCommand('alpha', true, ['GET', 'alpha']) // '1'

const second = cluster.duplicate() // same keyspace, own connections
await second.get('alpha') // '1'
await second.quit()

await cluster.quit()
```

### `createInMemoryClient` — our own socketless client

If you don't need to look like any particular client library,
`createInMemoryClient()` returns an in-process client with its **own** keyspace
that drives the command pipeline directly — no TCP loopback, no RESP encoding —
and resolves to native JS replies (throwing `RedisCommandError` on `-ERR`).
Standalone only.

```typescript
import { createInMemoryClient } from 'js-redis-server'

const client = await createInMemoryClient({
  // databaseCount?, database?, returnBuffers?, seed?
})

await client.command('SET', 'k', 'v')
await client.command('GET', 'k') // 'v'
await client.command('INCR', 'n') // 1 (number)
await client.command('HGETALL', 'h') // ['field', 'value', ...] — RESP2 shape
await client.command('HELLO', 3)
await client.command('HGETALL', 'h') // { field: 'value', ... } — RESP3 shape

client.close() // tears down its keyspace
```

It takes the same `seed` array as `createRedisMock().seed()` to pre-populate its
keyspace before the first command. Need to drive an existing `createRedisMock()`'s
keyspace instead of an independent one? Construct `InMemoryRedisClient` directly
with that mock's `state` and an executor from `js-redis-server/core`.
