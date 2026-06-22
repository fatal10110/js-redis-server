# Plan: In-memory drop-in mocks for `ioredis` and `node-redis`

## Context

Today the project ships a real RESP server that runs in-process. Two ways to talk to it:
- **TCP loopback** (`createRedisMock({ transport: 'tcp' })`) — real `ioredis`/`node-redis` clients connect over a real socket. Works, but it is real network I/O (binds a port, OS sockets), not pure in-memory.
- **`InMemoryRedisClient`** (`transport: 'memory'`) — socketless, but a *bespoke* client (`.command('GET','k')`). It is not `ioredis`/`node-redis`, bypasses RESP entirely, and can't do pub/sub push.

Gap: there is no **drop-in** that gives users a real-shaped `ioredis` / `node-redis` client backed by the in-memory pipeline (the `ioredis-mock` experience) **without** a TCP socket.

Goal: add `createIoredisMock()` and `createNodeRedisMock()` (standalone **and** cluster) that return client objects matching each library's public interface, driven by the existing in-memory server pipeline — no real network, no global monkeypatching.

**Approach (decided): Hybrid.**
- **ioredis** has a sanctioned `Connector` hook → hand it a fake `net.Socket`-shaped Duplex bridged to a server-side `ClientSession`. We drive the **real ioredis client** over real RESP → full fidelity (reply shaping, pub/sub, pipelines, `.multi`, `scanStream`) for free.
- **node-redis** has **no** socket/connector hook (`#socketFactory` is private) → avoiding a `net.createConnection` monkeypatch means we **replicate its client interface** as a facade over the shared in-memory server.

This is additive. It does not replace the client-agnostic core; it is a convenience layer on top. (Note the tension with `docs/why-not-ioredis-mock.md`, which markets the project as client-agnostic — the ioredis path keeps full protocol fidelity; only the node-redis facade re-enters the "fake the client API" space, by necessity.)

---

## Core shared primitive: the in-memory wire

The reusable piece both the ioredis Connector and the server side need is a **virtual connection**: a fake client-facing `net.Socket` already wired to a fresh server-side session.

### 1. Extract session wiring — `src/core/transports/attach-session.ts`
`Resp2Server.handleConnection` ([src/core/transports/resp2/server.ts:88](src/core/transports/resp2/server.ts#L88)) hard-codes `transport → ClientSession → Resp2SessionAdapter → adapter.run()`. Extract that into:

```ts
export function attachSession(
  transport: ConnectionTransport,
  opts: { state: RedisServerState; executor: CommandExecutor;
          nodeRole?: RedisClusterNodeRole; logger?: Pick<Logger,'error'>;
          clientAddress?: string },
): { session: ClientSession; done: Promise<void>; close(): void }
```

Refactor `Resp2Server.handleConnection` to call it (keeps its `adapters` set for teardown). Pure refactor — existing transport tests must stay green.

### 2. Virtual connection — `src/core/transports/virtual-connection.ts`
```ts
// A net.Socket-compatible Duplex handed to a client lib as its socket.
class VirtualClientSocket extends Duplex { /* + no-op setNoDelay/setKeepAlive/setTimeout/ref/unref, remoteAddress/remotePort getters */ }

// Server-side transport over a Duplex (read = async-iterate, write = push, signal via AbortController on close/error).
class DuplexConnectionTransport implements ConnectionTransport { ... }

export function createVirtualConnection(opts: {
  state: RedisServerState; executor: CommandExecutor;
  nodeRole?: RedisClusterNodeRole; logger?: Pick<Logger,'error'>;
}): { clientSocket: VirtualClientSocket; close(): void }
```
- Cross-wire two ends: client writes → server `read()`; server `write()` → client `'data'`.
- `clientSocket` emits `'connect'` on `process.nextTick` (matches `StandaloneConnector`, which resolves on `nextTick`).
- Internally calls `attachSession(serverTransport, opts)`. `close()`/socket `destroy()` tears down both ends + the session (propagate `'close'` both directions).
- Prefer a small `DuplexConnectionTransport` over forcing `SocketConnectionTransport` (which expects `net.Socket`); confirm during impl whether `SocketConnectionTransport` already works on a plain Duplex and, if so, reuse it instead.

### 3. Node registry — `src/client-mocks/node-registry.ts`
Maps a synthetic `host:port` → node pipeline, so a cluster's per-node connections resolve to the right state/executor.
```ts
type NodePipeline = { state: RedisServerState; executor: CommandExecutor; nodeRole: RedisClusterNodeRole }
class InMemoryNodeRegistry { register(host,port,pipeline); resolve(host,port): NodePipeline | undefined; nodes(): {...}[] }
```
Standalone = a single registered entry. Cluster = one entry per node (synthetic ports, e.g. `7000+i`, advertised by `CLUSTER SLOTS`).

---

## ioredis path — `src/client-mocks/ioredis-mock.ts`

`ioredis` and `redis` become **optional `peerDependencies`** (imported lazily via dynamic `import('ioredis')`); kept as devDeps for tests/types so the core stays dependency-free and browser-safe.

```ts
export type CreateIoredisMockOptions =
  | { cluster?: false; databaseCount?: number }
  | { cluster: { masters: number; replicasPerMaster?: number } }

export async function createIoredisMock(opts?): Promise<Redis | Cluster>
```

### Standalone
1. `createStandalonePipeline(databaseCount)` (reuse from [src/mock.ts](src/mock.ts)) → `{ state, executor }`.
2. Define an `InMemoryConnector extends AbstractConnector`; `connect()` returns `createVirtualConnection({ state, executor }).clientSocket`.
3. `new Redis({ Connector: InMemoryConnector, lazyConnect: false, ... })`. Track created virtual connections for teardown; attach a `close()`/`quit()` wrapper that tears them down.

### Cluster
1. Build the in-memory cluster nodes by reusing `createRedisCluster(...)`'s node-construction logic **without** `listen()` — refactor `createRedisCluster` so node assembly (states, per-node executor with `createClusterPolicy`/`createClusterCommands`, replication links, topology with synthetic ports) is callable without binding TCP (e.g. extract `buildClusterNodes(options)` returning pipelines + topology; `createRedisCluster` keeps wrapping it in `Resp2Server`s). Register each node in `InMemoryNodeRegistry`.
2. `InMemoryConnector.connect()` reads its own `options.host/port` → `registry.resolve(host,port)` → `createVirtualConnection(pipeline)`. (ioredis Cluster creates a per-node `Redis` with the node's host/port in connector options — verified.)
3. `new Redis.Cluster([{ host, port: firstNodePort }], { redisOptions: { Connector: InMemoryConnector } })`. Topology discovery (`CLUSTER SLOTS`) + `MOVED` already work because each node runs the real cluster executor and advertises the synthetic host:ports.

ioredis sends a small handshake on connect (`info`, `select`, cluster `cluster slots`, possibly `client setname/info`); these already exist via the shared executor / introspection commands — any gap will surface in the integration test (low risk).

---

## node-redis path — `src/client-mocks/node-redis-mock.ts`

No socket hook → facade replicating the `createClient` / `createCluster` public interface, routing commands through the in-memory pipeline. Reuse [InMemoryRedisClient](src/in-memory-client.ts)'s `decode()` (`RedisValue → native JS`) for default reply shaping (its map→object / array / integer→number behavior already matches node-redis RESP2 defaults for the common cases).

```ts
export async function createNodeRedisMock(opts?): Promise<NodeRedisMockClient | NodeRedisMockCluster>
```

**Primary (Tier 2 — recommended, predictable):** hand-written facade exposing the high-value surface, backed by a per-connection `ClientSession`:
- Generic `sendCommand(args: (string|Buffer)[])` → execute → `decode` (the escape hatch for any command).
- Curated camelCase command methods for the common set (`get/set/del/exists/expire/ttl/incr/hSet/hGet/hGetAll/lPush/rPush/lRange/sAdd/sMembers/zAdd/zRange/...`) with node-redis-correct return types (small per-command transform table layered over `decode`).
- `multi()` / transactions (`MULTI`/`EXEC`/`DISCARD` via the session), `watch`.
- Pub/sub (`subscribe`/`pSubscribe` + message callbacks) using a dedicated session reading pushes (`session.readPushes`) — a capability the bespoke `InMemoryRedisClient` lacks.
- `duplicate()`, `connect()`/`quit()`/`disconnect()` no-ops, EventEmitter `'connect'`/`'ready'`/`'error'`/`'end'`.
- Document explicitly: uncommon commands fall through to generic `decode` shapes (honest scope; avoids the `ioredis-mock` whack-a-mole trap).

**Cluster:** `NodeRedisMockCluster` computes the slot for a command's keys (reuse `RedisClusterTopology.calculateSlotForKeys`), routes to the owning node's session. Same method surface as standalone.

**Optional stretch (Tier 1.5 — higher fidelity, spike first):** reuse node-redis's own command specs / `transformReply` from `@redis/client` (every generated method routes through `_executeCommand → sendCommand`; `RedisClient.factory` attaches `commands` + transformers). If the command-spec table is importable and `transformReply`'s expected input shape can be produced from our decode, we get node-redis fidelity without sockets. **Risk:** private `#queue`/`#socket`, version-fragile internals. Validate with a throwaway spike before committing; otherwise ship Tier 2.

---

## Files

**New**
- [src/core/transports/attach-session.ts](src/core/transports/attach-session.ts) — extracted session wiring.
- [src/core/transports/virtual-connection.ts](src/core/transports/virtual-connection.ts) — `createVirtualConnection`, `VirtualClientSocket`, `DuplexConnectionTransport`.
- [src/client-mocks/node-registry.ts](src/client-mocks/node-registry.ts) — host:port → pipeline.
- [src/client-mocks/ioredis-mock.ts](src/client-mocks/ioredis-mock.ts) — `createIoredisMock`.
- [src/client-mocks/node-redis-mock.ts](src/client-mocks/node-redis-mock.ts) — `createNodeRedisMock` + facade classes.

**Modified**
- [src/core/transports/resp2/server.ts](src/core/transports/resp2/server.ts) — `handleConnection` delegates to `attachSession`.
- [src/cluster.ts](src/cluster.ts) — extract `buildClusterNodes()` (TCP-free node assembly) reused by both `createRedisCluster` and the in-memory cluster path.
- [src/index.ts](src/index.ts) — export `createIoredisMock`, `createNodeRedisMock` + option/return types.
- [src/internal.ts](src/internal.ts) — export `createVirtualConnection`/`attachSession` for advanced users.
- `package.json` — add `ioredis` + `redis` as optional `peerDependencies` (`peerDependenciesMeta: { optional: true }`); keep in devDeps; ensure tsup still emits the new module dual ESM+CJS.

---

## Testing (node:test + node:assert, integration-first, red before green)

Real `redis-server` is **not** needed — these are client-vs-mock tests (no "real Redis does X" wire claims here; the RESP fidelity is already covered by the existing suite, and ioredis itself is the real client).

- `tests-integration/ioredis/in-memory-standalone.test.ts` — real ioredis over the mock: string/hash/list/zset round-trips, `multi/exec`, `expire`/`ttl`, error wording (`WRONGTYPE`).
- `tests-integration/ioredis/in-memory-cluster.test.ts` — `Redis.Cluster` over in-memory nodes: cross-slot routing, `{tag}` hashing, `MOVED` follow.
- `tests-integration/ioredis/in-memory-pubsub.test.ts` — `subscribe`/`publish` proves push frames flow over the virtual socket.
- `tests/client-mocks/node-redis-mock.test.ts` — facade: common commands, `multi`, pub/sub, `sendCommand` fallback, standalone + cluster slot routing.
- `tests/core/virtual-connection.test.ts` — wire bytes round-trip + teardown (no leaked sessions/sockets after `close()`).
- Write each test red first (assert the new factory exists / behavior), confirm it fails, then implement.

Run: `npm test` and `npm run test:integration:mock`. Lifecycle check: after `close()`, no open handles (sessions/sockets) — assert via the `attachSession` teardown.

---

## Risks / assumptions (ranked)

1. **node-redis facade fidelity (highest).** Hand-replicating reply shapes is a maintenance surface. Mitigation: scope to a curated set + generic `sendCommand`; document fall-through. Spike Tier 1.5 (reuse `transformReply`) only if higher fidelity is required.
2. **Cluster refactor (`buildClusterNodes`)** must not regress the existing TCP cluster. Mitigation: pure extraction; existing cluster tests are the guard.
3. **VirtualClientSocket must satisfy ioredis's stream expectations** (`'connect'` timing, `write`/`drain`, `destroy`, no-op `setNoDelay`/`setTimeout`). Mitigation: integration test with the real client is the ground truth.
4. **ioredis connection handshake** may hit an unimplemented introspection command. Mitigation: surfaces in the standalone test; add the command if missing (low risk — INFO/CLIENT/SELECT exist).
