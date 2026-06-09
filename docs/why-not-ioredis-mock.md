# Why I built a real in-memory Redis server instead of mocking the client

If you've worked on a Node.js project that uses Redis, you've probably reached
for [`ioredis-mock`](https://github.com/stipsan/ioredis-mock) at some point —
or wished a `node-redis` equivalent existed (it [doesn't, and people keep
asking](https://github.com/redis/node-redis/issues)). Both projects sit on the
same fault line: they fake the **client API**, not the **protocol**. That
choice causes most of the pain people run into.

`js-redis-server` takes the other path: it's a real RESP2/RESP3 server that
runs in-memory, in-process, with zero external dependencies. Your client talks
to it exactly like it would talk to real Redis — because as far as the wire is
concerned, it *is* Redis.

## The problem with mocking the client

`ioredis-mock` re-implements `ioredis`'s JS-facing API surface: same method
names, same return shapes, in-memory storage underneath. That's a reasonable
bet when it works, but it means:

- **You're locked to one client.** A mock built against `ioredis`'s internals
  is useless if your code (or a library you depend on) uses `node-redis`,
  `redis`, or raw `net` sockets. Hence the recurring "is there a node-redis
  mock?" threads — there's no shortcut, someone has to write a second whole
  mock from scratch.
- **Edge cases drift from real Redis.** Error message formats, type-mismatch
  errors (`WRONGTYPE`), expiry semantics, `RESP3` push types — every one of
  these has to be hand-replicated in the mock's command implementations, and
  `ioredis-mock`'s open issues are full of "real Redis returns X, this returns
  Y" reports that pile up faster than they get fixed.
- **No cluster, no Lua, no MULTI/EXEC semantics that match the wire.** These
  aren't small features to bolt onto a client-side mock — they're protocol-
  and topology-level behaviors that only make sense when something is actually
  speaking RESP and routing by slot.
- **Maintenance burden scales with the client's API**, not with Redis's
  protocol. Every new `ioredis` method needs a matching mock method, forever.

## What "speak the protocol, not the client" buys you

`js-redis-server` is a TCP server that implements RESP2 and RESP3 directly.
Practically, that means:

- **Client-agnostic** — works with `ioredis`, `node-redis`, `redis`, or
  anything else that opens a socket and speaks RESP. Switch clients, switch
  nothing else.
- **Standalone *and* cluster mode** — `buildRedisCluster()` spins up a real
  multi-node topology with slot routing, `MOVED`/`ASK` redirects, and
  cross-slot validation, so you can test cluster-aware code paths without
  Docker or a real cluster.
- **Real Lua scripting** — `EVAL`/`EVALSHA` run actual Lua via WebAssembly
  ([`lua-redis-wasm`](https://www.npmjs.com/package/lua-redis-wasm)), so
  scripts behave like they would against real Redis, not like a JS
  reimplementation of a Lua interpreter.
- **Per-session RESP2/RESP3 negotiation** via `HELLO`, matching how real Redis
  and real clients negotiate protocol versions.
- **Errors that match the wire format** — `WRONGTYPE`, `MOVED`, `CROSSSLOT`,
  etc. come from the same code paths real Redis uses to generate them, because
  the server is generating real RESP error replies, not JS exceptions shaped
  to look like them.
- **Zero external dependencies, in-process** — no Docker, no real Redis
  install, spins up and tears down in milliseconds inside your test suite.

## Who this is for

- Anyone currently fighting `ioredis-mock` edge-case mismatches
- `node-redis` users who have nothing to reach for today
- Anyone testing cluster-mode code, Lua scripts, or RESP3-specific behavior
  that client-side mocks structurally can't reproduce
- Library authors who want client-agnostic Redis test coverage instead of
  coupling their tests to one client's mock

## Try it

```bash
npm install js-redis-server
```

```typescript
import {
  RedisServerState,
  createRedisCommandExecutor,
  Resp2Server,
} from 'js-redis-server'

const state = new RedisServerState()
const executor = createRedisCommandExecutor()
const server = new Resp2Server({ server: state, executor, logger: console })

await server.listen(6379)
// point ioredis, node-redis, or redis-cli at it — it's just Redis on the wire
```

Repo: https://github.com/fatal10110/js-redis-server

Feedback, command coverage gaps, and PRs welcome — it's still early, and the
fastest way to find what's missing is people trying it against their real test
suites.
