# js-redis-server

[![CI](https://github.com/fatal10110/js-redis-server/actions/workflows/ci.yml/badge.svg)](https://github.com/fatal10110/js-redis-server/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/js-redis-server.svg)](https://www.npmjs.com/package/js-redis-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**An in-memory Redis-compatible server implemented in JavaScript/TypeScript.**
Run it from the CLI, start a TCP server from code, or use a socketless mock.
No native Redis binary or Docker required. Lua scripting runs via WebAssembly.

[Try the browser demo](https://fatal10110.github.io/js-redis-server/) ·
[Supported commands](https://github.com/fatal10110/js-redis-server/blob/main/docs/COMMANDS.md) · [Testing guide](https://github.com/fatal10110/js-redis-server/blob/main/docs/TESTING.md) ·
[Server API](https://github.com/fatal10110/js-redis-server/blob/main/docs/API.md)

## Choose how to run it

| Mode                                                      | Entry point                                 | Real TCP? |
| --------------------------------------------------------- | ------------------------------------------- | --------- |
| CLI server for local development                          | `npx js-redis-server`                       | Yes       |
| Server started from code / isolated test server           | `createRedisServer()` / `createRedisMock()` | Yes       |
| Socketless mock backed by the same command implementation | `createIoredisMock()`                       | No        |

TCP mode lets unmodified ioredis or node-redis clients connect normally.
Socketless mode runs entirely in-process; it does not open a listening port
or make a real TCP connection.

## Install

Requires **Node.js 22+**. Examples below use ESM: save JavaScript snippets as
`.mjs` files and run them with `node`.

```bash
npm install --save-dev js-redis-server
```

For the ioredis examples, also install its supported major version:

```bash
npm install --save-dev ioredis@5
```

The upcoming `0.3.0` release adds `js-valkey-server` as an alternative package
name and `createValkeyMock` as an alias of `createRedisMock`. Publication is
pending; use `js-redis-server` for now. The existing package and GitHub repository
are not being renamed or deprecated.

## 1. Run from the CLI

```bash
npx js-redis-server --port 6380
```

Connect your application to `redis://127.0.0.1:6380`; press Ctrl+C to stop.
The default port is 6379. For a cluster or the full option list:

```bash
npx js-redis-server --cluster --masters 3 --base-port 30000
npx js-redis-server --help
```

[CLI options](https://github.com/fatal10110/js-redis-server/blob/main/docs/API.md#cli)

## 2. Start a TCP server from code

`createRedisServer()` starts listening immediately. This example stays running
until Ctrl+C so another process can connect:

```javascript
import { once } from 'node:events'
import { createRedisServer } from 'js-redis-server'

const server = await createRedisServer({ port: 6380 })
try {
  console.log(`Connect to redis://${server.host}:${server.port}`)
  await once(process, 'SIGINT')
} finally {
  await server.close()
}
```

Omit `port` to choose an available port. Call `await server.close()` when your
application is done. [Server and cluster API](https://github.com/fatal10110/js-redis-server/blob/main/docs/API.md)

### Use a TCP server in tests

`createRedisMock()` adds a connection URL, seeding, and reset helpers around
the listening server. **It still uses real TCP.** Here is a complete
`node:test` example; save it as `redis.test.mjs` and run
`node --test redis.test.mjs`:

```javascript
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Redis } from 'ioredis'
import { createRedisMock } from 'js-redis-server'

test('stores a value', async t => {
  const mock = await createRedisMock()
  let client
  t.after(async () => {
    client?.disconnect()
    await mock.close()
  })
  client = new Redis(mock.url)

  await client.set('greeting', 'hello')
  assert.equal(await client.get('greeting'), 'hello')
})
```

Use a fresh mock per test, or `await mock.flush()` between tests. Close all
clients before closing their server. For apps that connect at import time,
supply the mock URL before importing the app.

[node-redis, Vitest/Jest, seeding and options](https://github.com/fatal10110/js-redis-server/blob/main/docs/TESTING.md)

## 3. Mock without real TCP

`createIoredisMock()` connects a real ioredis client to the in-memory command
pipeline through a virtual socket. No Redis process, listening port, or real
TCP connection is involved:

```javascript
import { createIoredisMock } from 'js-redis-server'

const client = await createIoredisMock()
try {
  await client.set('greeting', 'hello')
  console.log(await client.get('greeting')) // hello
} finally {
  await client.quit()
}
```

This helper is **experimental**: it exercises ioredis' RESP handling but skips
the operating-system network path. It is not a guaranteed drop-in replacement
for every `ioredis-mock` test or every Redis behavior.

Also available: `createNodeRedisMock()` provides a node-redis-shaped facade
(not the real node-redis client); `createInMemoryClient()` exposes a
client-independent `command()` API with native JavaScript replies and no RESP
encoding. [Socketless APIs and examples](https://github.com/fatal10110/js-redis-server/blob/main/docs/TESTING.md#experimental-socketless-client-mocks)

## Capabilities and limits

- Standalone and cluster modes; RESP2 and RESP3; Lua scripting via WebAssembly.
- Strings, hashes, lists, sets, sorted sets, streams, transactions and pub/sub:
  check [command coverage](https://github.com/fatal10110/js-redis-server/blob/main/docs/COMMANDS.md) for specific operations.
- Redis/Valkey compatibility profiles gate implemented behavior; the default
  is `redis-8.0`. [Profiles and differences](https://github.com/fatal10110/js-redis-server/blob/main/docs/API.md#compatibility-profiles)
- TypeScript definitions and both ESM/CommonJS exports are included.
- This is a separate in-memory implementation, **not the native Redis/Valkey
  engine or a production datastore**. Keep real-server tests for compatibility,
  persistence, failover, timing and failure behavior.
- Unlike an API-only client mock, TCP mode exercises the real client and wire
  protocol. Unlike a binary-backed test server, it does not run genuine Redis.

## Documentation and contributing

- [Testing guide](https://github.com/fatal10110/js-redis-server/blob/main/docs/TESTING.md) — clients, test frameworks, seeding, reset and socketless APIs
- [Server API and CLI](https://github.com/fatal10110/js-redis-server/blob/main/docs/API.md) — standalone, cluster, profiles and low-level building blocks
- [Supported commands](https://github.com/fatal10110/js-redis-server/blob/main/docs/COMMANDS.md) · [Architecture](https://github.com/fatal10110/js-redis-server/blob/main/docs/ARCHITECTURE.md)
- [Contributing](https://github.com/fatal10110/js-redis-server/blob/main/CONTRIBUTING.md) · [Integration test setup](https://github.com/fatal10110/js-redis-server/blob/main/docs/TEST-INTEGRATION.md)

For repository development, use **Node.js 24**, as CI does. Run `npm ci`,
`npm run build`, `npm test` and `npm run lint`. Runtime consumers require Node.js
22 or newer.

MIT — see [LICENSE](LICENSE).
