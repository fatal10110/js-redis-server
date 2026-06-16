# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing

```bash
# Run unit tests (all tests in tests/ directory)
npm test

# Run integration tests with mock backend (uses ioredis-mock)
npm run test:integration:mock

# Run integration tests with real Redis backend (requires Redis cluster)
npm run test:integration:real

# Run all tests (unit + both integration test suites)
npm run test:all

# Run a single test file
node --enable-source-maps --import tsx --no-warnings --test ./tests/path/to/test.test.ts

# Run integration tests sequentially (needed for real Redis backend)
TEST_BACKEND=real node --enable-source-maps --import tsx --no-warnings --test-concurrency 1 --test ./tests-integration/**/*.test.ts
```

### Building & Running

```bash
# Build TypeScript to JavaScript
npm run build

# Start the server (builds first)
npm start

# Lint code
npm run lint

# Format code with Prettier
npm run format

# Clean Redis data (useful between integration test runs)
npm run clean:redis
```

## Architecture Overview

For full diagrams and a request-lifecycle walkthrough, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — keep it in sync with any structural change.

### Core Architecture Pattern

Redis-compatible server (standalone + cluster modes) built as a layered pipeline. The **same** `CommandExecutor` pipeline drives standalone mode, cluster mode, `MULTI`/`EXEC` transactions, and Lua `EVAL` alike, so routing, queueing, and command semantics never diverge:

1. **Transport Layer** - Frames RESP bytes on/off the wire (`SocketConnectionTransport` / `InMemoryConnectionTransport`)
2. **Session Layer** - Per-connection state: selected DB, RESP version, transaction queue, `WATCH`ed keys (`ClientSession`)
3. **Execution Layer** - Looks up commands, parses args, extracts routing keys, runs composable policies (`CommandExecutor`, `CommandRegistry`, `ExecutionPolicy`)
4. **Command Layer** - Pure `(args, ctx) → RedisResult` implementations grouped by data type ([src/commands/](src/commands/))
5. **State Layer** - In-memory keyspace, mutation events, cluster topology, script cache, pub/sub ([src/state/](src/state/))

### Key Components

#### 1. RedisServerState & RedisDatabase ([src/state/server-state.ts](src/state/server-state.ts), [src/state/database.ts](src/state/database.ts))

- `RedisServerState` owns one or more `RedisDatabase` instances plus server-wide state: cluster topology, Lua script cache, pub/sub broker
- Each `RedisDatabase` wraps a `RedisKeyspace` ([src/state/keyspace.ts](src/state/keyspace.ts)): a `Map<keyId, KeyspaceEntry>` of byte-safe `Buffer` keys → typed `RedisDataValue`s with an optional `expiresAt`
- Expiration is lazy — `getLiveEntry` evicts expired keys on read and emits an `evict` mutation event so `WATCH` sees expiry like a real delete
- Every mutation flows through `RedisMutationBus` ([src/state/mutation-events.ts](src/state/mutation-events.ts)), which clones values before fan-out (drives `WATCH` today, keyspace notifications later)
- `FLUSHALL`/`FLUSHDB` clear keyspace data but **not** the script cache — only `SCRIPT FLUSH` does

#### 2. CommandExecutor & ExecutionPolicy ([src/core/command-executor.ts](src/core/command-executor.ts), [src/core/execution-policies/](src/core/execution-policies/))

- `CommandExecutor.plan()` resolves a `CommandDefinition` from the `CommandRegistry`, parses raw `Buffer` args through the command's `schema`, and extracts routing keys via `definition.keys(args)` — producing a shared `CommandPlan`
- `executePlan` is the normal async path (supports `ResponseStream` + `afterExecute`/`onStream` rewriting); `executePlanSync` is a synchronous path used only by the Lua runtime for `redis.call`/`redis.pcall` — same registry/policies, and rejects anything that tries to go async or stream
- An `ExecutionPolicy` wraps every command with optional `beforeExecute` (can short-circuit: queue/redirect/reject), `afterExecute` (rewrite the result), and `onStream` (wrap a streaming result) hooks
- `TransactionPolicy` ([src/core/execution-policies/transaction-policy.ts](src/core/execution-policies/transaction-policy.ts)) is always appended last; `ClusterPolicy` ([src/core/execution-policies/cluster-policy.ts](src/core/execution-policies/cluster-policy.ts)) is prepended only for cluster nodes — order matters because cluster routing must validate (and possibly redirect/reject) **before** a command is queued into a transaction
- There is no separate "cluster commander" type — cluster mode is the same `Resp2Server` + `CommandExecutor`, configured with one extra `CLUSTER` command and a `ClusterPolicy` bound to that node's id ([src/cluster.ts](src/cluster.ts))

#### 3. Command Architecture ([src/commands/](src/commands/))

Commands are grouped by Redis data type/concern: `strings`, `hashes`, `lists`, `sets`, `zsets`, `keys`, `scan`, `scripts`, `transactions`, `connection`, `cluster`, `introspection`. Each is a `CommandDefinition` ([src/core/command-definition.ts](src/core/command-definition.ts)) built via `defineCommand`:

```typescript
interface CommandDefinition<TArgs> {
  readonly name: string
  readonly schema: CommandSchema<TArgs> // arity/syntax — single source of truth, parsed via `t`
  readonly flags: readonly CommandFlag[] // 'readonly' | 'write' | 'transaction' | 'noscript' | ...
  keys(args: TArgs): readonly Buffer[] // routing keys for cluster slot calculation
  execute(
    args: TArgs,
    ctx: RedisExecutionContext,
  ): RedisResult | Promise<RedisResult> | ResponseStream
}
```

Commands are pure `(args, ctx) → RedisResult` — they never touch the transport. That is what lets the *exact same* command run standalone, inside a cluster node, inside `MULTI`/`EXEC`, and inside a Lua script without rewrites.

#### 4. Data Structures ([src/state/data-types.ts](src/state/data-types.ts))

`RedisDataValue` is a typed union: `string`, `hash`, `list`, `set`, `zset`, `stream` (stream values store entries, generated/deleted ID metadata, consumer groups, pending-entry lists, and consumer idle metadata).

#### 5. Transaction Support (MULTI/EXEC/WATCH)

- `ClientSession` ([src/core/client-session.ts](src/core/client-session.ts)) tracks transaction mode, queues already-parsed `CommandPlan`s, and replays them through the normal `executePlan` path on `EXEC`
- `TransactionPolicy` intercepts queued commands in `beforeExecute` and replies `+QUEUED` — parsing and key-extraction (and therefore early `CROSSSLOT`/`MOVED` errors) happen at **queue time**, not at `EXEC` time
- `WATCH` subscribes to per-key mutation events on the database; any write/delete/evict on a watched key marks the session dirty, checked via `isWatchDirty()` before `EXEC` runs the queue
- In cluster mode, the slot of the *first* keyed command queued is pinned per-session so every subsequent queued command must hash to the same slot
- `DISCARD` cancels a transaction; `EXECABORT` is returned if the queue itself is dirty (e.g. an unknown command was queued)

#### 6. Dual Backend System

The integration test suite supports two backends via `TEST_BACKEND` (see [tests-integration/test-config.ts](tests-integration/test-config.ts)):

- `mock` (default): spins up an in-process mock cluster via `buildRedisCluster` — fast, no external dependencies
- `real`: uses an actual Redis cluster (validates real-world compatibility)

Integration tests live in [tests-integration/](tests-integration/) with subdirectories for `ioredis/` and `node-redis/` clients.

### Concurrency Model

Each `RedisDatabase` owns a `SerialTurnQueue` ([src/core/turn-queue.ts](src/core/turn-queue.ts)). Every `session.execute()` waits for a turn before reaching the executor, so commands within one database run to completion one at a time — mirroring single-threaded Redis semantics (sessions on different databases run independently). `RedisExecutionContext` carries a `park` handler ([src/core/redis-context.ts](src/core/redis-context.ts)) so a command can release its turn while waiting on something and re-acquire one with priority once it resolves — plumbing for future blocking commands (`BLPOP`, `WAIT`, `XREAD BLOCK`, ...); no shipped command uses it yet.

### Type System

Core protocol/result types live in [src/core/redis-value.ts](src/core/redis-value.ts), [src/core/redis-result.ts](src/core/redis-result.ts), and [src/core/response-stream.ts](src/core/response-stream.ts):

- `RedisValue` - protocol-agnostic reply union (`simple-string`, `bulk-string`, `integer`, `array`, `map`, `error`, ...), encoded to RESP2/RESP3 wire bytes by [src/core/resp-encoder.ts](src/core/resp-encoder.ts)
- `RedisResult` - command outcome wrapper returned by `execute()`
- `ResponseStream` - streaming/push-style replies
- `CommandDefinition` / `CommandPlan` / `CommandSchema` - command shape, parsed invocation, and arg-parsing ([src/core/command-definition.ts](src/core/command-definition.ts), [src/core/command-schema.ts](src/core/command-schema.ts))
- `RedisExecutionContext` - per-call context (`db`, `server`, `session`, `executor`, `signal`, `park`) ([src/core/redis-context.ts](src/core/redis-context.ts))

### Error Handling

Custom errors in [src/core/redis-error.ts](src/core/redis-error.ts), all extending `RedisCommandError`:

- `UnknownRedisCommandError` - command not found
- `RedisMovedError` - cluster redirect (`-MOVED` response)
- `RedisCrossSlotError` - cross-slot operation in cluster mode (`-CROSSSLOT`)
- `RedisClusterDownError` - slot unassigned (`-CLUSTERDOWN`)
- `WrongTypeRedisError`, `WrongNumberOfArgumentsError`, `RedisSyntaxError`, `NoScriptError`, `ExecWithoutMultiError`, ... - command-specific client-visible errors
- Error responses follow RESP protocol format

## Code Style & Conventions

### Early Returns and Linear Flow

Use early returns to avoid nested conditions. Keep code flow linear and predictable:

```typescript
// Good
if (!option) {
  if (inVariadicSection) {
    variadicArgs.push(args[i].toString())
    continue
  }
  // ...
}

// Bad - nested logic
if (option) {
  if (option.type === 'flag') {
    // nested logic
  }
}
```

### Loop Patterns

- Prefer `for...of` with `Object.entries()` over `for...in`
- Use traditional `for` loops for index-based iteration

```typescript
// Good
for (const [, option] of Object.entries(this.optionTable)) {
  // ...
}

// Good for index-based
for (let i = 0; i < args.length; i++) {
  // ...
}
```

### Performance Considerations

Performance is **not** a priority — this is a mocking library for tests, not a production datastore. Prefer correctness, Redis compatibility, and code clarity over micro-optimizations. Defensive cloning, extra allocations, and readable-but-slower constructs are all acceptable. Only treat performance as a problem if it makes test suites impractically slow.

## Testing Requirements

### Test Style (Mandatory)

All tests MUST use Node.js built-in test runner and assertions:

```typescript
import { test, describe } from 'node:test'
import assert from 'node:assert'

describe('MyFeature', () => {
  test('should do something', () => {
    assert.strictEqual(1 + 1, 2)
  })

  test('should throw error', () => {
    assert.throws(() => {
      throw new Error('fail')
    }, Error)
  })
})
```

**DO NOT use:**

- Jest (`it()`, `expect()`, `toBe()`, `toEqual()`)
- Mocha
- Chai or other assertion libraries

**DO use:**

- `test()` for individual test cases
- `describe()` for grouping tests
- `assert.strictEqual()` for strict equality
- `assert.deepStrictEqual()` for deep equality
- `assert.throws()` for error assertions

### Test Organization

- Unit tests: [tests/](tests/) directory
- Integration tests: [tests-integration/](tests-integration/) directory
  - `ioredis/` - Tests using ioredis client
  - `node-redis/` - Tests using node-redis client

## Adding New Commands

1. Implement a `CommandDefinition` — `name`, `schema` (via `t` from [src/core/command-schema.ts](src/core/command-schema.ts)), `flags`, `keys(args)`, and `execute(args, ctx)` — using `defineCommand` ([src/core/command-definition.ts](src/core/command-definition.ts)) in the matching [src/commands/<type>.ts](src/commands/) file
2. Register it in [src/commands/index.ts](src/commands/index.ts) (and re-export it if other modules need direct access)
3. Add unit tests in [tests/](tests/); add integration coverage under [tests-integration/](tests-integration/) if it has client-visible wire behavior worth checking against a real client
4. Set `flags` correctly — `'readonly'` marks it safe for replicas, `'noscript'` excludes it from Lua, `'transaction'` controls MULTI/EXEC eligibility

Because commands are pure `(args, ctx) → RedisResult` functions that never touch the transport, a correctly-implemented command automatically works standalone, in a cluster, inside `MULTI`/`EXEC`, and inside Lua — no special-casing needed.

## Cluster Slot Routing

When implementing commands that operate on keys:

1. Implement `keys(args)` to extract all routing keys from the parsed arguments
2. `ClusterPolicy` computes a slot for those keys via `RedisClusterTopology.calculateSlotForKeys` ([src/state/cluster-topology.ts](src/state/cluster-topology.ts))
3. If the keys span ≥2 slots, it throws `RedisCrossSlotError` (`-CROSSSLOT`)
4. If the slot is owned by another master, it throws `RedisMovedError` (`-MOVED (slot) (host):(port)`); if the slot is unassigned, `RedisClusterDownError` (`-CLUSTERDOWN`)
5. Replicas never "own" a slot for routing — a keyed command sent directly to a replica is redirected to its master


## grepai - Semantic Code Search

**IMPORTANT: Use grepai as the primary tool for code exploration and semantic search.**

### When to Use grepai (REQUIRED)

Use `grepai search` instead of `grep`, `glob`, or `find` for:
- Understanding what code does or where functionality lives
- Finding implementations by intent (e.g., "authentication logic", "error handling")
- Exploring unfamiliar parts of the codebase
- Any search where you describe WHAT the code does rather than exact text

### When to Use Standard Tools

Use standard text tools only when you need:
- Exact text matching (variable names, imports, specific strings)
- File path patterns (e.g., `**/*.go`)

### Fallback

If grepai is unavailable, the index is empty, or the command errors, fall back to standard text search.

### Setup / Health Check

Before relying on grepai for exploration:
1. Run `grepai status --no-ui`
2. If the index is empty or stale, initialize or refresh it with `grepai init --yes`
3. Make sure the watcher is running with `grepai watch`

Current repo state: grepai is installed, the watcher is running, and the index should be checked before semantic search if results look incomplete.

### Usage

```bash
grepai search "user authentication flow" --json --compact --limit 10
grepai search "error handling middleware" --json --compact
grepai search "database connection pool" --json --compact
grepai search "API request validation" --json --compact
```

### Query Tips

- Use English queries for better semantic matching.
- Describe intent, not implementation: "handles user login" instead of "func Login".
- Be specific: "JWT token validation" is better than "token".
- Prefer `--json` for machine-readable output and `--compact` when you only need file paths and scores.

### Call Graph Tracing

Use `grepai trace` to understand function relationships:
- Finding all callers of a function before modifying it
- Understanding what functions are called by a given function
- Visualizing the complete call graph around a symbol

#### Trace Commands

Use `--json` when you want machine-readable output.

```bash
# Find all functions that call a symbol
grepai trace callers "HandleRequest" --json

# Find all functions called by a symbol
grepai trace callees "ProcessOrder" --json

# Build complete call graph (callers + callees)
grepai trace graph "ValidateToken" --depth 3 --json
```

### Workflow

1. Start with `grepai search` to find relevant code by intent.
2. Use `grepai trace` to understand callers, callees, and the surrounding call graph.
3. Open the returned files to inspect the implementation details.
4. Use exact-text search only when you already know the literal symbol or string you need.

## OpenMemory - Durable Agent Memory

OpenMemory MCP is wired into this project as the persistent memory layer for stable context that should survive across turns and tasks. Server: `http://localhost:8080`. Always use `openmemory_*` MCP tools directly — do not invent a custom persistence layer.

Do **not** pass a `user_id` (or any tenant/owner argument) to `openmemory_*` tools. The tenant is resolved automatically from the configured API key — overriding it routes memories to the wrong tenant.

### MCP Tools Available

- `openmemory_store` — store a memory (text only; tenant comes from the API key)
- `openmemory_query` — semantic/keyword search
- `openmemory_list` — list recent memories
- `openmemory_get` — fetch by ID
- `openmemory_delete` — remove by ID
- `openmemory_reinforce` — boost salience of a recalled memory

### When to Use OpenMemory

Store:

- architectural decisions and the reasoning behind them
- non-obvious bug root causes and fix patterns
- recurring project conventions and constraints
- durable user/project preferences and long-lived task context
- summaries of completed phases/features

Do not store:

- transient debug notes or raw command output
- speculative ideas likely to be discarded
- secrets or credentials

### Session Protocol

1. **Session start**: query OpenMemory for context relevant to the current task before asking the user to repeat anything.
2. **After decisions**: store architectural choices, design constraints, and non-obvious patterns.
3. **After bug fixes**: store root cause + fix pattern if non-obvious.
4. Keep entries short, factual, and durable — outcome/rule, not full discussion.

### Practical Rules

- Always use this OpenMemory integration instead of inventing a custom persistence layer.
- If OpenMemory is unavailable, continue without it — do not block the task.
- If the server is down, start it: `cd ~/workspace/OpenMemory && colima start && docker-compose up -d`
