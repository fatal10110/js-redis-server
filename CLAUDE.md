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

### Core Architecture Pattern

This is a Redis-compatible server implementation that supports both standalone and cluster modes. The architecture uses a **commander pattern** where commands are executed through a pipeline:

1. **Transport Layer** - Handles RESP protocol encoding/decoding
2. **Commander Layer** - Routes commands and manages execution context
3. **DB Layer** - Manages in-memory data structures
4. **Command Layer** - Individual command implementations

### Key Components

#### 1. DB ([src/commanders/custom/db.ts](src/commanders/custom/db.ts))

- Central in-memory data store using three Maps:
  - `mapping`: string → Buffer (key lookup)
  - `data`: Buffer → DataTypes (actual data)
  - `timings`: Buffer → number (expiration timestamps)
  - `scriptsStore`: string → Buffer (Lua scripts by SHA)
- Handles key expiration via lazy eviction in `tryEvict()`
- All data operations go through this class

#### 2. Commander Types

**Commander** ([src/commanders/custom/commander.ts](src/commanders/custom/commander.ts))

- Standalone Redis mode
- Simple command execution without clustering logic
- Handles MULTI/EXEC transactions via `TransactionCommand`
- Creates commands via `createCommands()` factory

**ClusterCommander** ([src/commanders/custom/clusterCommander.ts](src/commanders/custom/clusterCommander.ts))

- Cluster mode implementation
- Validates slot ownership before executing commands
- Throws `MovedError` to redirect clients to correct nodes
- Uses `cluster-key-slot` to determine which node owns a key
- Handles cross-slot validation (throws `CorssSlot` error)
- Creates commands via `createClusterCommands()` factory

#### 3. Command Architecture

Commands are organized by Redis data type:

- **Strings**: [src/commanders/custom/commands/redis/data/strings/](src/commanders/custom/commands/redis/data/strings/)
- **Hashes**: [src/commanders/custom/commands/redis/data/hashes/](src/commanders/custom/commands/redis/data/hashes/)
- **Lists**: [src/commanders/custom/commands/redis/data/lists/](src/commanders/custom/commands/redis/data/lists/)
- **Sets**: [src/commanders/custom/commands/redis/data/sets/](src/commanders/custom/commands/redis/data/sets/)
- **Sorted Sets**: [src/commanders/custom/commands/redis/data/zsets/](src/commanders/custom/commands/redis/data/zsets/)
- **Keys**: [src/commanders/custom/commands/redis/data/keys/](src/commanders/custom/commands/redis/data/keys/)

Each command implements the `Command` interface:

```typescript
interface Command {
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] // Extract keys for cluster routing
  run(
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<CommandResult>
}
```

#### 4. Data Structures

Custom implementations in [src/commanders/custom/data-structures/](src/commanders/custom/data-structures/):

- `StringDataType` - String values
- `HashDataType` - Hash maps
- `ListDataType` - Doubly-linked lists
- `SetDataType` - Set implementation
- `SortedSetDataType` - Sorted sets with scores
- `StreamDataType` - Stream data structure (future)

#### 5. Transaction Support (MULTI/EXEC)

- Implemented via `TransactionCommand` class
- Commands are queued during MULTI state
- Executed atomically on EXEC
- DISCARD cancels transaction
- Cluster mode validates slots during transaction execution

#### 6. Dual Backend System

The test suite supports two backends via `TEST_BACKEND` environment variable:

- `mock`: Uses `ioredis-mock` (fast, no external dependencies)
- `real`: Uses actual Redis cluster (validates real-world compatibility)

Integration tests live in [tests-integration/](tests-integration/) with subdirectories for `ioredis/` and `node-redis/` clients.

### Type System

Core types in [src/types.ts](src/types.ts):

- `DBCommandExecutor` - Interface for command execution
- `Command` - Individual command interface
- `CommandResult` - Response with optional close flag
- `Transport` - RESP protocol writer
- `DiscoveryService` - Cluster node discovery
- `ClusterCommanderFactory` - Creates commanders for cluster nodes

### Error Handling

Custom errors in [src/core/errors.ts](src/core/errors.ts):

- `UserFacedError` - Base class for client-visible errors
- `UnknownCommand` - Command not found
- `MovedError` - Cluster redirect (MOVED response)
- `CorssSlot` - Cross-slot operation in cluster mode
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
- Avoid array methods in performance-critical code paths

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

1. Minimize object allocations in hot paths
2. Avoid unnecessary string conversions
3. Use efficient data structures
4. Cache frequently accessed values
5. Break early when possible

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

1. Create command file in appropriate directory (e.g., `src/commanders/custom/commands/redis/data/strings/mycommand.ts`)
2. Implement `Command` interface with `getKeys()` and `run()` methods
3. Add to command factory in [src/commanders/custom/commands/redis/index.ts](src/commanders/custom/commands/redis/index.ts)
4. Add tests in [tests/](tests/) directory
5. Consider adding to filtered command sets:
   - `createReadonlyCommands()` - Safe for replicas
   - `createMultiCommands()` - Allowed in transactions

## Cluster Slot Routing

When implementing commands that operate on keys:

1. Implement `getKeys()` to extract all keys from arguments
2. `ClusterCommander` uses `cluster-key-slot` to determine slot ownership
3. If command accesses multiple keys, all must hash to same slot (or throw `CorssSlot`)
4. If slot is not owned by current node, throw `MovedError` with correct node info

## Transaction Execution Context

Recent architecture changes introduced execution contexts:

- `ExecutionContext` interface for stateful command execution
- `TransactionExecutionContext` for MULTI/EXEC blocks
- Each context can transition to another context based on commands
- Allows for cleaner separation of transaction vs normal execution mode
