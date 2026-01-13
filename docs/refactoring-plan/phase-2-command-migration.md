# Phase 2: Migrate Commands to Metadata System

## Objective

Convert all 85 existing commands to use the new metadata-based registration system created in Phase 1.

## Scope

Migrate commands in the following categories:
- **Strings**: 13 commands
- **Keys**: 10 commands
- **Hashes**: 12 commands
- **Lists**: 9 commands
- **Sets**: 11 commands
- **Sorted Sets**: 11 commands
- **Scripts**: 6 commands
- **Server**: 6 commands
- **Cluster**: 4 commands
- **Transactions**: 3 commands

**Total**: ~85 commands

## Migration Strategy

### Incremental Approach

Commands can be migrated incrementally without breaking existing functionality. The migration process:

1. Convert command to export `CommandDefinition` instead of factory function
2. Add metadata using `defineCommand()` helper
3. Add `metadata` property to command class
4. Update imports in category index files
5. Test the converted command

Commands not yet migrated will continue using the old pattern until converted.

## Migration Pattern

### Step-by-Step Conversion

For each command file:

#### 1. Import metadata utilities

```typescript
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'
```

#### 2. Define metadata constant

```typescript
export const XxxCommand: CommandDefinition = {
  metadata: defineCommand('commandname', {
    arity: /* ... */,
    flags: { /* ... */ },
    firstKey: /* ... */,
    lastKey: /* ... */,
    keyStep: /* ... */,
    categories: [/* ... */],
  }),
  factory: (deps) => new XxxClass(deps.db /* ... */),
}
```

#### 3. Add metadata property to class

```typescript
class XxxClass implements Command {
  readonly metadata = XxxCommand.metadata  // ADD THIS

  constructor(/* ... */) {}

  // ... rest of implementation
}
```

#### 4. Update error messages

Replace hardcoded command names with `this.metadata.name`:

```typescript
// Before
throw new WrongNumberOfArguments('get')

// After
throw new WrongNumberOfArguments(this.metadata.name)
```

## Command Metadata Reference

### Arity Calculation

```typescript
// Fixed argument count
// GET key → arity: 2 (command + 1 arg)
arity: 2

// Variable arguments (minimum)
// MGET key [key ...] → arity: -2 (at least command + 1 arg)
arity: -2

// MSET key value [key value ...] → arity: -3 (command + min 2 args)
arity: -3
```

### Flag Guidelines

```typescript
flags: {
  // Read-only (safe for replicas)
  readonly: true,  // GET, HGET, LRANGE, etc.

  // Writes data
  write: true,  // SET, HSET, LPUSH, etc.

  // Fast O(1) operations
  fast: true,  // GET, HGET, LPUSH, RPUSH

  // Random/non-deterministic
  random: true,  // RANDOMKEY, SPOP, SRANDMEMBER

  // Not allowed in Lua
  noscript: true,  // EVAL, SCRIPT, MULTI, EXEC

  // Transaction commands
  transaction: true,  // MULTI, EXEC, DISCARD

  // Admin commands
  admin: true,  // FLUSHDB, FLUSHALL, CONFIG

  // Blocking operations
  blocking: true,  // BLPOP, BRPOP (future)
}
```

### Key Position Guidelines

```typescript
// Single key in first argument
// GET key, SET key value, HGET key field
firstKey: 0,
lastKey: 0,
keyStep: 1,

// Multiple consecutive keys
// MGET key [key ...], DEL key [key ...]
firstKey: 0,
lastKey: -1,  // Last argument
keyStep: 1,

// Key-value pairs
// MSET key value [key value ...]
firstKey: 0,
lastKey: -1,
keyStep: 2,  // Every other argument is a key

// No keys
// PING, COMMAND, INFO
firstKey: -1,
lastKey: -1,
keyStep: 0,
```

### Category Mapping

```typescript
// String commands
categories: [CommandCategory.STRING, CommandCategory.GENERIC]

// Hash commands
categories: [CommandCategory.HASH]

// List commands
categories: [CommandCategory.LIST]

// Set commands
categories: [CommandCategory.SET]

// Sorted set commands
categories: [CommandCategory.ZSET]

// Key management commands
categories: [CommandCategory.KEYS, CommandCategory.GENERIC]

// Server commands
categories: [CommandCategory.SERVER]

// Script commands
categories: [CommandCategory.SCRIPT]

// Transaction commands
categories: [CommandCategory.TRANSACTION]

// Cluster commands
categories: [CommandCategory.CLUSTER]
```

## Migration Order (Priority)

### Phase 2a: Critical Commands (High Priority)

These are most commonly used and should be migrated first:

1. **Transaction commands** (3 commands)
   - [src/commanders/custom/commands/redis/multi.ts](src/commanders/custom/commands/redis/multi.ts) - MULTI
   - [src/commanders/custom/commands/redis/exec.ts](src/commanders/custom/commands/redis/exec.ts) - EXEC
   - [src/commanders/custom/commands/redis/discard.ts](src/commanders/custom/commands/redis/discard.ts) - DISCARD

2. **String commands** (13 commands)
   - [src/commanders/custom/commands/redis/data/strings/get.ts](src/commanders/custom/commands/redis/data/strings/get.ts)
   - [src/commanders/custom/commands/redis/data/strings/set.ts](src/commanders/custom/commands/redis/data/strings/set.ts)
   - [src/commanders/custom/commands/redis/data/strings/mget.ts](src/commanders/custom/commands/redis/data/strings/mget.ts)
   - [src/commanders/custom/commands/redis/data/strings/mset.ts](src/commanders/custom/commands/redis/data/strings/mset.ts)
   - [src/commanders/custom/commands/redis/data/strings/incr.ts](src/commanders/custom/commands/redis/data/strings/incr.ts)
   - [src/commanders/custom/commands/redis/data/strings/decr.ts](src/commanders/custom/commands/redis/data/strings/decr.ts)
   - [src/commanders/custom/commands/redis/data/strings/incrby.ts](src/commanders/custom/commands/redis/data/strings/incrby.ts)
   - [src/commanders/custom/commands/redis/data/strings/decrby.ts](src/commanders/custom/commands/redis/data/strings/decrby.ts)
   - [src/commanders/custom/commands/redis/data/strings/incrbyfloat.ts](src/commanders/custom/commands/redis/data/strings/incrbyfloat.ts)
   - [src/commanders/custom/commands/redis/data/strings/append.ts](src/commanders/custom/commands/redis/data/strings/append.ts)
   - [src/commanders/custom/commands/redis/data/strings/strlen.ts](src/commanders/custom/commands/redis/data/strings/strlen.ts)
   - [src/commanders/custom/commands/redis/data/strings/getset.ts](src/commanders/custom/commands/redis/data/strings/getset.ts)
   - [src/commanders/custom/commands/redis/data/strings/msetnx.ts](src/commanders/custom/commands/redis/data/strings/msetnx.ts)

3. **Key commands** (10 commands)
   - [src/commanders/custom/commands/redis/data/keys/del.ts](src/commanders/custom/commands/redis/data/keys/del.ts)
   - [src/commanders/custom/commands/redis/data/keys/exists.ts](src/commanders/custom/commands/redis/data/keys/exists.ts)
   - [src/commanders/custom/commands/redis/data/keys/type.ts](src/commanders/custom/commands/redis/data/keys/type.ts)
   - [src/commanders/custom/commands/redis/data/keys/ttl.ts](src/commanders/custom/commands/redis/data/keys/ttl.ts)
   - [src/commanders/custom/commands/redis/data/keys/pttl.ts](src/commanders/custom/commands/redis/data/keys/pttl.ts)
   - [src/commanders/custom/commands/redis/data/keys/expire.ts](src/commanders/custom/commands/redis/data/keys/expire.ts)
   - [src/commanders/custom/commands/redis/data/keys/expireat.ts](src/commanders/custom/commands/redis/data/keys/expireat.ts)
   - [src/commanders/custom/commands/redis/data/keys/flushdb.ts](src/commanders/custom/commands/redis/data/keys/flushdb.ts)
   - [src/commanders/custom/commands/redis/data/keys/flushall.ts](src/commanders/custom/commands/redis/data/keys/flushall.ts)
   - [src/commanders/custom/commands/redis/data/keys/dbsize.ts](src/commanders/custom/commands/redis/data/keys/dbsize.ts)

### Phase 2b: Common Data Structures (Medium Priority)

4. **Hash commands** (12 commands in [src/commanders/custom/commands/redis/data/hashes/](src/commanders/custom/commands/redis/data/hashes/))
5. **List commands** (9 commands in [src/commanders/custom/commands/redis/data/lists/](src/commanders/custom/commands/redis/data/lists/))
6. **Set commands** (11 commands in [src/commanders/custom/commands/redis/data/sets/](src/commanders/custom/commands/redis/data/sets/))
7. **Sorted Set commands** (11 commands in [src/commanders/custom/commands/redis/data/zsets/](src/commanders/custom/commands/redis/data/zsets/))

### Phase 2c: Server & Scripting (Lower Priority)

8. **Script commands** (6 commands in [src/commanders/custom/commands/redis/](src/commanders/custom/commands/redis/))
9. **Server commands** (6 commands in [src/commanders/custom/commands/redis/](src/commanders/custom/commands/redis/))
10. **Cluster commands** (4 commands in [src/commanders/custom/commands/redis/cluster/](src/commanders/custom/commands/redis/cluster/))

## Example Migrations

### Example 1: GET (Simple readonly command)

**File:** [src/commanders/custom/commands/redis/data/strings/get.ts](src/commanders/custom/commands/redis/data/strings/get.ts)

```typescript
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'
import type { Command, CommandResult } from '../../../../../types'
import { WrongNumberOfArguments } from '../../../../../core/errors'
import type { DB } from '../../../db'

export const GetCommand: CommandDefinition = {
  metadata: defineCommand('get', {
    arity: 2,
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING, CommandCategory.GENERIC],
  }),
  factory: (deps) => new Get(deps.db),
}

class Get implements Command {
  readonly metadata = GetCommand.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  async run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const val = this.db.get(args[0])
    return { response: val ?? null }
  }
}
```

### Example 2: MSET (Variable args, key-value pairs)

**File:** [src/commanders/custom/commands/redis/data/strings/mset.ts](src/commanders/custom/commands/redis/data/strings/mset.ts)

```typescript
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'
import type { Command, CommandResult } from '../../../../../types'
import { WrongNumberOfArguments } from '../../../../../core/errors'
import type { DB } from '../../../db'

export const MsetCommand: CommandDefinition = {
  metadata: defineCommand('mset', {
    arity: -3,  // At least 1 key-value pair
    flags: {
      write: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: -1,
    keyStep: 2,  // key value key value ...
    categories: [CommandCategory.STRING],
  }),
  factory: (deps) => new Mset(deps.db),
}

class Mset implements Command {
  readonly metadata = MsetCommand.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2 || args.length % 2 !== 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const keys: Buffer[] = []
    for (let i = 0; i < args.length; i += 2) {
      keys.push(args[i])
    }
    return keys
  }

  async run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2 || args.length % 2 !== 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    for (let i = 0; i < args.length; i += 2) {
      const key = args[i]
      const value = args[i + 1]
      this.db.set(key, value)
    }

    return { response: 'OK' }
  }
}
```

### Example 3: HSET (Multiple keys in hash)

**File:** [src/commanders/custom/commands/redis/data/hashes/hset.ts](src/commanders/custom/commands/redis/data/hashes/hset.ts)

```typescript
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'
import type { Command, CommandResult } from '../../../../../types'
import { WrongNumberOfArguments, WrongType } from '../../../../../core/errors'
import type { DB } from '../../../db'
import { HashDataType } from '../../../data-structures/hash'

export const HsetCommand: CommandDefinition = {
  metadata: defineCommand('hset', {
    arity: -4,  // HSET key field value [field value ...]
    flags: {
      write: true,
      denyoom: true,
      fast: true,
    },
    firstKey: 0,
    lastKey: 0,  // Only one key (the hash key)
    keyStep: 1,
    categories: [CommandCategory.HASH],
  }),
  factory: (deps) => new Hset(deps.db),
}

class Hset implements Command {
  readonly metadata = HsetCommand.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 3 || args.length % 2 === 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }
    return [args[0]]
  }

  async run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 3 || args.length % 2 === 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const key = args[0]
    let hash = this.db.get(key)

    if (hash === null) {
      hash = new HashDataType()
      this.db.set(key, hash)
    } else if (!(hash instanceof HashDataType)) {
      throw new WrongType()
    }

    let added = 0
    for (let i = 1; i < args.length; i += 2) {
      const field = args[i]
      const value = args[i + 1]
      const isNew = hash.hset(field, value)
      if (isNew) added++
    }

    return { response: added }
  }
}
```

## Update Category Index Files

Each category has an index file that exports commands. Update these to export `CommandDefinition` objects:

### Before (strings/index.ts):
```typescript
export { createGet } from './get'
export { createSet } from './set'
```

### After:
```typescript
export { GetCommand } from './get'
export { SetCommand } from './set'
```

## Testing Strategy

### Per-Command Testing

After converting each command:

1. **Run unit tests**: `npm test`
2. **Run integration tests**: `npm run test:integration:mock`
3. **Verify command in registry**:
   ```typescript
   const registry = createCommandRegistry({ db, luaEngine })
   assert(registry.has('get'))
   assert.strictEqual(registry.get('get').metadata.name, 'get')
   ```

### Batch Testing

After converting a category (e.g., all string commands):

1. Run category-specific tests
2. Verify filter methods work:
   ```typescript
   const readonly = registry.getReadonlyCommands()
   assert(readonly.some(d => d.metadata.name === 'get'))
   ```

### Final Verification

After all commands migrated:

```bash
# Run full test suite
npm run test:all

# Verify command count
const registry = createCommandRegistry({ db, luaEngine })
assert.strictEqual(registry.count(), 85)  # Or actual count

# Verify filtering
const readonly = registry.getReadonlyCommands()
const luaSafe = registry.getLuaCommands()
const multiSafe = registry.getMultiCommands()
```

## Files Modified

Total: ~85 command files + ~10 category index files

**Command files** (one modification per file):
- Add metadata definition
- Add `metadata` property to class
- Update error messages to use `this.metadata.name`

**Category index files**:
- Update exports to use `XxxCommand` naming

**Main index file**:
- Import command definitions instead of factories
- Use `registry.registerAll([...])`

## Verification Checklist

- [ ] All 85 commands converted to metadata system
- [ ] All command classes have `metadata` property
- [ ] All WrongNumberOfArguments errors use `this.metadata.name`
- [ ] Category index files export CommandDefinition objects
- [ ] Main index registers all commands in registry
- [ ] Existing unit tests pass: `npm test`
- [ ] Integration tests pass: `npm run test:integration:mock`
- [ ] Command count matches: `registry.count() === 85`
- [ ] Readonly filter works correctly
- [ ] Lua filter works correctly
- [ ] Multi filter works correctly

## Benefits After Migration

1. **Single touch point** - Adding new command requires only defining CommandDefinition
2. **No manual Sets** - Filters automatically generated from metadata
3. **Type safety** - Metadata validated at compile time
4. **Introspection ready** - COMMAND can use metadata
5. **Consistent errors** - All commands use metadata name
6. **Documentation** - Metadata serves as command documentation

## Next Steps

After Phase 2 is complete:
- **Phase 3**: Fix Lua atomicity (depends on command registry)
- **Phase 4**: Implement COMMAND (uses metadata from registry)
