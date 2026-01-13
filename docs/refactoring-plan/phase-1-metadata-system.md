# Phase 1: Command Metadata System (Foundation)

## Objective

Create a declarative command registration system that:
- Eliminates boilerplate when adding commands
- Provides centralized metadata storage
- Enables automatic filtering by command properties
- Supports command introspection (COMMAND command)

## Problem Statement

Currently, adding a command requires manual modifications in 5-8 places:
1. Create command file
2. Add to category index exports
3. Import in main index
4. Add to `createCommands()` registry
5. Optionally add to `readonlyCommandNames` Set
6. Optionally add to `multiCommandNames` Set
7. Optionally add to `luaCommandNames` Set
8. Repeat for `createClusterCommands()` if applicable

Metadata about commands (arity, key positions, flags) is scattered across individual command implementations without a centralized source of truth.

## Solution Design

### 1.1 Command Metadata Schema

Create `src/commanders/custom/commands/metadata.ts`:

```typescript
/**
 * Redis command metadata following Redis COMMAND specification
 * @see https://redis.io/commands/command
 */
export interface CommandMetadata {
  /** Command name (lowercase) */
  name: string

  /**
   * Number of arguments
   * Positive: exact count (including command name)
   * Negative: minimum count (abs value), variable args
   * Example: GET = 2 (GET key), MGET = -2 (MGET key [key ...])
   */
  arity: number

  /** Command flags */
  flags: CommandFlags

  /**
   * First key position (0-indexed in args, not including command name)
   * -1 means no keys
   */
  firstKey: number

  /**
   * Last key position
   * -1 means last argument is key
   * -2 means second-to-last, etc.
   */
  lastKey: number

  /**
   * Step between keys (usually 1)
   * Example: MGET has step 1, MSET has step 2 (key val key val)
   */
  keyStep: number

  /** Redis command categories */
  categories: CommandCategory[]
}

/**
 * Command flags following Redis specification
 */
export interface CommandFlags {
  /** Command doesn't modify data (safe for replicas) */
  readonly?: boolean

  /** Command modifies data */
  write?: boolean

  /** Deny command when used memory > maxmemory */
  denyoom?: boolean

  /** Administrative command */
  admin?: boolean

  /** Not allowed in Lua scripts */
  noscript?: boolean

  /** Returns random/non-deterministic results */
  random?: boolean

  /** Blocking operation (BLPOP, BRPOP, etc.) */
  blocking?: boolean

  /** O(1) time complexity */
  fast?: boolean

  /** Keys are not in fixed positions (requires key extraction) */
  movablekeys?: boolean

  /** Transaction-related command */
  transaction?: boolean
}

/**
 * Redis command categories
 * @see https://redis.io/commands#command-categories
 */
export enum CommandCategory {
  STRING = '@string',
  HASH = '@hash',
  LIST = '@list',
  SET = '@set',
  ZSET = '@zset',
  KEYS = '@keys',
  GENERIC = '@generic',
  SCRIPT = '@scripting',
  SERVER = '@server',
  CONNECTION = '@connection',
  CLUSTER = '@cluster',
  TRANSACTION = '@transactions',
  PUBSUB = '@pubsub',
  STREAM = '@stream',
}

/**
 * Helper to create metadata with defaults
 */
export function defineCommand(
  name: string,
  options: {
    arity: number
    flags: CommandFlags
    firstKey?: number
    lastKey?: number
    keyStep?: number
    categories: CommandCategory[]
  }
): CommandMetadata {
  return {
    name: name.toLowerCase(),
    arity: options.arity,
    flags: options.flags,
    firstKey: options.firstKey ?? -1,
    lastKey: options.lastKey ?? -1,
    keyStep: options.keyStep ?? 1,
    categories: options.categories,
  }
}
```

### 1.2 Command Registry

Create `src/commanders/custom/commands/registry.ts`:

```typescript
import type { Command } from '../../../types'
import type { CommandMetadata } from './metadata'
import type { DB } from '../db'
import type { LuaEngine } from '../../../lua-engine'  // Adjust path as needed

/**
 * Dependencies passed to command factories
 */
export interface CommandDependencies {
  db: DB
  luaEngine?: LuaEngine
  discoveryService?: any  // For cluster commands
  mySelfId?: string
}

/**
 * Command definition combining metadata and factory
 */
export interface CommandDefinition {
  metadata: CommandMetadata
  factory: (deps: CommandDependencies) => Command
}

/**
 * Central command registry
 */
export class CommandRegistry {
  private commands = new Map<string, CommandDefinition>()

  /**
   * Register a command
   */
  register(definition: CommandDefinition): void {
    const name = definition.metadata.name.toLowerCase()

    if (this.commands.has(name)) {
      throw new Error(`Command '${name}' is already registered`)
    }

    this.commands.set(name, definition)
  }

  /**
   * Register multiple commands at once
   */
  registerAll(definitions: CommandDefinition[]): void {
    definitions.forEach(def => this.register(def))
  }

  /**
   * Get command definition by name
   */
  get(name: string): CommandDefinition | undefined {
    return this.commands.get(name.toLowerCase())
  }

  /**
   * Check if command exists
   */
  has(name: string): boolean {
    return this.commands.has(name.toLowerCase())
  }

  /**
   * Get all registered commands
   */
  getAll(): CommandDefinition[] {
    return Array.from(this.commands.values())
  }

  /**
   * Get command names
   */
  getNames(): string[] {
    return Array.from(this.commands.keys())
  }

  /**
   * Get readonly commands (safe for replicas)
   */
  getReadonlyCommands(): CommandDefinition[] {
    return this.getAll().filter(def => def.metadata.flags.readonly === true)
  }

  /**
   * Get commands allowed in MULTI/EXEC transactions
   * (excludes blocking, transaction control, etc.)
   */
  getMultiCommands(): CommandDefinition[] {
    return this.getAll().filter(def => {
      const { flags } = def.metadata
      return (
        !flags.blocking &&
        !flags.transaction &&
        !flags.noscript
      )
    })
  }

  /**
   * Get commands allowed in Lua scripts
   * (excludes random, blocking, noscript, admin)
   */
  getLuaCommands(): CommandDefinition[] {
    return this.getAll().filter(def => {
      const { flags } = def.metadata
      return (
        !flags.random &&
        !flags.blocking &&
        !flags.noscript &&
        !flags.admin
      )
    })
  }

  /**
   * Filter commands by category
   */
  getByCategory(category: CommandCategory): CommandDefinition[] {
    return this.getAll().filter(def =>
      def.metadata.categories.includes(category)
    )
  }

  /**
   * Filter commands by custom predicate
   */
  filter(predicate: (def: CommandDefinition) => boolean): CommandDefinition[] {
    return this.getAll().filter(predicate)
  }

  /**
   * Create command instances from registry
   * (Legacy compatibility with Record<string, Command> pattern)
   */
  createCommands(deps: CommandDependencies): Record<string, Command> {
    const commands: Record<string, Command> = {}

    this.getAll().forEach(def => {
      commands[def.metadata.name] = def.factory(deps)
    })

    return commands
  }

  /**
   * Get count of registered commands
   */
  count(): number {
    return this.commands.size
  }
}

/**
 * Global registry instance (singleton)
 */
export const globalCommandRegistry = new CommandRegistry()
```

### 1.3 Update Command Interface

Modify `src/types.ts` to include metadata:

```typescript
import type { CommandMetadata } from './commanders/custom/commands/metadata'

export interface Command {
  /** Command metadata (arity, flags, key positions) */
  readonly metadata: CommandMetadata

  /**
   * Extract keys from command arguments
   * Used for cluster slot routing and WATCH tracking
   */
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[]

  /**
   * Execute command
   */
  run(
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal
  ): Promise<CommandResult>
}

// ... rest of types.ts remains unchanged
```

### 1.4 Example Command Conversion

Show how a command would be converted to use the new system.

**Before** - `src/commanders/custom/commands/redis/data/strings/get.ts`:

```typescript
export function createGet(db: DB): Command {
  return new Get(db)
}

class Get implements Command {
  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('get')
    }
    return [args[0]]
  }

  async run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('get')
    }
    const val = this.db.get(args[0])
    return { response: val ?? null }
  }
}
```

**After** - with metadata:

```typescript
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'
import type { Command, CommandResult } from '../../../../../types'
import { WrongNumberOfArguments } from '../../../../../core/errors'
import type { DB } from '../../../db'

// Command definition with metadata
export const GetCommand: CommandDefinition = {
  metadata: defineCommand('get', {
    arity: 2,  // GET key
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: 0,  // First arg is the key
    lastKey: 0,   // Last arg is the key
    keyStep: 1,   // Single key
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

### 1.5 Update Main Index

Modify `src/commanders/custom/commands/redis/index.ts`:

```typescript
import { CommandRegistry } from './registry'
import type { CommandDependencies } from './registry'
import type { DB } from '../../db'
import type { LuaEngine } from '../../../../lua-engine'

// Import command definitions (not factories)
import { GetCommand } from './data/strings/get'
import { SetCommand } from './data/strings/set'
// ... import all other XxxCommand definitions

/**
 * Create and populate command registry
 */
export function createCommandRegistry(deps: CommandDependencies): CommandRegistry {
  const registry = new CommandRegistry()

  // Register all commands
  registry.registerAll([
    // Strings
    GetCommand,
    SetCommand,
    // ... all other commands
  ])

  return registry
}

/**
 * Legacy: Create commands as Record<string, Command>
 * Used by existing Commander implementations
 */
export function createCommands(luaEngine: LuaEngine, db: DB): Record<string, Command> {
  const registry = createCommandRegistry({ db, luaEngine })
  return registry.createCommands({ db, luaEngine })
}

/**
 * Legacy: Create readonly commands
 */
export function createReadonlyCommands(luaEngine: LuaEngine, db: DB): Record<string, Command> {
  const registry = createCommandRegistry({ db, luaEngine })
  const readonlyDefs = registry.getReadonlyCommands()

  const commands: Record<string, Command> = {}
  readonlyDefs.forEach(def => {
    commands[def.metadata.name] = def.factory({ db, luaEngine })
  })
  return commands
}

/**
 * Legacy: Create commands allowed in MULTI
 */
export function createMultiCommands(luaEngine: LuaEngine, db: DB): Record<string, Command> {
  const registry = createCommandRegistry({ db, luaEngine })
  const multiDefs = registry.getMultiCommands()

  const commands: Record<string, Command> = {}
  multiDefs.forEach(def => {
    commands[def.metadata.name] = def.factory({ db, luaEngine })
  })
  return commands
}

/**
 * Legacy: Create commands allowed in Lua
 */
export function createLuaCommands(luaEngine: LuaEngine, db: DB): Record<string, Command> {
  const registry = createCommandRegistry({ db, luaEngine })
  const luaDefs = registry.getLuaCommands()

  const commands: Record<string, Command> = {}
  luaDefs.forEach(def => {
    commands[def.metadata.name] = def.factory({ db, luaEngine })
  })
  return commands
}

// Export cluster commands (delegates to createCommands + cluster-specific)
export { createClusterCommands } from './cluster'
```

## Files to Create

1. **[src/commanders/custom/commands/metadata.ts](src/commanders/custom/commands/metadata.ts)** (~150 lines)
   - CommandMetadata interface
   - CommandFlags interface
   - CommandCategory enum
   - defineCommand() helper

2. **[src/commanders/custom/commands/registry.ts](src/commanders/custom/commands/registry.ts)** (~200 lines)
   - CommandRegistry class
   - CommandDefinition interface
   - CommandDependencies interface
   - Filter methods

## Files to Modify

1. **[src/types.ts](src/types.ts)** (~5 lines added)
   - Add `metadata: CommandMetadata` to Command interface

2. **[src/commanders/custom/commands/redis/index.ts](src/commanders/custom/commands/redis/index.ts)** (~50 lines modified)
   - Replace manual Sets with registry filters
   - Use `registerAll()` instead of manual object literal

## Testing

### Unit Tests

Create `tests/command-registry.test.ts`:

```typescript
import { test, describe } from 'node:test'
import assert from 'node:assert'
import { CommandRegistry } from '../src/commanders/custom/commands/registry'
import { defineCommand, CommandCategory } from '../src/commanders/custom/commands/metadata'

describe('CommandRegistry', () => {
  test('should register and retrieve commands', () => {
    const registry = new CommandRegistry()

    const getDef = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: () => ({ /* mock */ }),
    }

    registry.register(getDef)
    assert.strictEqual(registry.has('get'), true)
    assert.strictEqual(registry.get('get'), getDef)
  })

  test('should filter readonly commands', () => {
    const registry = new CommandRegistry()

    registry.registerAll([
      {
        metadata: defineCommand('get', {
          arity: 2,
          flags: { readonly: true },
          categories: [CommandCategory.STRING],
        }),
        factory: () => ({ /* mock */ }),
      },
      {
        metadata: defineCommand('set', {
          arity: -3,
          flags: { write: true },
          categories: [CommandCategory.STRING],
        }),
        factory: () => ({ /* mock */ }),
      },
    ])

    const readonly = registry.getReadonlyCommands()
    assert.strictEqual(readonly.length, 1)
    assert.strictEqual(readonly[0].metadata.name, 'get')
  })

  test('should filter lua-safe commands', () => {
    const registry = new CommandRegistry()

    registry.registerAll([
      {
        metadata: defineCommand('get', {
          arity: 2,
          flags: { readonly: true },
          categories: [CommandCategory.STRING],
        }),
        factory: () => ({ /* mock */ }),
      },
      {
        metadata: defineCommand('randomkey', {
          arity: 1,
          flags: { readonly: true, random: true },
          categories: [CommandCategory.KEYS],
        }),
        factory: () => ({ /* mock */ }),
      },
    ])

    const luaSafe = registry.getLuaCommands()
    assert.strictEqual(luaSafe.length, 1)
    assert.strictEqual(luaSafe[0].metadata.name, 'get')
  })

  test('should throw on duplicate registration', () => {
    const registry = new CommandRegistry()

    const getDef = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: () => ({ /* mock */ }),
    }

    registry.register(getDef)

    assert.throws(() => {
      registry.register(getDef)
    }, /already registered/)
  })
})
```

### Integration Tests

Verify existing integration tests still pass after changes:

```bash
npm run test:integration:mock
```

## Verification Checklist

- [ ] CommandRegistry can register and retrieve commands
- [ ] Metadata filtering works (readonly/multi/lua)
- [ ] Command interface includes metadata property
- [ ] Example command (GET) converted successfully
- [ ] Existing tests still pass: `npm test`
- [ ] No breaking changes to external API

## Benefits

1. **Single source of truth** - Command metadata in one place
2. **Automatic filtering** - No manual Set maintenance
3. **Type safety** - Metadata validated at compile time
4. **Introspection** - Foundation for COMMAND implementation
5. **Reduced boilerplate** - Import one definition instead of managing multiple lists

## Next Steps

After Phase 1 is complete:
- **Phase 2**: Migrate remaining 84 commands to metadata system
- **Phase 3**: Fix Lua atomicity using command registry
- **Phase 4**: Implement COMMAND using metadata
