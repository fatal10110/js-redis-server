# Phase 4: Implement COMMAND Command

## Objective

Implement full COMMAND introspection capability using the metadata registry created in Phase 1, enabling clients to query command properties, arity, flags, and key positions.

## Problem Statement

**Current State:** [src/commanders/custom/commands/redis/command/index.ts](src/commanders/custom/commands/redis/command/index.ts)

```typescript
export class CommandsInfo implements Command {
  run(): Promise<CommandResult> {
    return Promise.resolve({ response: 'mock response' }) // TODO
  }
}
```

The COMMAND implementation is a stub that returns a mock response. This prevents:
- Clients from discovering available commands
- Tools from introspecting command metadata
- Compatibility with Redis clients that rely on COMMAND
- Proper arity and flag validation

## Redis COMMAND Specification

The COMMAND command supports multiple subcommands:

### COMMAND (no args)
Returns array of all commands with metadata.

**Response format:**
```
1) 1) "get"                    # Command name
   2) (integer) 2              # Arity
   3) 1) readonly              # Flags
      2) fast
   4) (integer) 1              # First key position
   5) (integer) 1              # Last key position
   6) (integer) 1              # Key step
   7) 1) @read                 # Categories
      2) @string
      3) @fast
```

### COMMAND INFO <command> [command ...]
Returns metadata for specific commands.

**Example:**
```redis
COMMAND INFO GET SET
```

Returns array with metadata for GET and SET, or `null` for unknown commands.

### COMMAND COUNT
Returns count of total commands.

**Example:**
```redis
COMMAND COUNT
(integer) 85
```

### COMMAND GETKEYS <command> <arg> [arg ...]
Returns keys that would be extracted from a command.

**Example:**
```redis
COMMAND GETKEYS MSET key1 val1 key2 val2
1) "key1"
2) "key2"
```

### COMMAND DOCS (Optional, Redis 7.0+)
Returns documentation for commands. Can be implemented later.

## Solution Design

### Architecture

```
COMMAND command
  ↓
CommandRegistry.getAll()
  ↓
Format metadata → RESP response
```

The COMMAND implementation will:
1. Use `CommandRegistry` to access metadata
2. Format metadata according to Redis RESP protocol
3. Support all COMMAND subcommands
4. Handle unknown commands gracefully

### Metadata Formatting

Convert `CommandMetadata` to Redis response format:

```typescript
CommandMetadata → [
  name: string,
  arity: number,
  flags: string[],
  firstKey: number,
  lastKey: number,
  keyStep: number,
  categories: string[],
]
```

## Implementation

### 4.1 Implement COMMAND

**File:** [src/commanders/custom/commands/redis/command/index.ts](src/commanders/custom/commands/redis/command/index.ts)

```typescript
import type { Command, CommandResult } from '../../../../types'
import type { CommandRegistry } from '../../registry'
import type { CommandDefinition } from '../../registry'
import { CommandCategory } from '../../metadata'
import { UnknownCommand, WrongNumberOfArguments } from '../../../../core/errors'

export class CommandsInfo implements Command {
  // Metadata for the COMMAND command itself
  readonly metadata = {
    name: 'command',
    arity: -1,  // Variable args
    flags: { readonly: true, fast: true },
    firstKey: -1,  // No keys
    lastKey: -1,
    keyStep: 0,
    categories: [CommandCategory.SERVER],
  }

  constructor(private readonly registry: CommandRegistry) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return []  // COMMAND has no keys
  }

  async run(rawCmd: Buffer, args: Buffer[], signal: AbortSignal): Promise<CommandResult> {
    // COMMAND (no args) - list all commands
    if (args.length === 0) {
      return this.handleCommandList()
    }

    const subcommand = args[0].toString().toLowerCase()

    switch (subcommand) {
      case 'info':
        return this.handleCommandInfo(args.slice(1))
      case 'count':
        return this.handleCommandCount()
      case 'getkeys':
        return this.handleCommandGetKeys(args.slice(1))
      case 'docs':
        return this.handleCommandDocs(args.slice(1))
      case 'list':
        return this.handleCommandNames()
      default:
        throw new UnknownCommand(rawCmd, args)
    }
  }

  /**
   * COMMAND - Return all command metadata
   */
  private handleCommandList(): CommandResult {
    const allCommands = this.registry.getAll()
    const response = allCommands.map(def => this.formatCommand(def))
    return { response }
  }

  /**
   * COMMAND INFO <cmd> [<cmd> ...]
   */
  private handleCommandInfo(args: Buffer[]): CommandResult {
    if (args.length === 0) {
      throw new WrongNumberOfArguments('command|info')
    }

    const response = args.map(arg => {
      const cmdName = arg.toString().toLowerCase()
      const def = this.registry.get(cmdName)
      return def ? this.formatCommand(def) : null
    })

    return { response }
  }

  /**
   * COMMAND COUNT
   */
  private handleCommandCount(): CommandResult {
    return { response: this.registry.count() }
  }

  /**
   * COMMAND GETKEYS <command> <arg> [arg ...]
   */
  private handleCommandGetKeys(args: Buffer[]): CommandResult {
    if (args.length < 1) {
      throw new WrongNumberOfArguments('command|getkeys')
    }

    const cmdName = args[0].toString().toLowerCase()
    const def = this.registry.get(cmdName)

    if (!def) {
      throw new Error(`Unknown command '${cmdName}'`)
    }

    // Create command instance to extract keys
    const cmd = def.factory({ db: null as any })  // DB not needed for getKeys
    const cmdArgs = args.slice(1)

    try {
      const keys = cmd.getKeys(args[0], cmdArgs)
      return { response: keys }
    } catch (err) {
      throw new Error(`Invalid arguments for ${cmdName}`)
    }
  }

  /**
   * COMMAND DOCS [<cmd> ...] (Optional, Redis 7.0+)
   */
  private handleCommandDocs(args: Buffer[]): CommandResult {
    // Stub for now - can be implemented later
    return { response: [] }
  }

  /**
   * COMMAND LIST (Redis 7.0+)
   */
  private handleCommandNames(): CommandResult {
    const names = this.registry.getNames()
    return { response: names }
  }

  /**
   * Format command metadata to Redis response format
   * Returns: [name, arity, flags, firstKey, lastKey, keyStep, categories]
   */
  private formatCommand(def: CommandDefinition): unknown[] {
    const { metadata } = def

    return [
      metadata.name,
      metadata.arity,
      this.formatFlags(metadata.flags),
      metadata.firstKey + 1,  // Redis uses 1-indexed positions (0 means no keys)
      metadata.lastKey === -1 ? metadata.lastKey : metadata.lastKey + 1,
      metadata.keyStep,
      metadata.categories,
    ]
  }

  /**
   * Convert CommandFlags to array of flag strings
   */
  private formatFlags(flags: Record<string, boolean | undefined>): string[] {
    const result: string[] = []

    if (flags.readonly) result.push('readonly')
    if (flags.write) result.push('write')
    if (flags.denyoom) result.push('denyoom')
    if (flags.admin) result.push('admin')
    if (flags.noscript) result.push('noscript')
    if (flags.random) result.push('random')
    if (flags.blocking) result.push('blocking')
    if (flags.fast) result.push('fast')
    if (flags.movablekeys) result.push('movablekeys')
    if (flags.transaction) result.push('transaction')

    return result
  }
}

/**
 * Factory function
 */
export function createCommand(registry: CommandRegistry): Command {
  return new CommandsInfo(registry)
}
```

### 4.2 Update Command Registration

**File:** [src/commanders/custom/commands/redis/index.ts](src/commanders/custom/commands/redis/index.ts)

```typescript
import { createCommand as createCommandInfo } from './command'

export function createCommands(
  luaEngine: LuaEngine,
  db: DB,
  executionContext?: ExecutionContext,
): Record<string, Command> {
  // Create registry first
  const registry = createCommandRegistry({ db, luaEngine })

  // Create commands from registry
  const commands = registry.createCommands({ db, luaEngine, executionContext })

  // Add COMMAND command with registry reference
  commands.command = createCommandInfo(registry)

  return commands
}
```

### 4.3 Update CommandDefinition Export

**File:** [src/commanders/custom/commands/redis/command/index.ts](src/commanders/custom/commands/redis/command/index.ts)

Add metadata definition:

```typescript
import { defineCommand, CommandCategory } from '../metadata'
import type { CommandDefinition } from '../registry'

export const CommandCommand: CommandDefinition = {
  metadata: defineCommand('command', {
    arity: -1,
    flags: {
      readonly: true,
      fast: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 0,
    categories: [CommandCategory.SERVER],
  }),
  factory: (deps) => {
    // COMMAND needs registry reference
    // This will be handled specially in createCommands()
    throw new Error('COMMAND must be created with createCommand(registry)')
  },
}
```

## Testing

### Unit Tests

Create `tests/command-command.test.ts`:

```typescript
import { test, describe } from 'node:test'
import assert from 'node:assert'
import { CommandRegistry } from '../src/commanders/custom/commands/registry'
import { CommandsInfo } from '../src/commanders/custom/commands/redis/command'
import { defineCommand, CommandCategory } from '../src/commanders/custom/commands/metadata'

describe('COMMAND Command', () => {
  test('COMMAND returns all commands', async () => {
    const registry = new CommandRegistry()

    registry.register({
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true, fast: true },
        categories: [CommandCategory.STRING],
      }),
      factory: () => ({ /* mock */ }),
    })

    const cmd = new CommandsInfo(registry)
    const result = await cmd.run(Buffer.from('COMMAND'), [], new AbortController().signal)

    assert.ok(Array.isArray(result.response))
    assert.strictEqual((result.response as any[]).length, 1)

    const [commandInfo] = result.response as any[]
    assert.strictEqual(commandInfo[0], 'get')
    assert.strictEqual(commandInfo[1], 2)
    assert.ok(commandInfo[2].includes('readonly'))
    assert.ok(commandInfo[2].includes('fast'))
  })

  test('COMMAND INFO returns specific command metadata', async () => {
    const registry = new CommandRegistry()

    registry.registerAll([
      {
        metadata: defineCommand('get', {
          arity: 2,
          flags: { readonly: true },
          firstKey: 0,
          lastKey: 0,
          categories: [CommandCategory.STRING],
        }),
        factory: () => ({ /* mock */ }),
      },
      {
        metadata: defineCommand('set', {
          arity: -3,
          flags: { write: true },
          firstKey: 0,
          lastKey: 0,
          categories: [CommandCategory.STRING],
        }),
        factory: () => ({ /* mock */ }),
      },
    ])

    const cmd = new CommandsInfo(registry)
    const result = await cmd.run(
      Buffer.from('COMMAND'),
      [Buffer.from('INFO'), Buffer.from('get'), Buffer.from('nonexistent')],
      new AbortController().signal,
    )

    const response = result.response as any[]
    assert.strictEqual(response.length, 2)

    // First result: GET
    assert.strictEqual(response[0][0], 'get')
    assert.strictEqual(response[0][1], 2)

    // Second result: nonexistent (null)
    assert.strictEqual(response[1], null)
  })

  test('COMMAND COUNT returns command count', async () => {
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

    const cmd = new CommandsInfo(registry)
    const result = await cmd.run(
      Buffer.from('COMMAND'),
      [Buffer.from('COUNT')],
      new AbortController().signal,
    )

    assert.strictEqual(result.response, 2)
  })

  test('COMMAND GETKEYS extracts keys from command', async () => {
    const registry = new CommandRegistry()

    const mockGetCommand = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        firstKey: 0,
        lastKey: 0,
        categories: [CommandCategory.STRING],
      }),
      factory: () => ({
        metadata: { name: 'get' },
        getKeys: (rawCmd: Buffer, args: Buffer[]) => [args[0]],
        run: async () => ({ response: null }),
      }),
    }

    registry.register(mockGetCommand)

    const cmd = new CommandsInfo(registry)
    const result = await cmd.run(
      Buffer.from('COMMAND'),
      [Buffer.from('GETKEYS'), Buffer.from('GET'), Buffer.from('mykey')],
      new AbortController().signal,
    )

    const keys = result.response as Buffer[]
    assert.strictEqual(keys.length, 1)
    assert.strictEqual(keys[0].toString(), 'mykey')
  })
})
```

### Integration Tests

Add to `tests-integration/ioredis/command.test.ts` (NEW):

```typescript
import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import Redis from 'ioredis'

describe('COMMAND integration tests', () => {
  let redisClient: Redis | null = null

  before(async () => {
    redisClient = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: parseInt(process.env.REDIS_PORT || '6379'),
    })
  })

  after(async () => {
    await redisClient?.quit()
  })

  test('COMMAND returns array of commands', async () => {
    const commands = await redisClient!.command()

    assert.ok(Array.isArray(commands))
    assert.ok(commands.length > 0)

    // Check format of first command
    const [cmdInfo] = commands
    assert.ok(Array.isArray(cmdInfo))
    assert.strictEqual(typeof cmdInfo[0], 'string')  // Name
    assert.strictEqual(typeof cmdInfo[1], 'number')  // Arity
    assert.ok(Array.isArray(cmdInfo[2]))  // Flags
  })

  test('COMMAND INFO returns metadata for specific commands', async () => {
    const result = await redisClient!.call('COMMAND', 'INFO', 'GET', 'SET')

    assert.ok(Array.isArray(result))
    assert.strictEqual(result.length, 2)

    const [getInfo, setInfo] = result as any[]

    // Verify GET metadata
    assert.strictEqual(getInfo[0], 'get')
    assert.strictEqual(getInfo[1], 2)  // Arity
    assert.ok(getInfo[2].includes('readonly'))

    // Verify SET metadata
    assert.strictEqual(setInfo[0], 'set')
    assert.ok(setInfo[1] < 0)  // Variable args (negative arity)
    assert.ok(setInfo[2].includes('write'))
  })

  test('COMMAND COUNT returns command count', async () => {
    const count = await redisClient!.call('COMMAND', 'COUNT')

    assert.strictEqual(typeof count, 'number')
    assert.ok(count > 80)  // At least 80 commands
  })

  test('COMMAND INFO returns null for unknown command', async () => {
    const result = await redisClient!.call('COMMAND', 'INFO', 'NONEXISTENT')

    assert.ok(Array.isArray(result))
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0], null)
  })
})
```

### Verification Tests

Add comprehensive test to verify all commands have proper metadata:

```typescript
test('all commands have valid metadata', async () => {
  const commands = await redisClient!.command()

  commands.forEach((cmdInfo: any) => {
    const [name, arity, flags, firstKey, lastKey, keyStep, categories] = cmdInfo

    // Verify structure
    assert.strictEqual(typeof name, 'string')
    assert.strictEqual(typeof arity, 'number')
    assert.ok(Array.isArray(flags))
    assert.strictEqual(typeof firstKey, 'number')
    assert.strictEqual(typeof lastKey, 'number')
    assert.strictEqual(typeof keyStep, 'number')
    assert.ok(Array.isArray(categories))

    // Verify arity
    assert.notStrictEqual(arity, 0)  // Arity should never be 0

    // Verify key positions
    if (firstKey > 0) {
      assert.ok(lastKey >= firstKey || lastKey === -1)
      assert.ok(keyStep > 0)
    }
  })
})
```

## Files Modified

1. **[src/commanders/custom/commands/redis/command/index.ts](src/commanders/custom/commands/redis/command/index.ts)** - Full implementation (~250 lines)
2. **[src/commanders/custom/commands/redis/index.ts](src/commanders/custom/commands/redis/index.ts)** - Add COMMAND creation (~10 lines)
3. **[tests/command-command.test.ts](tests/command-command.test.ts)** - NEW TEST FILE (~150 lines)
4. **[tests-integration/ioredis/command.test.ts](tests-integration/ioredis/command.test.ts)** - NEW TEST FILE (~100 lines)

## Verification Checklist

- [ ] COMMAND returns all command metadata
- [ ] COMMAND INFO returns metadata for specific commands
- [ ] COMMAND INFO returns null for unknown commands
- [ ] COMMAND COUNT returns correct count
- [ ] COMMAND GETKEYS extracts keys properly
- [ ] COMMAND LIST returns command names (Redis 7.0+)
- [ ] Metadata format matches Redis specification
- [ ] Key positions are 1-indexed in response
- [ ] Flags correctly formatted as string array
- [ ] Integration test with ioredis passes
- [ ] Integration test with node-redis passes
- [ ] All 85 commands have valid metadata

## Benefits

1. **Client compatibility** - Redis clients can introspect commands
2. **Documentation** - Metadata serves as command documentation
3. **Validation** - Clients can validate arity before sending
4. **Debugging** - Easy to inspect command properties
5. **Compatibility testing** - Can compare against real Redis
6. **Future features** - Foundation for command deprecation, aliasing

## Next Steps

After Phase 4 is complete:
- **Phase 5**: Optional transport improvements (command queuing, backpressure)
- Consider adding COMMAND DOCS subcommand with full documentation
- Add command aliasing support (e.g., SUBSTR → GETRANGE)
