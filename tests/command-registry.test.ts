import { test, describe } from 'node:test'
import assert from 'node:assert'
import { CommandRegistry } from '../src/commanders/custom/commands/registry'
import {
  defineCommand,
  CommandCategory,
} from '../src/commanders/custom/commands/metadata'
import type { CommandDefinition } from '../src/commanders/custom/commands/registry'
import type { Command, CommandResult } from '../src/types'

// Mock command class for testing
class MockCommand implements Command {
  constructor(public readonly metadata: any) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return args
  }

  run(
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<CommandResult> {
    return Promise.resolve({ response: 'OK' })
  }
}

describe('CommandRegistry', () => {
  test('should register and retrieve commands', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    registry.register(getDef)
    assert.strictEqual(registry.has('get'), true)
    assert.strictEqual(registry.get('get'), getDef)
  })

  test('should be case-insensitive for command names', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    registry.register(getDef)
    assert.strictEqual(registry.has('GET'), true)
    assert.strictEqual(registry.has('Get'), true)
    assert.strictEqual(registry.get('GET'), getDef)
  })

  test('should filter readonly commands', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    const setDef: CommandDefinition = {
      metadata: defineCommand('set', {
        arity: -3,
        flags: { write: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(setDef.metadata),
    }

    registry.registerAll([getDef, setDef])

    const readonly = registry.getReadonlyCommands()
    assert.strictEqual(readonly.length, 1)
    assert.strictEqual(readonly[0].metadata.name, 'get')
  })

  test('should filter lua-safe commands', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    const randomkeyDef: CommandDefinition = {
      metadata: defineCommand('randomkey', {
        arity: 1,
        flags: { readonly: true, random: true },
        categories: [CommandCategory.KEYS],
      }),
      factory: deps => new MockCommand(randomkeyDef.metadata),
    }

    const blpopDef: CommandDefinition = {
      metadata: defineCommand('blpop', {
        arity: -3,
        flags: { write: true, blocking: true },
        categories: [CommandCategory.LIST],
      }),
      factory: deps => new MockCommand(blpopDef.metadata),
    }

    registry.registerAll([getDef, randomkeyDef, blpopDef])

    const luaSafe = registry.getLuaCommands()
    assert.strictEqual(luaSafe.length, 1)
    assert.strictEqual(luaSafe[0].metadata.name, 'get')
  })

  test('should filter multi-safe commands', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    const blpopDef: CommandDefinition = {
      metadata: defineCommand('blpop', {
        arity: -3,
        flags: { write: true, blocking: true },
        categories: [CommandCategory.LIST],
      }),
      factory: deps => new MockCommand(blpopDef.metadata),
    }

    const multiDef: CommandDefinition = {
      metadata: defineCommand('multi', {
        arity: 1,
        flags: { transaction: true },
        categories: [CommandCategory.TRANSACTION],
      }),
      factory: deps => new MockCommand(multiDef.metadata),
    }

    registry.registerAll([getDef, blpopDef, multiDef])

    const multiSafe = registry.getMultiCommands()
    assert.strictEqual(multiSafe.length, 1)
    assert.strictEqual(multiSafe[0].metadata.name, 'get')
  })

  test('should throw on duplicate registration', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    registry.register(getDef)

    assert.throws(() => {
      registry.register(getDef)
    }, /already registered/)
  })

  test('should filter by category', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    const hgetDef: CommandDefinition = {
      metadata: defineCommand('hget', {
        arity: 3,
        flags: { readonly: true },
        categories: [CommandCategory.HASH],
      }),
      factory: deps => new MockCommand(hgetDef.metadata),
    }

    registry.registerAll([getDef, hgetDef])

    const stringCommands = registry.getByCategory(CommandCategory.STRING)
    assert.strictEqual(stringCommands.length, 1)
    assert.strictEqual(stringCommands[0].metadata.name, 'get')

    const hashCommands = registry.getByCategory(CommandCategory.HASH)
    assert.strictEqual(hashCommands.length, 1)
    assert.strictEqual(hashCommands[0].metadata.name, 'hget')
  })

  test('should create command instances', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    registry.register(getDef)

    const commands = registry.createCommands({ db: {} as any })
    assert.ok(commands.get)
    assert.ok(commands.get instanceof MockCommand)
  })

  test('should return correct count', () => {
    const registry = new CommandRegistry()

    assert.strictEqual(registry.count(), 0)

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    registry.register(getDef)
    assert.strictEqual(registry.count(), 1)
  })

  test('should get all command names', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    const setDef: CommandDefinition = {
      metadata: defineCommand('set', {
        arity: -3,
        flags: { write: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(setDef.metadata),
    }

    registry.registerAll([getDef, setDef])

    const names = registry.getNames()
    assert.strictEqual(names.length, 2)
    assert.ok(names.includes('get'))
    assert.ok(names.includes('set'))
  })

  test('should support custom filter predicates', () => {
    const registry = new CommandRegistry()

    const getDef: CommandDefinition = {
      metadata: defineCommand('get', {
        arity: 2,
        flags: { readonly: true, fast: true },
        categories: [CommandCategory.STRING],
      }),
      factory: deps => new MockCommand(getDef.metadata),
    }

    const scanDef: CommandDefinition = {
      metadata: defineCommand('scan', {
        arity: -2,
        flags: { readonly: true },
        categories: [CommandCategory.KEYS],
      }),
      factory: deps => new MockCommand(scanDef.metadata),
    }

    registry.registerAll([getDef, scanDef])

    const fastCommands = registry.filter(
      def => def.metadata.flags.fast === true,
    )
    assert.strictEqual(fastCommands.length, 1)
    assert.strictEqual(fastCommands[0].metadata.name, 'get')
  })
})

describe('defineCommand helper', () => {
  test('should set defaults for optional fields', () => {
    const metadata = defineCommand('get', {
      arity: 2,
      flags: { readonly: true },
      categories: [CommandCategory.STRING],
    })

    assert.strictEqual(metadata.name, 'get')
    assert.strictEqual(metadata.arity, 2)
    assert.strictEqual(metadata.firstKey, -1)
    assert.strictEqual(metadata.lastKey, -1)
    assert.strictEqual(metadata.keyStep, 1)
    assert.strictEqual(metadata.limit, 0)
  })

  test('should use provided optional fields', () => {
    const metadata = defineCommand('get', {
      arity: 2,
      flags: { readonly: true },
      firstKey: 0,
      lastKey: 0,
      keyStep: 1,
      categories: [CommandCategory.STRING],
    })

    assert.strictEqual(metadata.firstKey, 0)
    assert.strictEqual(metadata.lastKey, 0)
    assert.strictEqual(metadata.keyStep, 1)
  })

  test('should lowercase command name', () => {
    const metadata = defineCommand('GET', {
      arity: 2,
      flags: { readonly: true },
      categories: [CommandCategory.STRING],
    })

    assert.strictEqual(metadata.name, 'get')
  })
})
