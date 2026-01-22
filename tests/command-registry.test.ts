import { test, describe } from 'node:test'
import assert from 'node:assert'
import { CommandRegistry } from '../src/commanders/custom/commands/registry'
import {
  defineCommand,
  CommandCategory,
} from '../src/commanders/custom/commands/metadata'
import {
  SchemaCommand,
  CommandContext,
  t,
} from '../src/commanders/custom/schema'

// Mock command classes for testing
class GetCommand extends SchemaCommand<[Buffer]> {
  metadata = defineCommand('get', {
    arity: 2,
    flags: { readonly: true },
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key()])

  protected execute(_args: [Buffer], _ctx: CommandContext) {
    // Mock implementation
  }
}

class SetCommand extends SchemaCommand<[Buffer, string]> {
  metadata = defineCommand('set', {
    arity: -3,
    flags: { write: true },
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key(), t.string()])

  protected execute(_args: [Buffer, string], _ctx: CommandContext) {
    // Mock implementation
  }
}

class HgetCommand extends SchemaCommand<[Buffer, string]> {
  metadata = defineCommand('hget', {
    arity: 3,
    flags: { readonly: true },
    categories: [CommandCategory.HASH],
  })

  protected schema = t.tuple([t.key(), t.string()])

  protected execute(_args: [Buffer, string], _ctx: CommandContext) {
    // Mock implementation
  }
}

class RandomkeyCommand extends SchemaCommand<[]> {
  metadata = defineCommand('randomkey', {
    arity: 1,
    flags: { readonly: true, random: true },
    categories: [CommandCategory.KEYS],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], _ctx: CommandContext) {
    // Mock implementation
  }
}

class BlpopCommand extends SchemaCommand<[]> {
  metadata = defineCommand('blpop', {
    arity: -3,
    flags: { write: true, blocking: true },
    categories: [CommandCategory.LIST],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], _ctx: CommandContext) {
    // Mock implementation
  }
}

class MultiCommand extends SchemaCommand<[]> {
  metadata = defineCommand('multi', {
    arity: 1,
    flags: { transaction: true },
    categories: [CommandCategory.TRANSACTION],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], _ctx: CommandContext) {
    // Mock implementation
  }
}

class ScanCommand extends SchemaCommand<[]> {
  metadata = defineCommand('scan', {
    arity: -2,
    flags: { readonly: true },
    categories: [CommandCategory.KEYS],
  })

  protected schema = t.tuple([])

  protected execute(_args: [], _ctx: CommandContext) {
    // Mock implementation
  }
}

class FastGetCommand extends SchemaCommand<[Buffer]> {
  metadata = defineCommand('fastget', {
    arity: 2,
    flags: { readonly: true, fast: true },
    categories: [CommandCategory.STRING],
  })

  protected schema = t.tuple([t.key()])

  protected execute(_args: [Buffer], _ctx: CommandContext) {
    // Mock implementation
  }
}

describe('CommandRegistry', () => {
  test('should register and retrieve commands', () => {
    const registry = new CommandRegistry()
    const getCmd = new GetCommand()

    registry.register(getCmd)
    assert.strictEqual(registry.has('get'), true)
    assert.strictEqual(registry.get('get'), getCmd)
  })

  test('should be case-insensitive for command names', () => {
    const registry = new CommandRegistry()
    const getCmd = new GetCommand()

    registry.register(getCmd)
    assert.strictEqual(registry.has('GET'), true)
    assert.strictEqual(registry.has('Get'), true)
    assert.strictEqual(registry.get('GET'), getCmd)
  })

  test('should filter readonly commands', () => {
    const registry = new CommandRegistry()

    registry.registerAll([new GetCommand(), new SetCommand()])

    const readonly = registry.getReadonlyCommands()
    assert.strictEqual(readonly.length, 1)
    assert.strictEqual(readonly[0].metadata.name, 'get')
  })

  test('should filter lua-safe commands', () => {
    const registry = new CommandRegistry()

    registry.registerAll([
      new GetCommand(),
      new RandomkeyCommand(),
      new BlpopCommand(),
    ])

    const luaSafe = registry.getLuaCommands()
    assert.strictEqual(luaSafe.length, 1)
    assert.strictEqual(luaSafe[0].metadata.name, 'get')
  })

  test('should filter multi-safe commands', () => {
    const registry = new CommandRegistry()

    registry.registerAll([
      new GetCommand(),
      new BlpopCommand(),
      new MultiCommand(),
    ])

    const multiSafe = registry.getMultiCommands()
    assert.strictEqual(multiSafe.length, 1)
    assert.strictEqual(multiSafe[0].metadata.name, 'get')
  })

  test('should throw on duplicate registration', () => {
    const registry = new CommandRegistry()

    registry.register(new GetCommand())

    assert.throws(() => {
      registry.register(new GetCommand())
    }, /already registered/)
  })

  test('should filter by category', () => {
    const registry = new CommandRegistry()

    registry.registerAll([new GetCommand(), new HgetCommand()])

    const stringCommands = registry.getByCategory(CommandCategory.STRING)
    assert.strictEqual(stringCommands.length, 1)
    assert.strictEqual(stringCommands[0].metadata.name, 'get')

    const hashCommands = registry.getByCategory(CommandCategory.HASH)
    assert.strictEqual(hashCommands.length, 1)
    assert.strictEqual(hashCommands[0].metadata.name, 'hget')
  })

  test('should convert to record', () => {
    const registry = new CommandRegistry()

    registry.register(new GetCommand())

    const commands = registry.toRecord()
    assert.ok(commands.get)
    assert.ok(typeof commands.get.run === 'function')
  })

  test('should return correct count', () => {
    const registry = new CommandRegistry()

    assert.strictEqual(registry.count(), 0)

    registry.register(new GetCommand())
    assert.strictEqual(registry.count(), 1)
  })

  test('should get all command names', () => {
    const registry = new CommandRegistry()

    registry.registerAll([new GetCommand(), new SetCommand()])

    const names = registry.getNames()
    assert.strictEqual(names.length, 2)
    assert.ok(names.includes('get'))
    assert.ok(names.includes('set'))
  })

  test('should support custom filter predicates', () => {
    const registry = new CommandRegistry()

    registry.registerAll([new FastGetCommand(), new ScanCommand()])

    const fastCommands = registry.filter(
      cmd => cmd.metadata.flags.fast === true,
    )
    assert.strictEqual(fastCommands.length, 1)
    assert.strictEqual(fastCommands[0].metadata.name, 'fastget')
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
