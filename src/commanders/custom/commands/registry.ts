import type {
  Command,
  DiscoveryService,
  ExecutionContext,
} from '../../../types'
import type { CommandCategory } from './metadata'
import type { DB } from '../db'
import type { LuaRuntime } from '../lua-runtime'
import type { InputMapper } from '../schema'
import { createSchemaCommand, SchemaCommandRegistration } from '../schema'

/**
 * Dependencies passed to command factories
 */
export interface CommandDependencies {
  db: DB
  discoveryService?: DiscoveryService
  mySelfId?: string
  executionContext?: ExecutionContext
  commands?: Record<string, Command>
  luaRuntime?: LuaRuntime
}

/**
 * Central command registry
 */
export class CommandRegistry {
  private commands = new Map<
    string,
    {
      definition: SchemaCommandRegistration<any>
      mapper?: InputMapper<Buffer[]>
    }
  >()

  /**
   * Register a command
   */
  register(
    definition: SchemaCommandRegistration<any>,
    mapper?: InputMapper<Buffer[]>,
  ): void {
    const name = definition.metadata.name.toLowerCase()

    if (this.commands.has(name)) {
      throw new Error(`Command '${name}' is already registered`)
    }

    this.commands.set(name, { definition, mapper })
  }

  /**
   * Register multiple commands at once
   */
  registerAll(
    definitions: Array<SchemaCommandRegistration<any>>,
    mapper?: InputMapper<Buffer[]>,
  ): void {
    definitions.forEach(def => this.register(def, mapper))
  }

  /**
   * Get command definition by name
   */
  get(name: string): SchemaCommandRegistration<any> | undefined {
    return this.commands.get(name.toLowerCase())?.definition
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
  getAll(): SchemaCommandRegistration<any>[] {
    return Array.from(this.commands.values()).map(entry => entry.definition)
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
  getReadonlyCommands(): SchemaCommandRegistration<any>[] {
    return this.getAll().filter(def => def.metadata.flags.readonly === true)
  }

  /**
   * Get commands allowed in MULTI/EXEC transactions
   * (excludes blocking, transaction control, etc.)
   */
  getMultiCommands(): SchemaCommandRegistration<any>[] {
    return this.getAll().filter(def => {
      const { flags } = def.metadata
      return !flags.blocking && !flags.transaction && !flags.noscript
    })
  }

  /**
   * Get commands allowed in Lua scripts
   * (excludes random, blocking, noscript, admin)
   */
  getLuaCommands(): SchemaCommandRegistration<any>[] {
    return this.getAll().filter(def => {
      const { flags } = def.metadata
      return !flags.random && !flags.blocking && !flags.noscript && !flags.admin
    })
  }

  /**
   * Filter commands by category
   */
  getByCategory(category: CommandCategory): SchemaCommandRegistration<any>[] {
    return this.getAll().filter(def =>
      def.metadata.categories.includes(category),
    )
  }

  /**
   * Filter commands by custom predicate
   */
  filter(
    predicate: (def: SchemaCommandRegistration<any>) => boolean,
  ): SchemaCommandRegistration<any>[] {
    return this.getAll().filter(predicate)
  }

  /**
   * Create command instances from registry
   * (Legacy compatibility with Record<string, Command> pattern)
   */
  createCommands(deps: CommandDependencies): Record<string, Command> {
    const commands: Record<string, Command> = {}

    this.commands.forEach(({ definition, mapper }) => {
      commands[definition.metadata.name] = createSchemaCommand(
        definition,
        deps,
        mapper,
      )
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
