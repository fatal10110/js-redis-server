import type { Command } from '../../../types'
import type { CommandMetadata, CommandCategory } from './metadata'
import type { DB } from '../db'
import { LuaEngine } from 'wasmoon'

/**
 * Dependencies passed to command factories
 */
export interface CommandDependencies {
  db: DB
  luaEngine?: LuaEngine
  discoveryService?: any // For cluster commands
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
      return !flags.blocking && !flags.transaction && !flags.noscript
    })
  }

  /**
   * Get commands allowed in Lua scripts
   * (excludes random, blocking, noscript, admin)
   */
  getLuaCommands(): CommandDefinition[] {
    return this.getAll().filter(def => {
      const { flags } = def.metadata
      return !flags.random && !flags.blocking && !flags.noscript && !flags.admin
    })
  }

  /**
   * Filter commands by category
   */
  getByCategory(category: CommandCategory): CommandDefinition[] {
    return this.getAll().filter(def =>
      def.metadata.categories.includes(category),
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
