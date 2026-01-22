import type { Command } from '../../../types'
import type { CommandCategory } from './metadata'
import type { SchemaCommand } from '../schema/schema-command'

export class CommandRegistry {
  private commands = new Map<string, SchemaCommand<any>>()

  register(command: SchemaCommand<any>): void {
    const name = command.metadata.name.toLowerCase()

    if (this.commands.has(name)) {
      throw new Error(`Command '${name}' is already registered`)
    }

    this.commands.set(name, command)
  }

  registerAll(commands: SchemaCommand<any>[]): void {
    commands.forEach(cmd => this.register(cmd))
  }

  get(name: string): SchemaCommand<any> | undefined {
    return this.commands.get(name.toLowerCase())
  }

  has(name: string): boolean {
    return this.commands.has(name.toLowerCase())
  }

  getAll(): SchemaCommand<any>[] {
    return Array.from(this.commands.values())
  }

  getNames(): string[] {
    return Array.from(this.commands.keys())
  }

  getReadonlyCommands(): SchemaCommand<any>[] {
    return this.getAll().filter(cmd => cmd.metadata.flags.readonly === true)
  }

  getMultiCommands(): SchemaCommand<any>[] {
    return this.getAll().filter(cmd => {
      const { flags } = cmd.metadata
      return !flags.blocking && !flags.transaction && !flags.noscript
    })
  }

  getLuaCommands(): SchemaCommand<any>[] {
    return this.getAll().filter(cmd => {
      const { flags } = cmd.metadata
      return !flags.random && !flags.blocking && !flags.noscript && !flags.admin
    })
  }

  getByCategory(category: CommandCategory): SchemaCommand<any>[] {
    return this.getAll().filter(cmd =>
      cmd.metadata.categories.includes(category),
    )
  }

  filter(
    predicate: (cmd: SchemaCommand<any>) => boolean,
  ): SchemaCommand<any>[] {
    return this.getAll().filter(predicate)
  }

  toRecord(): Record<string, Command> {
    const commands: Record<string, Command> = {}
    this.commands.forEach((command, name) => {
      commands[name] = command
    })
    return commands
  }

  count(): number {
    return this.commands.size
  }
}

export const globalCommandRegistry = new CommandRegistry()
