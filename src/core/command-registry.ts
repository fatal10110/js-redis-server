import type { CommandDefinition } from './command-definition'

export class CommandRegistry {
  private readonly commands = new Map<string, CommandDefinition<unknown>>()

  register<TArgs>(
    definition: CommandDefinition<TArgs>,
    options?: { override?: boolean },
  ): void {
    const name = definition.name.toLowerCase()
    if (!options?.override && this.commands.has(name)) {
      throw new Error(`Command '${name}' is already registered`)
    }

    this.commands.set(name, definition as CommandDefinition<unknown>)
  }

  override<TArgs>(definition: CommandDefinition<TArgs>): void {
    this.register(definition, { override: true })
  }

  registerAll(
    definitions: readonly CommandDefinition<unknown>[],
    options?: { override?: boolean },
  ): void {
    for (const definition of definitions) {
      this.register(definition, options)
    }
  }

  get(name: string): CommandDefinition<unknown> | undefined {
    return this.commands.get(name.toLowerCase())
  }

  has(name: string): boolean {
    return this.commands.has(name.toLowerCase())
  }

  getAll(): CommandDefinition<unknown>[] {
    return Array.from(this.commands.values())
  }

  getNames(): string[] {
    return Array.from(this.commands.keys())
  }
}
