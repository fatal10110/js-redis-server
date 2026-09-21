import type { CommandDefinition } from './command-definition'

/**
 * The single place a command name is normalized. A definition may declare any
 * casing and a lookup may use any casing; everything downstream of
 * {@link CommandRegistry.register} — `plan.definition.name`, the policies that
 * match on it, COMMAND introspection — sees the lowercase form.
 */
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

    const normalized =
      definition.name === name ? definition : { ...definition, name }

    this.commands.set(name, normalized as CommandDefinition<unknown>)
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

  getAll(): CommandDefinition<unknown>[] {
    return Array.from(this.commands.values())
  }
}
