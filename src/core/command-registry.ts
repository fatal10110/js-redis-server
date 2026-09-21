import type { CommandDefinition } from './command-definition'

export class CommandRegistry {
  private readonly commands = new Map<string, CommandDefinition<unknown>>()

  /**
   * Registers a definition under its lowercased name.
   *
   * The definition object is stored **by reference**, never copied: a
   * `CommandDefinition` is an interface, so it may legally be a class instance
   * whose `keys`/`execute` live on the prototype, and callers may key
   * side-metadata off the object itself (`weakMap.get(plan.definition)`).
   * A spread here would strip the prototype and break that identity.
   *
   * Only the map key is normalized. The name a definition *carries* is
   * lowercased by {@link defineCommand} — but that returns a copy, with the
   * same prototype-stripping problem, so a class instance belongs here
   * directly rather than routed through it.
   *
   * The tradeoff of leaving the carried name alone: a directly-registered
   * mixed-case definition keeps its casing all the way to the client, so
   * `COMMAND INFO` and arity errors echo `ClassCmd` where real Redis
   * lowercases every built-in name. Normalizing here instead is what broke
   * definition identity, so the split stands.
   */
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
