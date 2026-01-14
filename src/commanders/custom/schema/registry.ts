import { SchemaCommandDefinition } from './types'

export class SchemaRegistry {
  private commands = new Map<string, SchemaCommandDefinition>()

  register(definition: SchemaCommandDefinition): void {
    const name = definition.name.toLowerCase()

    if (this.commands.has(name)) {
      throw new Error(`Schema command '${name}' is already registered`)
    }

    this.commands.set(name, definition)
  }

  registerAll(definitions: SchemaCommandDefinition[]): void {
    definitions.forEach(def => this.register(def))
  }

  get(name: string): SchemaCommandDefinition | undefined {
    return this.commands.get(name.toLowerCase())
  }

  has(name: string): boolean {
    return this.commands.has(name.toLowerCase())
  }

  getAll(): SchemaCommandDefinition[] {
    return Array.from(this.commands.values())
  }

  getNames(): string[] {
    return Array.from(this.commands.keys())
  }

  count(): number {
    return this.commands.size
  }
}

export const globalSchemaRegistry = new SchemaRegistry()
