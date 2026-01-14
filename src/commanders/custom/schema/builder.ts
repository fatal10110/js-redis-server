import { SchemaCommandDefinition, SchemaType } from './types'

export const t = {
  string(): SchemaType {
    return { type: 'string' }
  },
  integer(options?: { min?: number; max?: number }): SchemaType {
    return { type: 'integer', ...options }
  },
  key(): SchemaType {
    return { type: 'key' }
  },
  flag(name: string): SchemaType {
    return { type: 'flag', name }
  },
  literal(value: string): SchemaType {
    return { type: 'literal', value }
  },
  sequence(name: string, item: SchemaType): SchemaType {
    return { type: 'sequence', name, item }
  },
  named(name: string, item: SchemaType): SchemaType {
    return { type: 'sequence', name, item }
  },
  union(options: SchemaType[]): SchemaType {
    return { type: 'union', options }
  },
  xor(options: SchemaType[]): SchemaType {
    return { type: 'xor', options }
  },
  options(fields: Record<string, SchemaType>): SchemaType {
    return { type: 'options', fields }
  },
  tuple(items: SchemaType[]): SchemaType {
    return { type: 'tuple', items }
  },
  variadic(item: SchemaType): SchemaType {
    return { type: 'variadic', item }
  },
  optional(item: SchemaType): SchemaType {
    return { type: 'optional', item }
  },
}

export function cmd<TArgs = unknown, TContext = unknown>(
  name: string,
  definition: {
    schema: SchemaType
    handler: (args: TArgs, ctx: TContext) => Promise<unknown>
  },
): SchemaCommandDefinition<TArgs, TContext> {
  return {
    name: name.toLowerCase(),
    schema: definition.schema,
    handler: definition.handler,
  }
}
