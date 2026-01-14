import { SchemaType } from './types'

export interface ParseOptions {
  commandName?: string
}

export interface InputMapper<RawInput> {
  /**
   * Validates and converts raw input into the Schema's expected types.
   */
  parse(schema: SchemaType, input: RawInput, options?: ParseOptions): unknown
}

export interface ArityRange {
  min: number
  max: number
}

export function getSchemaArity(schema: SchemaType): ArityRange {
  switch (schema.type) {
    case 'string':
    case 'integer':
    case 'key':
    case 'flag':
    case 'literal':
      return { min: 1, max: 1 }
    case 'sequence': {
      const itemRange = getSchemaArity(schema.item)
      return {
        min: 1 + itemRange.min,
        max: addMax(1, itemRange.max),
      }
    }
    case 'optional': {
      const itemRange = getSchemaArity(schema.item)
      return { min: 0, max: itemRange.max }
    }
    case 'variadic':
      return { min: 0, max: Number.POSITIVE_INFINITY }
    case 'union': {
      let min = Number.POSITIVE_INFINITY
      let max = 0

      for (const option of schema.options) {
        const range = getSchemaArity(option)
        min = Math.min(min, range.min)
        max =
          max === Number.POSITIVE_INFINITY
            ? Number.POSITIVE_INFINITY
            : range.max === Number.POSITIVE_INFINITY
              ? Number.POSITIVE_INFINITY
              : Math.max(max, range.max)
      }

      if (min === Number.POSITIVE_INFINITY) {
        min = 0
      }

      return { min, max }
    }
    case 'xor': {
      let min = Number.POSITIVE_INFINITY
      let max = 0

      for (const option of schema.options) {
        const range = getSchemaArity(option)
        min = Math.min(min, range.min)
        max =
          max === Number.POSITIVE_INFINITY
            ? Number.POSITIVE_INFINITY
            : range.max === Number.POSITIVE_INFINITY
              ? Number.POSITIVE_INFINITY
              : Math.max(max, range.max)
      }

      if (min === Number.POSITIVE_INFINITY) {
        min = 0
      }

      return { min, max }
    }
    case 'options':
      return { min: 0, max: Number.POSITIVE_INFINITY }
    case 'tuple': {
      let min = 0
      let max = 0

      for (const item of schema.items) {
        const range = getSchemaArity(item)
        min += range.min
        max = addMax(max, range.max)
      }

      return { min, max }
    }
  }
}

function addMax(base: number, next: number): number {
  if (base === Number.POSITIVE_INFINITY || next === Number.POSITIVE_INFINITY) {
    return Number.POSITIVE_INFINITY
  }

  return base + next
}
