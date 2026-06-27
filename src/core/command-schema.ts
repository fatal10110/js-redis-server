import {
  ExpectedFloatError,
  ExpectedIntegerError,
  RedisCommandError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from './redis-error'
import {
  resolveCompatibilityProfile,
  type CompatibilityProfile,
} from './compatibility'

export type ParseContext = {
  commandName: string
  profile: CompatibilityProfile
}

export type ParseNodeResult<TValue> = {
  value: TValue
  nextIndex: number
}

export interface CommandSchema<TValue> {
  parse(
    input: readonly Buffer[],
    index: number,
    ctx: ParseContext,
  ): ParseNodeResult<TValue>
}

export type InferSchema<TSchema> =
  TSchema extends CommandSchema<infer TValue> ? TValue : never

type SchemaShape = Record<string, CommandSchema<unknown>>

type InferShape<TShape extends SchemaShape> = {
  [K in keyof TShape]: InferSchema<TShape[K]>
}

const INTEGER_TOKEN_PATTERN = /^(0|-?[1-9]\d*)$/
const FLOAT_TOKEN_PATTERN =
  /^[+-]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:[eE][+-]?\d+)?$/

class MissingInputError extends Error {
  constructor() {
    super('missing input')
  }
}

export class SchemaMismatchError extends Error {
  constructor() {
    super('schema mismatch')
  }
}

export function parseCommandArgs<TArgs>(
  schema: CommandSchema<TArgs>,
  input: readonly Buffer[],
  commandName: string,
  profile: CompatibilityProfile = resolveCompatibilityProfile(),
): TArgs {
  try {
    const result = schema.parse(input, 0, { commandName, profile })
    if (result.nextIndex !== input.length) {
      throw new WrongNumberOfArgumentsError(commandName)
    }

    return result.value
  } catch (err) {
    if (err instanceof MissingInputError) {
      throw new WrongNumberOfArgumentsError(commandName)
    }

    if (err instanceof SchemaMismatchError) {
      throw new RedisSyntaxError()
    }

    throw err
  }
}

function readToken(input: readonly Buffer[], index: number): Buffer {
  const token = input[index]
  if (!token) {
    throw new MissingInputError()
  }

  return token
}

function keywordMatches(actual: Buffer, expected: string): boolean {
  return actual.toString().toUpperCase() === expected.toUpperCase()
}

export function isIntegerToken(raw: string): boolean {
  return INTEGER_TOKEN_PATTERN.test(raw)
}

export function parseFiniteFloatToken(raw: string): number | undefined {
  if (!FLOAT_TOKEN_PATTERN.test(raw)) {
    return undefined
  }

  const value = Number(raw)
  if (!Number.isFinite(value)) {
    return undefined
  }

  return value
}

function makeSchema<TValue>(
  parse: CommandSchema<TValue>['parse'],
): CommandSchema<TValue> {
  return { parse }
}

export const t = {
  custom<TValue>(parse: CommandSchema<TValue>['parse']): CommandSchema<TValue> {
    return makeSchema(parse)
  },

  key(): CommandSchema<Buffer> {
    return t.bulk()
  },

  bulk(): CommandSchema<Buffer> {
    return makeSchema((input, index) => ({
      value: readToken(input, index),
      nextIndex: index + 1,
    }))
  },

  string(): CommandSchema<string> {
    return makeSchema((input, index) => ({
      value: readToken(input, index).toString(),
      nextIndex: index + 1,
    }))
  },

  integer(options?: { min?: number; max?: number }): CommandSchema<number> {
    return makeSchema((input, index) => {
      const raw = readToken(input, index).toString()
      if (!isIntegerToken(raw)) {
        throw new ExpectedIntegerError()
      }

      const value = Number(raw)
      if (!Number.isSafeInteger(value)) {
        throw new ExpectedIntegerError()
      }

      if (options?.min !== undefined && value < options.min) {
        throw new ExpectedIntegerError()
      }

      if (options?.max !== undefined && value > options.max) {
        throw new ExpectedIntegerError()
      }

      return { value, nextIndex: index + 1 }
    })
  },

  bigInteger(options?: { min?: bigint; max?: bigint }): CommandSchema<bigint> {
    return makeSchema((input, index) => {
      const raw = readToken(input, index).toString()
      if (!isIntegerToken(raw)) {
        throw new ExpectedIntegerError()
      }

      const value = BigInt(raw)

      if (options?.min !== undefined && value < options.min) {
        throw new ExpectedIntegerError()
      }

      if (options?.max !== undefined && value > options.max) {
        throw new ExpectedIntegerError()
      }

      return { value, nextIndex: index + 1 }
    })
  },

  float(): CommandSchema<number> {
    return makeSchema((input, index) => {
      const value = parseFiniteFloatToken(readToken(input, index).toString())
      if (value === undefined) {
        throw new ExpectedFloatError()
      }

      return { value, nextIndex: index + 1 }
    })
  },

  keyword<TKeyword extends string>(
    expected: TKeyword,
  ): CommandSchema<TKeyword> {
    return makeSchema((input, index) => {
      const token = readToken(input, index)
      if (!keywordMatches(token, expected)) {
        throw new SchemaMismatchError()
      }

      return { value: expected, nextIndex: index + 1 }
    })
  },

  optional<TValue>(
    schema: CommandSchema<TValue>,
  ): CommandSchema<TValue | undefined> {
    return makeSchema((input, index, ctx) => {
      if (index >= input.length) {
        return { value: undefined, nextIndex: index }
      }

      try {
        return schema.parse(input, index, ctx)
      } catch (err) {
        if (err instanceof SchemaMismatchError) {
          return { value: undefined, nextIndex: index }
        }

        throw err
      }
    })
  },

  variadic<TValue>(
    schema: CommandSchema<TValue>,
    options?: { min?: number },
  ): CommandSchema<TValue[]> {
    // Greedy by design for Phase 1. Do not place another positional schema
    // after a variadic field until the parser grows lookahead support.
    return makeSchema((input, index, ctx) => {
      const values: TValue[] = []
      let cursor = index

      while (cursor < input.length) {
        const result = schema.parse(input, cursor, ctx)
        values.push(result.value)
        cursor = result.nextIndex
      }

      if (options?.min !== undefined && values.length < options.min) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      return { value: values, nextIndex: cursor }
    })
  },

  object<TShape extends SchemaShape>(
    shape: TShape,
  ): CommandSchema<InferShape<TShape>> {
    return makeSchema((input, index, ctx) => {
      const value: Partial<InferShape<TShape>> = {}
      let cursor = index
      const fields = Object.entries(shape) as Array<
        [keyof TShape, TShape[keyof TShape]]
      >

      for (const [name, fieldSchema] of fields) {
        const result = fieldSchema.parse(input, cursor, ctx)
        value[name] = result.value as InferShape<TShape>[keyof TShape]
        cursor = result.nextIndex
      }

      return { value: value as InferShape<TShape>, nextIndex: cursor }
    })
  },

  union<TValue>(
    schemas: readonly CommandSchema<TValue>[],
  ): CommandSchema<TValue> {
    return makeSchema((input, index, ctx) => {
      let commandError: RedisCommandError | null = null

      for (const schema of schemas) {
        try {
          return schema.parse(input, index, ctx)
        } catch (err) {
          if (err instanceof SchemaMismatchError) {
            continue
          }

          if (err instanceof RedisCommandError) {
            commandError = err
          }
        }
      }

      if (commandError) {
        throw commandError
      }

      throw new SchemaMismatchError()
    })
  },
}
