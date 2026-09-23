import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
  errors,
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

/**
 * Static token layout of a schema node — what `COMMAND INFO` needs to report
 * arity and the legacy first/last/step key positions without a hand-written
 * copy (#370). Offsets are 0-based from the node's first token.
 */
export type SchemaLayout = {
  /** Fewest tokens the node consumes. */
  readonly min: number
  /** Most tokens the node consumes; `Infinity` when unbounded. */
  readonly max: number
  /** Offsets of key tokens that sit at a fixed position within the node. */
  readonly keys: readonly number[]
  /**
   * Keys repeating every `step` tokens from offset `start` through to the end
   * of the command. `last` counts back from the final token like Redis's
   * `lastkey` (-1 = the final token, -2 = the one before it).
   */
  readonly keyRange?: KeyRangeLayout
}

export type KeyRangeLayout = {
  readonly start: number
  readonly step: number
  readonly last: number
}

export interface CommandSchema<TValue> {
  parse(
    input: readonly Buffer[],
    index: number,
    ctx: ParseContext,
  ): ParseNodeResult<TValue>
  /**
   * Every `t` builder sets this. A schema written by hand as `{ parse }` has
   * none and counts as opaque: any number of tokens, no known key positions
   * (`COMMAND INFO` arity -1, key range 0 0 0).
   */
  readonly layout?: SchemaLayout
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
      throw errors.syntax()
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

const TOKEN_LAYOUT: SchemaLayout = { min: 1, max: 1, keys: [] }
const KEY_LAYOUT: SchemaLayout = { min: 1, max: 1, keys: [0] }
const OPAQUE_LAYOUT: SchemaLayout = { min: 0, max: Infinity, keys: [] }

function makeSchema<TValue>(
  parse: CommandSchema<TValue>['parse'],
  layout: SchemaLayout = TOKEN_LAYOUT,
): CommandSchema<TValue> {
  return { parse, layout }
}

/** `schema`'s layout, or the opaque default for a hand-built `{ parse }`. */
export function schemaLayout(schema: CommandSchema<unknown>): SchemaLayout {
  return schema.layout ?? OPAQUE_LAYOUT
}

/**
 * `COMMAND INFO` arity of a command whose arguments follow `schema`: the
 * token count including the command name, negated when it is only a minimum.
 */
export function schemaArity(schema: CommandSchema<unknown>): number {
  const { min, max } = schemaLayout(schema)
  return min === max ? min + 1 : -(min + 1)
}

/**
 * Redis's legacy `first/last/step` key range for `schema`, counted from the
 * command name at index 0. Like Redis it covers only the leading run of
 * consecutive fixed-position keys, extended by a key range that continues it;
 * keys after a gap or a variable-width argument are left to key specs.
 */
export function schemaKeyRange(schema: CommandSchema<unknown>): {
  firstKey: number
  lastKey: number
  keyStep: number
} {
  const { keys, keyRange } = schemaLayout(schema)
  if (keys.length > 0) {
    let last = keys[0]
    while (keys.includes(last + 1)) {
      last++
    }

    if (keyRange?.step === 1 && keyRange.start === last + 1) {
      return { firstKey: keys[0] + 1, lastKey: keyRange.last, keyStep: 1 }
    }

    return { firstKey: keys[0] + 1, lastKey: last + 1, keyStep: 1 }
  }

  if (keyRange) {
    return {
      firstKey: keyRange.start + 1,
      lastKey: keyRange.last,
      keyStep: keyRange.step,
    }
  }

  return { firstKey: 0, lastKey: 0, keyStep: 0 }
}

/**
 * A hand-written parser. Its token layout is opaque, so a parser that stands
 * for whole arguments declares whatever of it is static — at least `min` —
 * or `COMMAND INFO` under-reports the command's arity. Without a layout it
 * counts as zero or more tokens holding no keys.
 */
function custom<TValue>(
  parse: CommandSchema<TValue>['parse'],
): CommandSchema<TValue>
function custom<TValue>(
  layout: Partial<SchemaLayout>,
  parse: CommandSchema<TValue>['parse'],
): CommandSchema<TValue>
function custom<TValue>(
  layoutOrParse: Partial<SchemaLayout> | CommandSchema<TValue>['parse'],
  maybeParse?: CommandSchema<TValue>['parse'],
): CommandSchema<TValue> {
  const layout = typeof layoutOrParse === 'function' ? {} : layoutOrParse
  const parse = typeof layoutOrParse === 'function' ? layoutOrParse : maybeParse
  if (!parse) {
    throw new TypeError('t.custom needs a parse function')
  }

  return makeSchema(parse, declaredLayout(OPAQUE_LAYOUT, layout))
}

/**
 * `base` with `declared` laid over it, rejected unless it is coherent: key
 * offsets ascending, unique and within the guaranteed `min` tokens, and a key
 * range that starts at or after 0, steps forward and ends relative to the end.
 */
function declaredLayout(
  base: SchemaLayout,
  declared: Partial<SchemaLayout>,
): SchemaLayout {
  const layout = { ...base, ...declared }
  const { min, max, keys, keyRange } = layout
  const fail = (reason: string): never => {
    throw new TypeError(`Invalid schema layout: ${reason}`)
  }

  if (!Number.isInteger(min) || min < 0 || max < min) {
    fail(`min ${min} / max ${max}`)
  }

  for (const [i, offset] of keys.entries()) {
    if (!Number.isInteger(offset) || offset < 0 || offset >= min) {
      fail(`key offset ${offset} is outside the ${min} guaranteed tokens`)
    }

    if (i > 0 && offset <= keys[i - 1]) {
      fail('key offsets must be ascending and unique')
    }
  }

  if (
    keyRange &&
    (keyRange.start < 0 || keyRange.step < 1 || keyRange.last >= 0)
  ) {
    fail(`key range ${JSON.stringify(keyRange)}`)
  }

  return layout
}

export const t = {
  custom,

  /** `schema` with its layout declared where it is used — see `custom`. */
  withLayout<TValue>(
    schema: CommandSchema<TValue>,
    layout: Partial<SchemaLayout>,
  ): CommandSchema<TValue> {
    return makeSchema(
      schema.parse,
      declaredLayout(schemaLayout(schema), layout),
    )
  },

  key(): CommandSchema<Buffer> {
    return makeSchema(t.bulk().parse, KEY_LAYOUT)
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
        throw errors.expectedInteger()
      }

      const value = Number(raw)
      if (!Number.isSafeInteger(value)) {
        throw errors.expectedInteger()
      }

      if (options?.min !== undefined && value < options.min) {
        throw errors.expectedInteger()
      }

      if (options?.max !== undefined && value > options.max) {
        throw errors.expectedInteger()
      }

      return { value, nextIndex: index + 1 }
    })
  },

  bigInteger(options?: { min?: bigint; max?: bigint }): CommandSchema<bigint> {
    return makeSchema((input, index) => {
      const raw = readToken(input, index).toString()
      if (!isIntegerToken(raw)) {
        throw errors.expectedInteger()
      }

      const value = BigInt(raw)

      if (options?.min !== undefined && value < options.min) {
        throw errors.expectedInteger()
      }

      if (options?.max !== undefined && value > options.max) {
        throw errors.expectedInteger()
      }

      return { value, nextIndex: index + 1 }
    })
  },

  float(): CommandSchema<number> {
    return makeSchema((input, index) => {
      const value = parseFiniteFloatToken(readToken(input, index).toString())
      if (value === undefined) {
        throw errors.expectedFloat()
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
    return makeSchema(
      (input, index, ctx) => {
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
      },
      // A key that may be absent is outside Redis's legacy key range.
      { min: 0, max: schemaLayout(schema).max, keys: [] },
    )
  },

  variadic<TValue>(
    schema: CommandSchema<TValue>,
    options?: { min?: number },
  ): CommandSchema<TValue[]> {
    // Greedy by design for Phase 1. Do not place another positional schema
    // after a variadic field until the parser grows lookahead support.
    return makeSchema(
      (input, index, ctx) => {
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
      },
      variadicLayout(schemaLayout(schema), options?.min ?? 0),
    )
  },

  object<TShape extends SchemaShape>(
    shape: TShape,
  ): CommandSchema<InferShape<TShape>> {
    return makeSchema(
      (input, index, ctx) => {
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
      },
      objectLayout(Object.values(shape).map(schemaLayout)),
    )
  },

  union<TValue>(
    schemas: readonly CommandSchema<TValue>[],
  ): CommandSchema<TValue> {
    return makeSchema(
      (input, index, ctx) => {
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
      },
      unionLayout(schemas.map(schemaLayout)),
    )
  },
}

function variadicLayout(item: SchemaLayout, minItems: number): SchemaLayout {
  const layout = { min: item.min * minItems, max: Infinity, keys: [] }
  // An item with its own open-ended key range cannot repeat as one range.
  if (item.min !== item.max || item.keys.length === 0 || item.keyRange) {
    return layout
  }

  // One key per item repeats once per item; items made only of keys form one
  // contiguous run. Any other mix has no single range.
  if (item.keys.length === 1) {
    return {
      ...layout,
      keyRange: { start: item.keys[0], step: item.min, last: -1 },
    }
  }

  if (item.keys.length === item.min) {
    return { ...layout, keyRange: { start: 0, step: 1, last: -1 } }
  }

  return layout
}

function objectLayout(fields: readonly SchemaLayout[]): SchemaLayout {
  let min = 0
  let max = 0
  const keys: number[] = []
  let keyRange: KeyRangeLayout | undefined
  // Positions stay known only while every earlier field is fixed-width.
  let fixed = true

  for (const field of fields) {
    // A range's `last` counts back from the end of the command, so any token
    // a later field may take would shift it: the range no longer holds.
    if (keyRange && field.max > 0) {
      keyRange = undefined
      fixed = false
    }

    if (fixed) {
      keys.push(...field.keys.map(offset => min + offset))
      if (field.keyRange) {
        keyRange = { ...field.keyRange, start: min + field.keyRange.start }
      }
    }

    fixed = fixed && field.min === field.max
    min += field.min
    max += field.max
  }

  return { min, max, keys, keyRange }
}

function unionLayout(branches: readonly SchemaLayout[]): SchemaLayout {
  const [first, ...rest] = branches
  if (!first) {
    return { min: 0, max: 0, keys: [] }
  }

  const sharedRange = rest.every(
    branch =>
      branch.keyRange?.start === first.keyRange?.start &&
      branch.keyRange?.step === first.keyRange?.step &&
      branch.keyRange?.last === first.keyRange?.last,
  )
  return {
    min: Math.min(...branches.map(branch => branch.min)),
    max: Math.max(...branches.map(branch => branch.max)),
    keys: first.keys.filter(offset =>
      rest.every(branch => branch.keys.includes(offset)),
    ),
    keyRange: sharedRange ? first.keyRange : undefined,
  }
}
