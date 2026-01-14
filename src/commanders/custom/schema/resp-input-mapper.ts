import {
  ExpectedInteger,
  RedisSyntaxError,
  WrongNumberOfArguments,
} from '../../../core/errors'
import { InputMapper, ParseOptions, getSchemaArity } from './input-mapper'
import { SchemaType } from './types'

type ParseResult = {
  value: unknown
  nextIndex: number
}

class SchemaMismatchError extends Error {
  constructor() {
    super('schema mismatch')
  }
}

class MissingInputError extends Error {
  constructor() {
    super('missing input')
  }
}

export class RespInputMapper implements InputMapper<Buffer[]> {
  parse(schema: SchemaType, input: Buffer[], options?: ParseOptions): unknown {
    const arity = getSchemaArity(schema)

    if (input.length < arity.min) {
      throw this.makeWrongNumberError(options)
    }

    if (arity.max !== Number.POSITIVE_INFINITY && input.length > arity.max) {
      throw this.makeWrongNumberError(options)
    }

    try {
      const result = this.parseNode(schema, input, 0)

      if (result.nextIndex !== input.length) {
        throw this.makeWrongNumberError(options)
      }

      return result.value
    } catch (err) {
      if (err instanceof MissingInputError) {
        throw this.makeWrongNumberError(options)
      }

      if (err instanceof SchemaMismatchError) {
        throw new RedisSyntaxError()
      }

      throw err
    }
  }

  private parseNode(
    schema: SchemaType,
    input: Buffer[],
    index: number,
  ): ParseResult {
    switch (schema.type) {
      case 'string':
        return this.parseString(input, index)
      case 'integer':
        return this.parseInteger(schema, input, index)
      case 'key':
        return this.parseKey(input, index)
      case 'flag':
        return this.parseFlag(schema, input, index)
      case 'literal':
        return this.parseLiteral(schema, input, index)
      case 'sequence':
        return this.parseSequence(schema, input, index)
      case 'union':
        return this.parseUnion(schema, input, index)
      case 'xor':
        return this.parseXor(schema, input, index)
      case 'options':
        return this.parseOptions(schema, input, index)
      case 'tuple':
        return this.parseTuple(schema, input, index)
      case 'variadic':
        return this.parseVariadic(schema, input, index)
      case 'optional':
        return this.parseOptional(schema, input, index)
    }
  }

  private parseString(input: Buffer[], index: number): ParseResult {
    const token = input[index]
    if (!token) {
      throw new MissingInputError()
    }

    return { value: token.toString(), nextIndex: index + 1 }
  }

  private parseKey(input: Buffer[], index: number): ParseResult {
    const token = input[index]
    if (!token) {
      throw new MissingInputError()
    }

    return { value: token, nextIndex: index + 1 }
  }

  private parseInteger(
    schema: Extract<SchemaType, { type: 'integer' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    const token = input[index]
    if (!token) {
      throw new MissingInputError()
    }

    const raw = token.toString()
    if (!/^-?\d+$/.test(raw)) {
      throw new ExpectedInteger()
    }

    const value = Number(raw)

    if (schema.min !== undefined && value < schema.min) {
      throw new ExpectedInteger()
    }

    if (schema.max !== undefined && value > schema.max) {
      throw new ExpectedInteger()
    }

    return { value, nextIndex: index + 1 }
  }

  private parseFlag(
    schema: Extract<SchemaType, { type: 'flag' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    const token = input[index]
    if (!token) {
      throw new MissingInputError()
    }

    const value = token.toString()
    if (!this.matchesKeyword(value, schema.name)) {
      throw new SchemaMismatchError()
    }

    return { value: schema.name, nextIndex: index + 1 }
  }

  private parseLiteral(
    schema: Extract<SchemaType, { type: 'literal' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    const token = input[index]
    if (!token) {
      throw new MissingInputError()
    }

    const value = token.toString()
    if (!this.matchesKeyword(value, schema.value)) {
      throw new SchemaMismatchError()
    }

    return { value: schema.value, nextIndex: index + 1 }
  }

  private parseSequence(
    schema: Extract<SchemaType, { type: 'sequence' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    const token = input[index]
    if (!token) {
      throw new MissingInputError()
    }

    const value = token.toString()
    if (!this.matchesKeyword(value, schema.name)) {
      throw new SchemaMismatchError()
    }

    const next = this.parseNode(schema.item, input, index + 1)
    return {
      value: { type: schema.name, value: next.value },
      nextIndex: next.nextIndex,
    }
  }

  private parseUnion(
    schema: Extract<SchemaType, { type: 'union' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    let storedError: Error | null = null

    for (const option of schema.options) {
      try {
        return this.parseNode(option, input, index)
      } catch (err) {
        if (err instanceof SchemaMismatchError) {
          continue
        }

        if (!storedError && err instanceof Error) {
          storedError = err
        }
      }
    }

    if (storedError) {
      throw storedError
    }

    throw new SchemaMismatchError()
  }

  private parseXor(
    schema: Extract<SchemaType, { type: 'xor' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    const matches: ParseResult[] = []
    let storedError: Error | null = null

    for (const option of schema.options) {
      try {
        matches.push(this.parseNode(option, input, index))
      } catch (err) {
        if (err instanceof SchemaMismatchError) {
          continue
        }

        if (!storedError && err instanceof Error) {
          storedError = err
        }
      }
    }

    if (matches.length > 1) {
      throw new RedisSyntaxError()
    }

    if (matches.length === 1) {
      return matches[0]
    }

    if (storedError) {
      throw storedError
    }

    throw new SchemaMismatchError()
  }

  private parseTuple(
    schema: Extract<SchemaType, { type: 'tuple' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    const values: unknown[] = []
    let cursor = index

    for (let i = 0; i < schema.items.length; i += 1) {
      const item = schema.items[i]

      if (item.type === 'variadic') {
        if (i !== schema.items.length - 1) {
          throw new RedisSyntaxError()
        }

        const variadic = this.parseVariadic(item, input, cursor)
        values.push(variadic.value)
        cursor = variadic.nextIndex
        break
      }

      const parsed = this.parseNode(item, input, cursor)
      values.push(parsed.value)
      cursor = parsed.nextIndex
    }

    return { value: values, nextIndex: cursor }
  }

  private parseOptions(
    schema: Extract<SchemaType, { type: 'options' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    const values: Record<string, unknown> = {}
    let cursor = index
    const fields = Object.entries(schema.fields)

    while (cursor < input.length) {
      const matches: Array<{ name: string; result: ParseResult }> = []
      let storedError: Error | null = null

      for (const [name, fieldSchema] of fields) {
        try {
          const result = this.parseNode(fieldSchema, input, cursor)
          if (Object.prototype.hasOwnProperty.call(values, name)) {
            throw new RedisSyntaxError()
          }
          matches.push({ name, result })
        } catch (err) {
          if (err instanceof SchemaMismatchError) {
            continue
          }

          if (!storedError && err instanceof Error) {
            storedError = err
          }
        }
      }

      if (matches.length > 1) {
        throw new RedisSyntaxError()
      }

      if (matches.length === 1) {
        const match = matches[0]
        values[match.name] = match.result.value
        cursor = match.result.nextIndex
        continue
      }

      if (storedError) {
        throw storedError
      }

      throw new RedisSyntaxError()
    }

    return { value: values, nextIndex: cursor }
  }

  private parseVariadic(
    schema: Extract<SchemaType, { type: 'variadic' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    const values: unknown[] = []
    let cursor = index

    while (cursor < input.length) {
      const parsed = this.parseNode(schema.item, input, cursor)
      values.push(parsed.value)
      cursor = parsed.nextIndex
    }

    return { value: values, nextIndex: cursor }
  }

  private parseOptional(
    schema: Extract<SchemaType, { type: 'optional' }>,
    input: Buffer[],
    index: number,
  ): ParseResult {
    if (index >= input.length) {
      return { value: undefined, nextIndex: index }
    }

    try {
      return this.parseNode(schema.item, input, index)
    } catch (err) {
      if (err instanceof SchemaMismatchError) {
        return { value: undefined, nextIndex: index }
      }

      if (err instanceof MissingInputError) {
        throw new RedisSyntaxError()
      }

      throw err
    }
  }

  private matchesKeyword(value: string, expected: string): boolean {
    return value.toUpperCase() === expected.toUpperCase()
  }

  private makeWrongNumberError(options?: ParseOptions): Error {
    if (options?.commandName) {
      return new WrongNumberOfArguments(options.commandName)
    }

    return new RedisSyntaxError()
  }
}
