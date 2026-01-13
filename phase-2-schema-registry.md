# Phase 2: Protocol-Agnostic Schema Registry

## Objective

Decouple command validation from the RESP protocol. The schema parser should define the _logical_ structure of a command, allowing different transports (RESP2, RESP3, JSON/HTTP) to map their inputs to this schema.

## The Problem

Currently, commands manually parse `Buffer[]`. This couples every command to the RESP wire format. If we wanted to add an HTTP endpoint or a CLI parser, we would have to mock RESP buffers.

## The Solution: Abstract Schema Definitions

We will create a declarative schema builder that defines types abstractly.

### 2.1 Schema Types

The schema does not know about "Buffers". It knows about "Strings", "Integers", "Keys", and "Flags".

```typescript
export type SchemaType =
  | { type: 'string' }
  | { type: 'integer'; min?: number; max?: number }
  | { type: 'key' }
  | { type: 'flag'; name: string }
  | { type: 'union'; options: SchemaType[] }
  | { type: 'tuple'; items: SchemaType[] }
  | { type: 'variadic'; item: SchemaType }

export interface CommandDefinition {
  name: string
  schema: SchemaType
  handler: (args: any, ctx: Context) => Promise<any>
}
```

### 2.2 Example Command Definition

```typescript
import { cmd, t } from './schema-builder'

export const SetCommand = cmd('SET', {
  schema: t.tuple([
    t.key(), // Arg 0
    t.string(), // Arg 1
    t.optional(
      t.union([
        // Arg 2 (Options)
        t.sequence('EX', t.integer()),
        t.sequence('PX', t.integer()),
        t.literal('NX'),
        t.literal('XX'),
      ]),
    ),
  ]),
  handler: async ([key, value, options], { db }) => {
    // Types are inferred automatically
    // options is { type: 'EX', value: number } | 'NX' | ...
    await db.set(key, value, options)
  },
})
```

### 2.3 The Parser Interface (Decoupling Layer)

To support different implementations (parsers), we introduce an `InputMapper`.

```typescript
interface InputMapper<RawInput> {
  /**
   * Validates and converts raw input into the Schema's expected types.
   */
  parse(schema: SchemaType, input: RawInput): any
}
```

#### Implementation A: RESP Mapper

Handles positional arguments and Buffer conversion.

```typescript
class RespInputMapper implements InputMapper<Buffer[]> {
  parse(schema: SchemaType, input: Buffer[]): any {
    // 1. Check arity
    // 2. Convert Buffers to strings/numbers based on schema.type
    // 3. Handle flags (e.g. find 'EX' in the array)
    // 4. Return typed object/array
  }
}
```

#### Implementation B: JSON/HTTP Mapper

Handles named or positional JSON arguments.

```typescript
class JsonInputMapper implements InputMapper<any> {
  parse(schema: SchemaType, input: any): any {
    // Input might be { key: "foo", value: "bar", ttl: 10 }
    // Map JSON fields to Schema positions
  }
}
```

### 2.4 Benefits

1.  **Validation Logic is Centralized:** The `InputMapper` handles type checking (Is this an integer?). The command handler just gets a number.
2.  **Multi-Protocol:** We can expose the same `SetCommand` over HTTP/JSON without changing a single line of the command implementation.
3.  **Introspection:** `COMMAND DOCS` can be generated directly from the schema tree.
