import type {
  Command,
  CommandResult,
  ExecutionContext,
  Transport,
} from '../../../types'
import type { CommandMetadata } from '../commands/metadata'
import type { DB } from '../db'
import type { LuaRuntime } from '../lua-runtime'
import type { SchemaType } from './types'
import type { CompiledSchema, InputMapper } from './input-mapper'
import { compileSchema } from './input-mapper'
import { RespInputMapper } from './resp-input-mapper'

export interface SchemaCommandContext {
  db: DB
  discoveryService?: unknown
  mySelfId?: string
  executionContext?: ExecutionContext
  commands?: Record<string, Command>
  luaRuntime?: LuaRuntime
  signal: AbortSignal
  transport: Transport
}

export interface SchemaCommandRegistration<TArgs = unknown> {
  metadata: CommandMetadata
  schema: SchemaType
  handler: (args: TArgs, ctx: SchemaCommandContext) => CommandResult | void
  getKeys?: (rawCmd: Buffer, args: Buffer[]) => Buffer[]
}

export function createSchemaCommand(
  definition: SchemaCommandRegistration<any>,
  ctx: Omit<SchemaCommandContext, 'signal' | 'transport'>,
  mapper: InputMapper<Buffer[]> = new RespInputMapper(),
): Command {
  return new SchemaCommandAdapter(definition, ctx, mapper)
}

class SchemaCommandAdapter implements Command {
  readonly metadata: CommandMetadata
  private readonly compiledSchema: CompiledSchema

  constructor(
    private readonly definition: SchemaCommandRegistration<any>,
    private readonly ctx: Omit<SchemaCommandContext, 'signal' | 'transport'>,
    private readonly mapper: InputMapper<Buffer[]>,
  ) {
    this.metadata = definition.metadata
    this.compiledSchema = compileSchema(definition.schema)
  }

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (this.definition.getKeys) {
      return this.definition.getKeys(_rawCmd, args)
    }

    const { firstKey, lastKey, keyStep, limit } = this.metadata

    if (firstKey < 0 || args.length === 0) {
      return []
    }

    let resolvedLast = lastKey < 0 ? args.length + lastKey : lastKey
    if (lastKey === -1 && limit > 1) {
      const remaining = Math.max(args.length - firstKey, 0)
      const keyCount = Math.floor(remaining / limit)
      if (keyCount <= 0) {
        return []
      }
      resolvedLast = firstKey + (keyCount - 1) * keyStep
    }
    const end = Math.min(resolvedLast, args.length - 1)

    if (end < firstKey) {
      return []
    }

    const keys: Buffer[] = []
    for (let i = firstKey; i <= end; i += keyStep) {
      const key = args[i]
      if (key) {
        keys.push(key)
      }
    }

    return keys
  }

  run(
    _rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
    transport: Transport,
  ): CommandResult | void {
    const parsed = this.mapper.parse(this.compiledSchema, args, {
      commandName: this.metadata.name,
    })
    const result = this.definition.handler(parsed, {
      ...this.ctx,
      signal,
      transport,
    })

    return result
  }
}
