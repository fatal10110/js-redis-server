import type { Command, CommandResult, ExecutionContext } from '../../../types'
import type { CommandMetadata } from '../commands/metadata'
import type { DB } from '../db'
import type { SchemaType } from './types'
import type { InputMapper } from './input-mapper'
import { RespInputMapper } from './resp-input-mapper'
import { LuaEngine } from 'wasmoon'

export interface SchemaCommandContext {
  db: DB
  luaEngine?: LuaEngine
  discoveryService?: unknown
  mySelfId?: string
  executionContext?: ExecutionContext
  commands?: Record<string, Command>
  signal: AbortSignal
}

export interface SchemaCommandRegistration<TArgs = unknown> {
  metadata: CommandMetadata
  schema: SchemaType
  handler: (args: TArgs, ctx: SchemaCommandContext) => Promise<unknown>
  getKeys?: (rawCmd: Buffer, args: Buffer[]) => Buffer[]
}

export function createSchemaCommand(
  definition: SchemaCommandRegistration<any>,
  ctx: Omit<SchemaCommandContext, 'signal'>,
  mapper: InputMapper<Buffer[]> = new RespInputMapper(),
): Command {
  return new SchemaCommandAdapter(definition, ctx, mapper)
}

class SchemaCommandAdapter implements Command {
  readonly metadata: CommandMetadata

  constructor(
    private readonly definition: SchemaCommandRegistration<any>,
    private readonly ctx: Omit<SchemaCommandContext, 'signal'>,
    private readonly mapper: InputMapper<Buffer[]>,
  ) {
    this.metadata = definition.metadata
  }

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (this.definition.getKeys) {
      return this.definition.getKeys(_rawCmd, args)
    }

    const { firstKey, lastKey, keyStep } = this.metadata

    if (firstKey < 0 || args.length === 0) {
      return []
    }

    const resolvedLast = lastKey < 0 ? args.length + lastKey : lastKey
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

  async run(
    _rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<CommandResult> {
    const parsed = this.mapper.parse(this.definition.schema, args, {
      commandName: this.metadata.name,
    })
    const result = await this.definition.handler(parsed, {
      ...this.ctx,
      signal,
    })

    return normalizeResult(result)
  }
}

function normalizeResult(result: unknown): CommandResult {
  if (result && typeof result === 'object' && 'response' in result) {
    return result as CommandResult
  }

  return { response: result }
}
