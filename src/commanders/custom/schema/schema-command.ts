import type {
  Command,
  CommandContext as BaseCommandContext,
  CommandResult,
  ExecutionContext,
  Transport,
} from '../../../types'
import type { CommandMetadata } from '../commands/metadata'
import type { SchemaType } from './types'
import type { CompiledSchema, InputMapper } from './input-mapper'
import { compileSchema } from './input-mapper'
import { RespInputMapper } from './resp-input-mapper'

export interface CommandContext {
  executionContext?: ExecutionContext
  commands?: Record<string, Command>
  luaCommands?: Record<string, Command>
  signal: AbortSignal
  transport: Transport
}

export abstract class SchemaCommand<TArgs = unknown> implements Command {
  abstract readonly metadata: CommandMetadata
  protected abstract readonly schema: SchemaType

  private _compiledSchema: CompiledSchema | null = null
  private readonly mapper: InputMapper<Buffer[]>

  constructor(mapper: InputMapper<Buffer[]> = new RespInputMapper()) {
    this.mapper = mapper
  }

  protected get compiledSchema(): CompiledSchema {
    if (!this._compiledSchema) {
      this._compiledSchema = compileSchema(this.schema)
    }
    return this._compiledSchema
  }

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
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
    ctx: BaseCommandContext,
  ): CommandResult | void {
    const parsed = this.mapper.parse(this.compiledSchema, args, {
      commandName: this.metadata.name,
    })
    return this.execute(parsed as TArgs, ctx as CommandContext)
  }

  protected abstract execute(
    args: TArgs,
    ctx: CommandContext,
  ): CommandResult | void
}
