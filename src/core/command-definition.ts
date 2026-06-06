import type { CommandSchema } from './command-schema'
import type { RedisExecutionContext } from './redis-context'
import type { RedisResult } from './redis-result'
import type { ResponseStream } from './response-stream'

export type CommandFlag =
  | 'readonly'
  | 'write'
  | 'denyoom'
  | 'admin'
  | 'noscript'
  | 'random'
  | 'blocking'
  | 'fast'
  | 'movablekeys'
  | 'transaction'
  | 'pubsub'
  | 'subscribed'

export type CommandCapabilities = {
  blocking?: boolean
  pushOnly?: boolean
  movableKeys?: boolean
  scriptKeys?: boolean
}

export type CommandExecutionResult =
  | RedisResult
  | Promise<RedisResult>
  | ResponseStream

export interface CommandDefinition<TArgs = unknown> {
  readonly name: string
  readonly schema: CommandSchema<TArgs>
  readonly flags: readonly CommandFlag[]
  readonly capabilities?: CommandCapabilities
  keys(args: TArgs): readonly Buffer[]
  execute(args: TArgs, ctx: RedisExecutionContext): CommandExecutionResult
}

export type CommandPlan<TArgs = unknown> = {
  definition: CommandDefinition<TArgs>
  args: TArgs
  keys: readonly Buffer[]
  flags: readonly CommandFlag[]
}

export function defineCommand<TArgs>(
  definition: CommandDefinition<TArgs>,
): CommandDefinition<TArgs> {
  return {
    ...definition,
    name: definition.name.toLowerCase(),
  }
}
