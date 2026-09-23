import type { CommandSchema } from './command-schema'
import type { RedisExecutionContext } from './redis-context'
import type { RedisResult } from './redis-result'
import type { ResponseStream } from './response-stream'
import type { VersionGate } from './compatibility'
import { asciiLowerCase } from './ascii-case'

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
  /**
   * How the command behaves under cluster mode. `'forbidden'` is always
   * rejected; `'singleDb'` is rejected only when it targets a non-zero
   * database. Consumed by `ClusterPolicy` instead of matching command names.
   */
  clusterMode?: 'forbidden' | 'singleDb'
  /**
   * Marks the command as a transaction boundary. `'begin'` opens a transaction
   * (MULTI); `'end'` closes one (EXEC/DISCARD). `ClusterPolicy` uses this to
   * reset the per-session pinned slot instead of matching command names.
   */
  transactionBoundary?: 'begin' | 'end'
}

export type CommandMonitorMetadata = {
  skip?: boolean
  redactArgs?: (rawArgs: readonly Buffer[]) => readonly Buffer[]
}

export type CommandKeySpec = {
  flags: readonly string[]
  beginSearchIndex: number
  lastKey: number
  keyStep: number
  limit?: number
  notes?: string
}

export type CommandDocumentation = {
  summary: string
  since?: string
  group: string
  complexity?: string
  arguments?: readonly CommandDocumentationArgument[]
}

export type CommandDocumentationArgument = {
  name: string
  type: string
  keySpecIndex?: number
  token?: string
  flags?: readonly string[]
}

export type CommandIntrospection = {
  name?: string
  arity: number
  flags?: readonly string[]
  firstKey?: number
  lastKey?: number
  keyStep?: number
  categories?: readonly string[]
  tips?: readonly string[]
  keySpecs?: readonly CommandKeySpec[]
  subcommands?: readonly CommandIntrospection[]
  docs?: CommandDocumentation
}

export type CommandExecutionResult =
  | RedisResult
  | Promise<RedisResult>
  | ResponseStream

export interface CommandDefinition<TArgs = unknown> {
  readonly name: string
  readonly since?: VersionGate
  readonly schema: CommandSchema<TArgs>
  readonly flags: readonly CommandFlag[]
  readonly capabilities?: CommandCapabilities
  readonly monitor?: CommandMonitorMetadata
  readonly introspection?: CommandIntrospection
  keys(args: TArgs): readonly Buffer[]
  execute(args: TArgs, ctx: RedisExecutionContext): CommandExecutionResult
}

export type CommandPlan<TArgs = unknown> = {
  definition: CommandDefinition<TArgs>
  args: TArgs
  keys: readonly Buffer[]
  rawCommand: Buffer
  rawArgs: readonly Buffer[]
}

/**
 * Builds a command definition, pinning `TArgs` from the schema so `keys` and
 * `execute` get their arguments typed without a manual annotation, and
 * lowercasing the declared name.
 *
 * It returns a **copy**. That is unobservable for the idiomatic literal form —
 * `export const getCommand = defineCommand({ ... })`, where nothing else ever
 * held the argument — but it is not unobservable in general:
 *
 *  - a definition you already hold a reference to comes back as a *different*
 *    object, so metadata keyed off the one you authored will not match the one
 *    that ends up registered;
 *  - a class instance loses the `keys`/`execute` that live on its prototype,
 *    because a spread copies own enumerable properties only. This type-checks —
 *    `CommandDefinition` is an interface — and fails at the first invocation.
 *
 * Register those with {@link CommandRegistry.register} directly: it stores by
 * reference, at the cost of leaving the name's casing alone.
 */
export function defineCommand<TArgs>(
  definition: CommandDefinition<TArgs>,
): CommandDefinition<TArgs> {
  return {
    ...definition,
    name: asciiLowerCase(definition.name),
  }
}
