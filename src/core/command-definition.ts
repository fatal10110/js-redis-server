import type { CommandSchema } from './command-schema'
import type { RedisExecutionContext } from './redis-context'
import type { RedisCommandError } from './redis-error'
import type { RedisResult } from './redis-result'
import type { CompatibilityProfile, VersionGate } from './compatibility'
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
   * How the command behaves under cluster mode. `'forbidden'` is rejected
   * outright, and `'singleDb'` only when it targets a non-zero database,
   * unless the profile models Valkey 9's cluster databases
   * (`cluster.multi-db`). Either is checked when the command runs, so inside
   * MULTI it is queued and answers at EXEC. Consumed by `ClusterPolicy`
   * instead of matching command names.
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

/**
 * A Redis key spec (`COMMAND INFO` / `COMMAND DOCS`). `begin_search` is the
 * index `beginSearchIndex`, or with `beginSearchKeyword` the argument after a
 * keyword searched for from `startFrom` (backwards from the end when
 * negative). `find_keys` is the range `lastKey` / `keyStep` / `limit`, or with
 * `findKeysKeynum` a count read from the argument `keyNumIdx` after the
 * begin position, the keys starting `firstKey` after it. Besides COMMAND INFO,
 * specs are how a queued command whose own parser failed is routed in a
 * cluster (see `keysFromKeySpecs`).
 */
export type CommandKeySpec = {
  flags: readonly string[]
  beginSearchIndex: number
  beginSearchKeyword?: { keyword: string; startFrom: number }
  lastKey: number
  keyStep: number
  limit?: number
  findKeysKeynum?: { keyNumIdx: number; firstKey: number; keyStep: number }
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

/**
 * `COMMAND INFO` / `COMMAND DOCS` metadata that cannot be derived from the
 * rest of the definition (#370). Arity comes from `schema` and the legacy
 * first/last/step key range from `keySpecs` (or, without them, from the
 * schema's key positions) — declare `arity` only where the schema cannot
 * express it, such as synthetic subcommand entries or a version-gated
 * argument whose arity differs by compatibility profile.
 */
export type CommandIntrospection = {
  name?: string
  arity?: number | ((profile: CompatibilityProfile) => number)
  flags?: readonly string[]
  categories?: readonly string[]
  tips?: readonly string[]
  keySpecs?: readonly CommandKeySpec[]
  subcommands?: readonly CommandIntrospection[]
  docs?: CommandDocumentation
  /**
   * The fields that differ on some profiles, merged over the rest when it
   * returns them (see `introspectionFor`): XINFO's 6.2 entry, say, or the
   * `variable_flags` Valkey puts on GEORADIUS's STORE key specs.
   */
  forProfile?: (
    profile: CompatibilityProfile,
  ) => Omit<CommandIntrospection, 'forProfile' | 'name'> | undefined
}

/** `introspection` as `profile` reports it: `forProfile` merged over it. */
export function introspectionFor(
  introspection: CommandIntrospection | undefined,
  profile: CompatibilityProfile,
): CommandIntrospection | undefined {
  const override = introspection?.forProfile?.(profile)
  return override ? { ...introspection, ...override } : introspection
}

export type CommandExecutionResult = RedisResult | Promise<RedisResult>

export interface CommandDefinition<TArgs = unknown> {
  readonly name: string
  readonly since?: VersionGate
  readonly schema: CommandSchema<TArgs>
  readonly flags: readonly CommandFlag[]
  readonly capabilities?: CommandCapabilities
  readonly monitor?: CommandMonitorMetadata
  readonly introspection?: CommandIntrospection
  keys(args: TArgs): readonly Buffer[]
  /**
   * Redis's getkeys proc: the keys in a raw `argv` (command name at index 0)
   * without parsing it. Only consulted to route a command queued inside MULTI
   * whose own parser failed; without one, the legacy first/last/step range
   * `COMMAND INFO` reports is used, as Redis does.
   */
  rawKeys?(argv: readonly Buffer[]): readonly Buffer[]
  execute(args: TArgs, ctx: RedisExecutionContext): CommandExecutionResult
}

type CommandPlanBase<TArgs> = {
  definition: CommandDefinition<TArgs>
  keys: readonly Buffer[]
  rawCommand: Buffer
  rawArgs: readonly Buffer[]
}

/**
 * A resolved command, ready to run: its definition, parsed `args` and routing
 * `keys`. A command queued inside MULTI whose own argument parsing failed is
 * the second form: no `args`, and `deferredError` set to the error its EXEC
 * slot answers, raised after the policy chain; its `keys` come from the
 * command's getkeys proc or legacy key range over `rawArgs`. A policy that
 * reads `args` narrows on `deferredError` first.
 */
export type CommandPlan<TArgs = unknown> =
  | (CommandPlanBase<TArgs> & { args: TArgs; deferredError?: undefined })
  | (CommandPlanBase<TArgs> & {
      args?: undefined
      deferredError: RedisCommandError
    })

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
