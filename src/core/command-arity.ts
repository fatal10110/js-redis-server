import { asciiLowerCase } from './ascii-case'
import type {
  CommandDefinition,
  CommandIntrospection,
} from './command-definition'
import { schemaArity, type CommandSchema } from './command-schema'
import type { CompatibilityProfile } from './compatibility'
import {
  containerSubcommandArity,
  containerSubcommandExists,
} from './compatibility/subcommand-gates'

/**
 * A command's command-table arity, as `COMMAND INFO` reports it: the token
 * count including the command name, negated when it is only a minimum. An
 * explicit `introspection.arity` wins (per profile when it is a function);
 * otherwise it is derived from `schema`, and without one it is -1 (anything).
 */
export function commandTableArity(
  introspection: CommandIntrospection | undefined,
  profile: CompatibilityProfile,
  schema?: CommandSchema<unknown>,
): number {
  const arity = introspection?.arity
  if (typeof arity === 'function') {
    return arity(profile)
  }

  if (arity !== undefined) {
    return arity
  }

  return schema ? schemaArity(schema) : -1
}

/**
 * The `container|subcommand` command-table entry a call resolves to, or
 * `null` when lookup has only the container's entry: on 6.2, which has no
 * subcommand entries, for a command that is not a container, or for a
 * subcommand without an entry. From 7.0 (the
 * `error.unknown-subcommand-dispatch-timing` gate) the arity comes from the
 * *real* command table (`containerSubcommandArity`), so it applies even to a
 * subcommand this server does not implement; a container the real table does
 * not know (a custom one from `extraCommands`) falls back to the subcommand
 * entries its definition declares. `introspection` is the declared entry,
 * when there is one. `rawArgs` excludes the command name.
 */
export function lookupSubcommandEntry(
  definition: CommandDefinition<unknown>,
  rawArgs: readonly Buffer[],
  profile: CompatibilityProfile,
): {
  name: string
  arity: number
  introspection?: CommandIntrospection
} | null {
  if (
    rawArgs.length === 0 ||
    !profile.has('error.unknown-subcommand-dispatch-timing')
  ) {
    return null
  }

  const name = `${definition.name}|${asciiLowerCase(rawArgs[0].toString('latin1'))}`
  const introspection = definition.introspection?.subcommands?.find(
    entry => entry.name === name,
  )
  const real = containerSubcommandArity(definition.name, rawArgs[0], profile)
  if (real !== undefined) {
    return { name, arity: real, introspection }
  }

  // Only a container the real table does not model falls back to its own
  // declarations; a real container's missing entry is an unknown subcommand,
  // which lookup has already refused.
  if (
    !introspection ||
    containerSubcommandExists(definition.name, rawArgs[0], profile) !==
      undefined
  ) {
    return null
  }
  return {
    name,
    arity: commandTableArity(introspection, profile),
    introspection,
  }
}

/**
 * The command-table entry a call is looked up as, and its arity: the
 * subcommand's entry when lookup resolves one ({@link lookupSubcommandEntry}),
 * otherwise the command's own.
 */
export function lookupTableArity(
  definition: CommandDefinition<unknown>,
  rawArgs: readonly Buffer[],
  profile: CompatibilityProfile,
): { name: string; arity: number } {
  const subcommand = lookupSubcommandEntry(definition, rawArgs, profile)
  if (subcommand) {
    return { name: subcommand.name, arity: subcommand.arity }
  }
  return {
    name: definition.name,
    arity: commandTableArity(
      definition.introspection,
      profile,
      definition.schema,
    ),
  }
}

/**
 * Whether `argc` tokens (command name included) fail Redis's command-table
 * arity check, the one `processCommand` / `scriptCall` run before the command
 * itself: an exact count for a positive arity, a minimum for a negative one.
 * A command can still reject a count that passes this (HSET's field/value
 * pairs, an odd MSET), with its own error.
 */
export function failsTableArity(arity: number, argc: number): boolean {
  return arity > 0 ? argc !== arity : argc < -arity
}
