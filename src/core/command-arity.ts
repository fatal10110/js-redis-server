import { asciiLowerCase } from './ascii-case'
import type {
  CommandDefinition,
  CommandIntrospection,
} from './command-definition'
import { schemaArity, type CommandSchema } from './command-schema'
import type { CompatibilityProfile } from './compatibility'

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
 * The command-table entry a call is looked up as, and its arity. From 7.0
 * (the `error.unknown-subcommand-dispatch-timing` gate) lookup resolves a
 * container's `container|subcommand` entry, so the subcommand's own arity
 * applies; 6.2 has only the container's. `rawArgs` excludes the command name.
 */
export function lookupTableArity(
  definition: CommandDefinition<unknown>,
  rawArgs: readonly Buffer[],
  profile: CompatibilityProfile,
): { name: string; arity: number } {
  const own = {
    name: definition.name,
    arity: commandTableArity(
      definition.introspection,
      profile,
      definition.schema,
    ),
  }
  const subcommands = definition.introspection?.subcommands
  if (
    !subcommands ||
    rawArgs.length === 0 ||
    !profile.has('error.unknown-subcommand-dispatch-timing')
  ) {
    return own
  }

  const name = `${definition.name}|${asciiLowerCase(rawArgs[0].toString())}`
  const subcommand = subcommands.find(entry => entry.name === name)
  return subcommand
    ? { name, arity: commandTableArity(subcommand, profile) }
    : own
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
