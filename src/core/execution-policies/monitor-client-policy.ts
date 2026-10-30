import type { ExecutionPolicy } from './index'
import type { CommandPlan } from '../command-definition'
import { lookupSubcommandEntry } from '../command-arity'
import type { CompatibilityProfile } from '../compatibility'
import { commandTableEntry } from '../compatibility/command-table'
import { mayReplicate } from '../compatibility/may-replicate'
import { errors } from '../redis-error'
import { RedisResult } from '../redis-result'

/**
 * The command-table flags that make `processCommand` refuse a replica. Only
 * 6.2's table and a custom command's declared entry still report
 * `may_replicate`; the real set is {@link mayReplicate}.
 */
const KEYSPACE_FLAGS = ['readonly', 'write', 'may_replicate']

/**
 * A MONITOR connection is a replica to real Redis (`CLIENT_SLAVE`), and
 * `processCommand` refuses a replica any command flagged readonly, write or
 * may_replicate (GET, SET, KEYS, DBSIZE, PUBLISH, EVAL, ...) with `Replica
 * can't interact with the keyspace`. Other commands (PING, ECHO, SELECT,
 * CLIENT, INFO, MULTI, ...) still run. It comes after the auth, subscribed
 * and cluster checks and before MULTI queues the command, so inside MULTI the
 * refusal dirties the transaction. The flags are those of the entry lookup
 * resolves (`container|subcommand` from 7.0). Verified against a live
 * redis-server 7.0.15 and the 6.2.14 - 8.0.0 / Valkey 8.0 - 9.0 server.c.
 */
export function createMonitorClientPolicy(): ExecutionPolicy {
  return {
    name: 'monitor-client',
    rejectsBeforeCall: true,
    beforeExecute(plan, ctx) {
      // Script calls and EXEC's replay skip processCommand in Redis.
      if (!ctx.session.monitoring || ctx.inScript || ctx.transactionReplay) {
        return
      }

      const profile = ctx.executor.profile
      if (!touchesKeyspace(plan, profile)) {
        return
      }

      return RedisResult.fromError(errors.replicaKeyspace(profile))
    },
  }
}

function touchesKeyspace(
  plan: CommandPlan,
  profile: CompatibilityProfile,
): boolean {
  const subcommand = lookupSubcommandEntry(
    plan.definition,
    plan.rawArgs,
    profile,
  )
  const name = subcommand?.name ?? plan.definition.name
  // 6.2 answers QUIT in processCommand before any lookup or check, and has no
  // table entry for it; from 7.0 its entry is not a keyspace one.
  if (name === 'quit') {
    return false
  }

  // Every may-replicate command, EVAL / EVALSHA / FCALL included: unlike a
  // script, a client reaches them all. PFCOUNT is readonly as well.
  if (mayReplicate(name, profile)) {
    return true
  }

  // Never the mock's own `flags`: they are not the real table's (CLIENT is
  // `readonly` here). The captured table lists only the subcommands this
  // server implements, and every real subcommand it lacks that is flagged
  // readonly / write belongs to a command it does not have (OBJECT, MEMORY),
  // so a miss is not a keyspace command. A custom command answers with the
  // entry it declares for COMMAND INFO.
  const declared = subcommand
    ? subcommand.introspection
    : plan.definition.introspection
  const flags = commandTableEntry(name, profile)?.flags ?? declared?.flags ?? []
  return flags.some(flag => KEYSPACE_FLAGS.includes(flag))
}
