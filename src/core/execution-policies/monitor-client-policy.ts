import type { ExecutionPolicy } from './index'
import type { CommandPlan } from '../command-definition'
import { lookupTableArity } from '../command-arity'
import type { CompatibilityProfile } from '../compatibility'
import { commandTableEntry } from '../compatibility/command-table'
import { errors } from '../redis-error'
import { RedisResult } from '../redis-result'

/** The command-table flags that make `processCommand` refuse a replica. */
const KEYSPACE_FLAGS = ['readonly', 'write', 'may_replicate']

/**
 * The commands flagged `CMD_MAY_REPLICATE` and neither readonly nor write.
 * From 7.0 `COMMAND INFO` no longer reports `may_replicate`, so the captured
 * table cannot tell; these come from the 7.0.15 commands.c and the 7.2.4 /
 * 7.4.4 / 8.0.0 commands.def (Redis 6.2's table still reports the flag).
 */
const MAY_REPLICATE_COMMANDS = new Set([
  'eval',
  'evalsha',
  'fcall',
  'publish',
  'spublish',
])

/** Valkey 8.0 / 9.0 commands.def add these to {@link MAY_REPLICATE_COMMANDS}. */
const VALKEY_MAY_REPLICATE_COMMANDS = new Set([
  'cluster|setslot',
  'cluster|syncslots',
])

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
  const { name } = lookupTableArity(plan.definition, plan.rawArgs, profile)
  // 6.2 answers QUIT in processCommand before any lookup or check, and has no
  // table entry for it; from 7.0 its entry is not a keyspace one.
  if (name === 'quit') {
    return false
  }

  if (
    MAY_REPLICATE_COMMANDS.has(name) ||
    (profile.flavor === 'valkey' && VALKEY_MAY_REPLICATE_COMMANDS.has(name))
  ) {
    return true
  }

  const flags =
    commandTableEntry(name, profile)?.flags ??
    plan.definition.introspection?.flags ??
    plan.definition.flags
  return flags.some(flag => KEYSPACE_FLAGS.includes(flag))
}
