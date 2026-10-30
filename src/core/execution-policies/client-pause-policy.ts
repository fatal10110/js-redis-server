import type { ExecutionPolicy } from './index'
import type { CommandPlan } from '../command-definition'
import { lookupSubcommandEntry } from '../command-arity'
import type { CompatibilityProfile } from '../compatibility'
import { commandTableEntry } from '../compatibility/command-table'
import { mayReplicate } from '../compatibility/may-replicate'
import type {
  RedisClientSession,
  RedisExecutionContext,
} from '../redis-context'

/**
 * Sessions in MULTI that queued a write or may-replicate command
 * (`c->mstate.cmd_flags`): their EXEC is held back by a WRITE pause.
 */
const queuedWrites = new WeakSet<RedisClientSession>()

/**
 * CLIENT PAUSE (`processCommand` in server.c): while a pause is active, a
 * command of any client but a replica (here: a MONITOR connection) is
 * postponed, not refused, and runs once the pause is lifted. An ALL pause
 * holds back every command, CLIENT UNPAUSE included; a WRITE pause the
 * commands flagged write or may-replicate, and an EXEC whose transaction
 * queued one. The check comes after the auth, subscribed, cluster and
 * MONITOR checks (so their refusals, like unknown commands and wrong arity,
 * are answered at once) and before MULTI queues the command. Scripts and
 * EXEC's replay never reach it. The wait parks the command, which hands the
 * server turn to the other clients.
 */
export function createClientPausePolicy(): ExecutionPolicy {
  return {
    name: 'client-pause',
    beforeExecute(plan, ctx) {
      if (
        ctx.inScript ||
        ctx.transactionReplay ||
        ctx.session.monitoring ||
        answeredBeforePause(plan, ctx.executor.profile)
      ) {
        return
      }

      if (!isPostponed(plan, ctx)) {
        trackQueuedWrite(plan, ctx)
        return
      }

      return waitForUnpause(plan, ctx)
    },
  }
}

/**
 * Before 7.0 (`command.quit-table-entry`) processCommand answers QUIT itself,
 * ahead of every check, the pause included.
 */
function answeredBeforePause(
  plan: CommandPlan,
  profile: CompatibilityProfile,
): boolean {
  return (
    plan.definition.name === 'quit' && !profile.has('command.quit-table-entry')
  )
}

async function waitForUnpause(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
): Promise<void> {
  const pause = ctx.server.clientPause
  do {
    await ctx.park({
      waitFor: pause.whenUnpaused(ctx.signal),
      signal: ctx.signal,
    })
  } while (isPostponed(plan, ctx))
  trackQueuedWrite(plan, ctx)
}

function isPostponed(plan: CommandPlan, ctx: RedisExecutionContext): boolean {
  const mode = ctx.server.clientPause.mode
  if (mode === null) {
    return false
  }
  if (mode === 'all') {
    return true
  }

  if (plan.definition.name === 'exec') {
    return ctx.session.mode === 'transaction' && queuedWrites.has(ctx.session)
  }
  return isWriteCommand(plan, ctx.executor.profile)
}

function trackQueuedWrite(plan: CommandPlan, ctx: RedisExecutionContext): void {
  if (ctx.session.mode !== 'transaction') {
    queuedWrites.delete(ctx.session)
    return
  }
  if (
    !plan.definition.flags.includes('transaction') &&
    isWriteCommand(plan, ctx.executor.profile)
  ) {
    queuedWrites.add(ctx.session)
  }
}

/**
 * `is_may_replicate_command`: flagged write or may-replicate in the real
 * command table, by the entry lookup resolves (`container|subcommand` from
 * 7.0). A custom command answers with the entry it declares for COMMAND INFO.
 */
function isWriteCommand(
  plan: CommandPlan,
  profile: CompatibilityProfile,
): boolean {
  const subcommand = lookupSubcommandEntry(
    plan.definition,
    plan.rawArgs,
    profile,
  )
  const name = subcommand?.name ?? plan.definition.name
  if (mayReplicate(name, profile)) {
    return true
  }

  const declared = subcommand
    ? subcommand.introspection
    : plan.definition.introspection
  const flags = commandTableEntry(name, profile)?.flags ?? declared?.flags ?? []
  return flags.includes('write') || flags.includes('may_replicate')
}
