import type { ExecutionPolicy } from './index'
import {
  RedisClusterDownError,
  RedisCommandError,
  RedisCrossSlotError,
  RedisMovedError,
} from '../redis-error'
import type { RedisClientSession, ScriptSlotPin } from '../redis-context'
import type { RedisClusterTopology } from '../../state'

export type ClusterPolicyOptions = {
  localNodeId: string
  topology?: RedisClusterTopology
}

/** The queue of a transaction touches more than one slot. */
const CROSS_SLOT = -1

export function createClusterPolicy(
  options: ClusterPolicyOptions,
): ExecutionPolicy {
  const transactionSlots = new WeakMap<RedisClientSession, number>()
  let localNodeChecked = false

  return {
    name: 'cluster',
    beforeExecute(plan, ctx) {
      const topology = options.topology ?? ctx.server.clusterTopology

      if (!localNodeChecked) {
        if (!topology.getNode(options.localNodeId)) {
          throw new Error(
            `ClusterPolicy localNodeId ${options.localNodeId} is not present in topology`,
          )
        }
        localNodeChecked = true
      }

      const capabilities = plan.definition.capabilities

      // The cluster refusals below are the command's own, so inside MULTI the
      // command is queued and the check runs when EXEC replays it (the session
      // is no longer in 'transaction' mode then): `MULTI; MOVE k 1; EXEC` answers
      // `MOVE is not allowed in cluster mode` in its EXEC slot. A Valkey 9
      // cluster has databases, so there the command runs and its own
      // `DB index is out of range` answers instead.
      const queueing = ctx.session.mode === 'transaction'
      const multiDb = ctx.server.profile.has('cluster.multi-db')

      // Cluster mode has a single logical database (0). DB 0 is a no-op and
      // accepted; any non-zero index is rejected like real Redis unless the
      // selected profile models Valkey's cluster multi-DB support. A queued
      // plan whose parse failed has no `args` and answers its own error.
      if (
        capabilities?.clusterMode === 'singleDb' &&
        !queueing &&
        !multiDb &&
        !plan.deferredError
      ) {
        const index = (plan.args as { database: number }).database
        if (index !== 0) {
          throw new RedisCommandError(
            `${plan.definition.name.toUpperCase()} is not allowed in cluster mode`,
          )
        }
      }

      // A transaction boundary resets the per-session queue slot: 'begin'
      // (MULTI) starts fresh, 'end' (EXEC/DISCARD) releases it once the
      // current transaction is over.
      if (
        capabilities?.transactionBoundary === 'begin' &&
        ctx.session.mode !== 'transaction'
      ) {
        transactionSlots.delete(ctx.session)
      }

      if (
        capabilities?.transactionBoundary === 'end' &&
        ctx.session.mode === 'transaction'
      ) {
        const queuedSlot = transactionSlots.get(ctx.session)
        transactionSlots.delete(ctx.session)
        // EXEC's own cluster check runs over the whole queue, before EXEC
        // looks at the dirty flag: commands in different slots answer a bare
        // CROSSSLOT and discard the transaction (and its WATCHes). Valkey
        // 8.0 empties the queue when a command is refused, so a dirty
        // transaction has nothing to check there and aborts instead.
        if (
          plan.definition.name === 'exec' &&
          queuedSlot === CROSS_SLOT &&
          !(
            ctx.session.isTransactionDirty() &&
            ctx.server.profile.has('transaction.refusal-empties-queue')
          )
        ) {
          ctx.session.discardTransaction()
          throw new RedisCrossSlotError()
        }
      }

      // Redirection is decided here, before a command runs. SORT's BY/GET
      // cluster guard runs later, inside SORT's own option scan (#417), so a
      // stale slot map still gets a MOVED it can follow rather than a terminal
      // "denied in Cluster mode".
      const slot = validateClusterSlot(
        topology,
        options.localNodeId,
        plan.keys,
        {
          allowReplicaRead:
            ctx.session.clusterReadOnly &&
            plan.definition.flags.includes('readonly'),
        },
      )

      if (slot !== null && ctx.scriptSlot) {
        pinScriptSlot(ctx.scriptSlot, slot)
      }

      // MOVE refuses the cluster in its own command proc, so a key on
      // another node is MOVED first, and a local `MOVE k x` is refused before
      // its database argument is parsed (a parse error waits in the plan).
      const forbidden =
        capabilities?.clusterMode === 'forbidden' ||
        (capabilities?.clusterMode === 'multiDbOnly' && !multiDb)
      if (forbidden && !queueing) {
        throw new RedisCommandError(
          `${plan.definition.name.toUpperCase()} is not allowed in cluster mode`,
        )
      }

      // Inside MULTI, remember which slot the queue touches; EXEC refuses a
      // queue that spans slots. A refused command, or any command after it,
      // is not queued by Redis (`queueMultiCommand` stops once the
      // transaction is dirty), so it takes no part.
      if (
        slot === null ||
        !queueing ||
        plan.definition.flags.includes('transaction') ||
        ctx.session.isTransactionDirty()
      ) {
        return
      }

      const queuedSlot = transactionSlots.get(ctx.session)
      if (queuedSlot === undefined) {
        transactionSlots.set(ctx.session, slot)
      } else if (queuedSlot !== slot) {
        transactionSlots.set(ctx.session, CROSS_SLOT)
      }
    },
  }
}

/**
 * Redis's `scriptVerifyClusterState`: once a command's keys are known to be
 * local, a script that must keep to one slot refuses any slot but the first
 * it touched (or its declared keys').
 */
function pinScriptSlot(pin: ScriptSlotPin, slot: number): void {
  if (pin.slot === null) {
    pin.slot = slot
    return
  }
  if (pin.slot !== slot) {
    throw new RedisCommandError(
      'Script attempted to access keys that do not hash to the same slot',
    )
  }
}

function validateClusterSlot(
  topology: RedisClusterTopology,
  localNodeId: string,
  keys: readonly Buffer[],
  options: { allowReplicaRead?: boolean } = {},
): number | null {
  const slot = topology.calculateSlotForKeys(keys)
  if (slot === null) {
    return null
  }

  if (slot === -1) {
    throw new RedisCrossSlotError()
  }

  if (topology.nodeOwnsSlot(localNodeId, slot)) {
    return slot
  }

  if (
    options.allowReplicaRead &&
    topology.nodeCanServeReadonlySlot(localNodeId, slot)
  ) {
    return slot
  }

  const owner = topology.getSlotOwner(slot)
  if (!owner) {
    throw new RedisClusterDownError()
  }

  throw new RedisMovedError(slot, owner.host, owner.port)
}
