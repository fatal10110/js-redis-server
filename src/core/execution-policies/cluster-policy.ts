import type { ExecutionPolicy } from './index'
import {
  RedisClusterDownError,
  RedisCommandError,
  RedisCrossSlotError,
  RedisMovedError,
} from '../redis-error'
import type { RedisClientSession } from '../redis-context'
import type { RedisClusterTopology } from '../../state'

export type ClusterPolicyOptions = {
  localNodeId: string
  topology?: RedisClusterTopology
}

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

      // Both checks below are the command's own, so inside MULTI the command
      // is queued and the check runs when EXEC replays it (the session is no
      // longer in 'transaction' mode then): `MULTI; MOVE k 1; EXEC` answers
      // `MOVE is not allowed in cluster mode` in its EXEC slot. A Valkey 9
      // cluster has databases, so there the command runs and its own
      // `DB index is out of range` answers instead.
      const queueing = ctx.session.mode === 'transaction'
      const multiDb = ctx.server.profile.has('cluster.multi-db')

      if (capabilities?.clusterMode === 'forbidden' && !queueing && !multiDb) {
        throw new RedisCommandError(
          `${plan.definition.name.toUpperCase()} is not allowed in cluster mode`,
        )
      }

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

      // A transaction boundary resets the per-session pinned slot: 'begin'
      // (MULTI) starts fresh, 'end' (EXEC/DISCARD) releases the pin once the
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
        transactionSlots.delete(ctx.session)
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

      if (slot === null || ctx.session.mode !== 'transaction') {
        return
      }

      const pinnedSlot = transactionSlots.get(ctx.session)
      if (pinnedSlot === undefined) {
        transactionSlots.set(ctx.session, slot)
        return
      }

      if (pinnedSlot !== slot) {
        throw new RedisCrossSlotError()
      }
    },
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
