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

      if (plan.definition.name === 'select') {
        throw new RedisCommandError('SELECT is not allowed in cluster mode')
      }

      if (
        plan.definition.name === 'multi' &&
        ctx.session.mode !== 'transaction'
      ) {
        transactionSlots.delete(ctx.session)
      }

      if (
        ctx.session.mode === 'transaction' &&
        (plan.definition.name === 'exec' || plan.definition.name === 'discard')
      ) {
        transactionSlots.delete(ctx.session)
      }

      const slot = validateClusterSlot(
        topology,
        options.localNodeId,
        plan.keys,
        {
          allowReplicaRead:
            ctx.session.clusterReadOnly && plan.flags.includes('readonly'),
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
    throw new RedisClusterDownError(slot)
  }

  throw new RedisMovedError(slot, owner.host, owner.port)
}
