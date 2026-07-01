import type { ExecutionPolicy } from './index'
import type { CommandPlan } from '../command-definition'
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

      if (capabilities?.clusterMode === 'forbidden') {
        throw new RedisCommandError(
          `${plan.definition.name.toUpperCase()} is not allowed in cluster mode`,
        )
      }

      if (capabilities?.clusterMode === 'singleDb') {
        // Cluster mode has a single logical database (0). DB 0 is a no-op and
        // accepted; any non-zero index is rejected like real Redis unless the
        // selected profile models Valkey's cluster multi-DB support.
        const index = (plan.args as { database: number }).database
        if (index !== 0 && !ctx.server.profile.has('cluster.multi-db')) {
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

      const sortPatternError = getSortClusterPatternError(plan, topology)
      if (sortPatternError) {
        throw sortPatternError
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

function getSortClusterPatternError(
  plan: CommandPlan,
  topology: RedisClusterTopology,
): RedisCommandError | null {
  if (plan.definition.name !== 'sort' && plan.definition.name !== 'sort_ro') {
    return null
  }

  const args = plan.args as {
    key?: unknown
    by?: unknown
    get?: unknown
  }
  if (!Buffer.isBuffer(args.key)) {
    return null
  }

  if (
    Buffer.isBuffer(args.by) &&
    sortPatternMayCrossSlot(args.key, args.by, topology)
  ) {
    return new RedisCommandError(
      'BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.',
    )
  }

  const get = Array.isArray(args.get) ? args.get : []
  for (const pattern of get) {
    if (
      Buffer.isBuffer(pattern) &&
      !isSelfSortPattern(pattern) &&
      sortPatternMayCrossSlot(args.key, pattern, topology)
    ) {
      return new RedisCommandError(
        'GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.',
      )
    }
  }

  return null
}

function sortPatternMayCrossSlot(
  source: Buffer,
  pattern: Buffer,
  topology: RedisClusterTopology,
): boolean {
  if (!pattern.includes(0x2a)) {
    return topology.calculateSlotForKeys([source, pattern]) === -1
  }

  const sourceTag = hashTag(source)
  const patternTag = hashTag(pattern)
  return !sourceTag || !patternTag || !sourceTag.equals(patternTag)
}

function hashTag(key: Buffer): Buffer | null {
  const open = key.indexOf(0x7b)
  if (open === -1) {
    return null
  }

  const close = key.indexOf(0x7d, open + 1)
  if (close <= open + 1) {
    return null
  }

  return key.subarray(open + 1, close)
}

function isSelfSortPattern(pattern: Buffer): boolean {
  return pattern.length === 1 && pattern[0] === 0x23
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
