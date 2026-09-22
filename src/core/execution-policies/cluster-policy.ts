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
import type { CompatibilityProfile } from '../compatibility'
import {
  isConstantSortPattern,
  isSelfSortPattern,
  type ClusterSortArgs,
} from '../sort-patterns'

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

      // Redirection is decided in processCommand(), before sortCommand() ever
      // parses BY/GET — so a stale slot map must still get a MOVED it can
      // follow rather than a terminal "denied in Cluster mode".
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

      const sortPatternError = getSortClusterPatternError(
        plan,
        topology,
        ctx.server.profile,
      )
      if (sortPatternError) {
        throw sortPatternError
      }

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

const SORT_BY_DENIED_LEGACY = 'BY option of SORT denied in Cluster mode.'
const SORT_GET_DENIED_LEGACY = 'GET option of SORT denied in Cluster mode.'
const SORT_BY_DENIED =
  'BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.'
const SORT_GET_DENIED =
  'GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.'

/**
 * Mirrors the cluster guards `sortCommand()` applies while parsing BY/GET.
 *
 * Two behaviours differ by version, so both are profile-gated:
 *
 * - `sort.cluster-pattern-slot` (Redis 7.4 / Valkey 8.0) replaced a blanket
 *   refusal of every BY glob and *every* GET pattern with a slot comparison —
 *   the pattern is accepted when the keys it can form provably hash to the
 *   sort key's slot — and switched to the longer error wording.
 * - `sort.cluster-get-hash` (Redis 7.4.2 / Valkey 8.0.2) exempts `GET #`, which
 *   returns the element itself and reads no other key, from that comparison.
 *
 * A BY pattern with no `*` is constant on every version: real Redis sets
 * `dontsort`, never looks a weight key up, and so never reaches the guard —
 * which is why the documented `BY nosort` works in cluster mode.
 */
function getSortClusterPatternError(
  plan: CommandPlan,
  topology: RedisClusterTopology,
  profile: CompatibilityProfile,
): RedisCommandError | null {
  if (plan.definition.name !== 'sort' && plan.definition.name !== 'sort_ro') {
    return null
  }

  const args = plan.args as ClusterSortArgs
  const comparesPatternSlots = profile.has('sort.cluster-pattern-slot')

  if (!comparesPatternSlots) {
    if (args.by && !isConstantSortPattern(args.by)) {
      return new RedisCommandError(SORT_BY_DENIED_LEGACY)
    }
    if (args.get.length > 0) {
      return new RedisCommandError(SORT_GET_DENIED_LEGACY)
    }
    return null
  }

  const keySlot = topology.calculateSlot(args.key)
  const exemptsGetSelf = profile.has('sort.cluster-get-hash')

  if (
    args.by &&
    !isConstantSortPattern(args.by) &&
    patternHashSlot(args.by, topology) !== keySlot
  ) {
    return new RedisCommandError(SORT_BY_DENIED)
  }

  for (const pattern of args.get) {
    if (isSelfSortPattern(pattern) && exemptsGetSelf) {
      continue
    }
    if (patternHashSlot(pattern, topology) !== keySlot) {
      return new RedisCommandError(SORT_GET_DENIED)
    }
  }

  return null
}

/**
 * Port of `patternHashSlot()` from Redis `cluster.c`: the slot every key a
 * glob pattern can match must belong to, or `-1` when that cannot be inferred.
 *
 * A wildcard or an escape before the closing brace makes the match set
 * unbounded; a non-empty `{...}` tag pins the slot to the tag; anything else
 * is a literal key and hashes as one.
 */
function patternHashSlot(
  pattern: Buffer,
  topology: RedisClusterTopology,
): number {
  let tagStart = -1

  for (let i = 0; i < pattern.length; i++) {
    const byte = pattern[i]

    // '*', '?', '[' or '\' — keys can be in any slot.
    if (byte === 0x2a || byte === 0x3f || byte === 0x5b || byte === 0x5c) {
      return -1
    }

    if (tagStart === -1 && byte === 0x7b) {
      tagStart = i
      continue
    }

    if (tagStart < 0 || byte !== 0x7d) {
      continue
    }

    // '{}' hashes the whole key; -2 stops any later brace from opening a tag.
    if (i === tagStart + 1) {
      tagStart = -2
      continue
    }

    return topology.calculateSlot(pattern.subarray(tagStart + 1, i))
  }

  return topology.calculateSlot(pattern)
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
