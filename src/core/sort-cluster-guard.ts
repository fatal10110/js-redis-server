import { RedisCommandError } from './redis-error'
import type { RedisExecutionContext } from './redis-context'
import type { RedisClusterTopology } from '../state/cluster-topology'
import { isSelfSortPattern } from './sort-patterns'

const SORT_BY_DENIED_LEGACY = 'BY option of SORT denied in Cluster mode.'
const SORT_GET_DENIED_LEGACY = 'GET option of SORT denied in Cluster mode.'
const SORT_BY_DENIED =
  'BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.'
const SORT_GET_DENIED =
  'GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.'

/**
 * The cluster guard `sortCommand()` applies to a `BY` glob or a `GET` pattern,
 * at the point its left-to-right option scan reaches it.
 *
 * It is called from SORT's own scan rather than from `ClusterPolicy` because
 * the order is observable (#417): the first offending option wins, a later
 * token that fails to parse is never reached, and — since the check is part of
 * running the command, not of routing it — inside `MULTI` the command queues
 * and the denial surfaces in `EXEC`. SORT only says "I am about to use this
 * pattern"; which modes refuse it, the slot arithmetic and the wording all
 * stay here.
 *
 * The caller passes only patterns that would dereference keys: a `BY` without
 * `*` is constant (`dontsort`) and never reaches the guard, on any version —
 * which is why the documented `BY nosort` works in cluster mode.
 *
 * Two behaviours differ by version, so both are profile-gated:
 *
 * - `sort.cluster-pattern-slot` (Redis 7.4 / Valkey 8.0) replaced a blanket
 *   refusal of every BY glob and *every* GET pattern with a slot comparison —
 *   the pattern is accepted when the keys it can form provably hash to the
 *   sort key's slot — and switched to the longer error wording.
 * - `sort.cluster-get-hash` (Redis 7.4.2 / Valkey 8.0.2) exempts `GET #`, which
 *   returns the element itself and reads no other key, from that comparison.
 */
export function assertSortPatternAllowed(
  ctx: RedisExecutionContext,
  option: 'BY' | 'GET',
  pattern: Buffer,
  key: Buffer,
): void {
  const topology = ctx.server.clusterTopology
  if (topology.nodes.length === 0) {
    return
  }

  const profile = ctx.server.profile
  if (!profile.has('sort.cluster-pattern-slot')) {
    throw new RedisCommandError(
      option === 'BY' ? SORT_BY_DENIED_LEGACY : SORT_GET_DENIED_LEGACY,
    )
  }

  if (
    option === 'GET' &&
    isSelfSortPattern(pattern) &&
    profile.has('sort.cluster-get-hash')
  ) {
    return
  }

  if (patternHashSlot(pattern, topology) !== topology.calculateSlot(key)) {
    throw new RedisCommandError(
      option === 'BY' ? SORT_BY_DENIED : SORT_GET_DENIED,
    )
  }
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
