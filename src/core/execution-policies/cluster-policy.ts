import type { ExecutionPolicy } from './index'
import {
  RedisAskError,
  RedisClusterDownError,
  RedisCommandError,
  RedisCrossSlotError,
  RedisMovedError,
  RedisTryAgainError,
} from '../redis-error'
import type { RedisClientSession, ScriptSlotPin } from '../redis-context'
import type { RedisClusterTopology, RedisDatabase } from '../../state'

export type ClusterPolicyOptions = {
  localNodeId: string
  topology?: RedisClusterTopology
}

/** The queue of a transaction touches more than one slot. */
const CROSS_SLOT = -1

const PUBSUB_SHARD_COMMANDS = new Set([
  'spublish',
  'ssubscribe',
  'sunsubscribe',
])

/** What EXEC routes the whole queue by, as `getNodeByQuery` does for EXEC. */
type QueuedRouting = {
  keys: Buffer[]
  /** A queued command has the `asking` flag (RESTORE-ASKING). */
  asking: boolean
}

export function createClusterPolicy(
  options: ClusterPolicyOptions,
): ExecutionPolicy {
  const transactionSlots = new WeakMap<RedisClientSession, number>()
  const transactionKeys = new WeakMap<RedisClientSession, QueuedRouting>()
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
        transactionKeys.delete(ctx.session)
      }

      if (
        capabilities?.transactionBoundary === 'end' &&
        ctx.session.mode === 'transaction'
      ) {
        const queuedSlot = transactionSlots.get(ctx.session)
        const queued = transactionKeys.get(ctx.session)
        transactionSlots.delete(ctx.session)
        transactionKeys.delete(ctx.session)
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

        // EXEC routes the queue as one command (getNodeByQuery treats EXEC
        // as its queue): keys of a resharded slot that went missing since
        // they were queued answer ASK or TRYAGAIN for EXEC itself, which
        // discards the transaction. Valkey 9.0 counts the keys only here.
        if (
          plan.definition.name === 'exec' &&
          queued &&
          queuedSlot !== undefined &&
          queuedSlot !== CROSS_SLOT &&
          !(
            ctx.session.isTransactionDirty() &&
            ctx.server.profile.has('transaction.refusal-empties-queue')
          ) &&
          slotIsOpen(topology, options.localNodeId, queuedSlot)
        ) {
          try {
            validateClusterSlot(topology, options.localNodeId, queued.keys, {
              db: ctx.db,
              asking: ctx.session.clusterAsking || queued.asking,
              migration: 'keys',
              mixedKeysTryAgain: ctx.server.profile.has(
                'cluster.migrating-mixed-keys-tryagain',
              ),
            })
          } catch (err) {
            ctx.session.discardTransaction()
            throw err
          }
        }
      }

      // Redirection is decided here, before a command runs. SORT's BY/GET
      // cluster guard runs later, inside SORT's own option scan (#417), so a
      // stale slot map still gets a MOVED it can follow rather than a terminal
      // "denied in Cluster mode".
      //
      // A slot open for resharding is decided by which of the keys this node
      // still holds (ASK, TRYAGAIN, or served after ASKING). A command EXEC
      // replays was decided with the whole queue when EXEC started, as Redis
      // runs queued commands without routing them again; a script's own
      // calls are still checked. Valkey 9.0 looks the keys up only at EXEC.
      const replay = ctx.transactionReplay === true && ctx.inScript !== true
      const slot = validateClusterSlot(
        topology,
        options.localNodeId,
        plan.keys,
        {
          db: ctx.db,
          asking:
            ctx.session.clusterAsking ||
            plan.definition.flags.includes('asking'),
          migration: replay
            ? 'serve'
            : queueing && ctx.server.profile.has('cluster.ask-at-exec')
              ? 'defer'
              : 'keys',
          isMigrate: plan.definition.name === 'migrate',
          pubsubShard: PUBSUB_SHARD_COMMANDS.has(plan.definition.name),
          mixedKeysTryAgain: ctx.server.profile.has(
            'cluster.migrating-mixed-keys-tryagain',
          ),
          allowReplicaRead:
            ctx.session.clusterReadOnly &&
            plan.definition.flags.includes('readonly'),
          crossSlotFirst: ctx.server.profile.has(
            'cluster.crossslot-before-unserved',
          ),
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
      // transaction is dirty), so it takes no part. From 7.0 a touched WATCH
      // stops queueing too (CLIENT_DIRTY_CAS), so EXEC answers nil.
      if (
        slot === null ||
        !queueing ||
        plan.definition.flags.includes('transaction') ||
        ctx.session.isTransactionDirty() ||
        (ctx.session.isWatchDirty() &&
          ctx.server.profile.has('transaction.watch-dirty-stops-queue'))
      ) {
        return
      }

      const queuedSlot = transactionSlots.get(ctx.session)
      if (queuedSlot === undefined) {
        transactionSlots.set(ctx.session, slot)
      } else if (queuedSlot !== slot) {
        transactionSlots.set(ctx.session, CROSS_SLOT)
      }

      let queued = transactionKeys.get(ctx.session)
      if (!queued) {
        queued = { keys: [], asking: false }
        transactionKeys.set(ctx.session, queued)
      }
      queued.keys.push(...plan.keys)
      queued.asking ||= plan.definition.flags.includes('asking')
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

function slotIsOpen(
  topology: RedisClusterTopology,
  localNodeId: string,
  slot: number,
): boolean {
  return (
    topology.migratingTarget(localNodeId, slot) !== undefined ||
    topology.importingSource(localNodeId, slot) !== undefined
  )
}

type SlotRouting = {
  /** The database the keys of a resharded slot are looked up in. */
  db: RedisDatabase
  /** The client sent ASKING, or the command is flagged `asking`. */
  asking: boolean
  /**
   * How a slot open for resharding is decided: `keys` looks the keys up
   * (getNodeByQuery), `defer` leaves that to EXEC (valkey 9.0 queueing),
   * `serve` runs the command here (EXEC replaying its queue).
   */
  migration: 'keys' | 'defer' | 'serve'
  /** MIGRATE always runs on a node with the slot open. */
  isMigrate?: boolean
  /** Shard channels are not keys: they are never looked up. */
  pubsubShard?: boolean
  /** Present and missing keys of a MIGRATING slot are TRYAGAIN (7.0.3+). */
  mixedKeysTryAgain: boolean
  allowReplicaRead?: boolean
  crossSlotFirst?: boolean
}

/**
 * Redis's `getNodeByQuery` (redis 7.2.4 cluster.c, valkey 9.0.0 cluster.c):
 * the slot the keys route to on this node, or the redirection or refusal.
 */
function validateClusterSlot(
  topology: RedisClusterTopology,
  localNodeId: string,
  keys: readonly Buffer[],
  options: SlotRouting,
): number | null {
  if (keys.length === 0) {
    return null
  }

  // Redis's getNodeByQuery walks the keys in order and looks the first key's
  // slot up before it compares the others, so an unserved first key answers
  // CLUSTERDOWN even when a later key is in another slot. Valkey 9.0 checks
  // that every key shares one slot first (CROSSSLOT wins there).
  const slot = topology.calculateSlot(keys[0])
  if (!options.crossSlotFirst && !topology.getSlotOwner(slot)) {
    throw new RedisClusterDownError()
  }

  if (topology.calculateSlotForKeys(keys) === -1) {
    throw new RedisCrossSlotError()
  }

  const owner = topology.getSlotOwner(slot)
  if (!owner) {
    throw new RedisClusterDownError()
  }

  // A slot this node owns and migrates out, or one it imports from its
  // owner. Its keys decide: the source keeps serving the keys it still
  // holds and ASKs for the others, the target serves a client that sent
  // ASKING (or RESTORE-ASKING); a mix of present and missing keys is
  // TRYAGAIN, the client retries once the move is done.
  const migratingTo =
    owner.id === localNodeId
      ? topology.migratingTarget(localNodeId, slot)
      : undefined
  const importing =
    !migratingTo && topology.importingSource(localNodeId, slot) !== undefined
  if ((migratingTo || importing) && options.migration !== 'keys') {
    if (
      options.migration === 'serve' ||
      migratingTo ||
      options.asking ||
      options.isMigrate
    ) {
      return slot
    }
  } else if (migratingTo || importing) {
    if (options.isMigrate) {
      return slot
    }

    let missing = 0
    let existing = 0
    let multipleKeys = false
    const now = Date.now()
    for (const key of keys) {
      if (importing && !multipleKeys && !key.equals(keys[0])) {
        multipleKeys = true
      }
      if (options.pubsubShard) {
        continue
      }
      if (options.db.peekExists(key, now)) {
        existing++
      } else {
        missing++
      }
    }

    if (migratingTo && missing > 0) {
      if (existing > 0 && options.mixedKeysTryAgain) {
        throw new RedisTryAgainError()
      }
      throw new RedisAskError(slot, migratingTo.host, migratingTo.port)
    }

    if (importing && options.asking) {
      if (multipleKeys && missing > 0) {
        throw new RedisTryAgainError()
      }
      return slot
    }
  }

  if (owner.id === localNodeId) {
    return slot
  }

  if (
    options.allowReplicaRead &&
    topology.nodeCanServeReadonlySlot(localNodeId, slot)
  ) {
    return slot
  }

  throw new RedisMovedError(slot, owner.host, owner.port)
}
