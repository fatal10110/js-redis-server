/**
 * The decisions `scripts/flush-redis.ts` makes about a cluster, kept free of
 * I/O so they can be unit-tested against CLUSTER NODES fixtures instead of by
 * breaking a real cluster (#458):
 *
 *  - which nodes the flush has to cover (`planCluster`), and
 *  - how a replica is judged when its master was not flushed
 *    (`replicaMasterProblem`).
 */

/** What to actually do about each kind of failure — never one generic line. */
export const HINT = {
  unreachable:
    'Start the test backends: docker compose -f docker-compose.test.yml up -d --wait',
  notCluster:
    'REDIS_CLUSTER_PORTS must point at cluster-enabled nodes; standalone servers go in REDIS_STANDALONE_PORT / REDIS_STANDALONE_AUTH_PORT.',
  clusterUnhealthy:
    'The cluster itself is unhealthy: check CLUSTER INFO / CLUSTER NODES on a reachable node and restart any failed ones.',
  staleReplica:
    'A replica is not following a flushed master: wait for it to resync, or restart it so it performs a full sync.',
  leftKeys:
    'Keys survived FLUSHALL: something is writing to this node concurrently — stop whatever else is using it.',
} as const

export type Failure = { message: string; hint?: string }

export type ClusterNode = {
  id: string
  /** Empty when the node has not learned its own address yet. */
  host: string
  port: number
  flags: string[]
  /** The id of the master this node replicates, or null for a master. */
  masterId: string | null
  /** Whether the node serves at least one slot (import/migrate markers aside). */
  ownsSlots: boolean
}

/**
 * Parse CLUSTER NODES, one node per line:
 *
 *   <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv>
 *   <config-epoch> <link-state> <slot> <slot> ...
 *
 * `<master>` is `-` for a master. Slots are `N` or `N-M`; the bracketed
 * `[N->-id]` / `[N-<-id]` entries only describe a migration in progress and do
 * not by themselves mean the node serves anything.
 */
export function parseClusterNodes(text: string): ClusterNode[] {
  return text
    .split('\n')
    .map(line => line.trim())
    .filter(line => line !== '')
    .map(line => {
      const [id, address = '', flags = '', master = '-', ...rest] =
        line.split(' ')
      const hostPort = address.split('@')[0]
      const separator = hostPort.lastIndexOf(':')
      const slots = rest.slice(4)
      return {
        id,
        host: hostPort.slice(0, separator),
        port: Number(hostPort.slice(separator + 1)),
        flags: flags.split(','),
        masterId: master === '-' ? null : master,
        ownsSlots: slots.some(slot => slot !== '' && !slot.startsWith('[')),
      }
    })
}

/** A node CLUSTER NODES lists without an address anyone could connect to. */
export function isAddressless(node: ClusterNode): boolean {
  return !(node.port > 0) || node.flags.includes('noaddr')
}

function isReplica(node: ClusterNode): boolean {
  return node.masterId !== null || node.flags.includes('slave')
}

export type ClusterPlan = {
  /** Ids of the nodes that answered as `myself` — the seeds themselves. */
  seedIds: Set<string>
  /** Non-seed nodes the flush must cover: slot-owning masters and their replicas. */
  members: ClusterNode[]
  /**
   * Non-seed nodes that hold no part of the keyspace — a node still in
   * `handshake`, a `noaddr` placeholder, a master left without slots — and
   * are therefore neither flushed nor verified.
   */
  skipped: ClusterNode[]
}

/**
 * Decide which nodes the flush covers, given the CLUSTER NODES view of every
 * seed that answered.
 *
 * Only masters that own slots, plus their replicas, can hold keys a test will
 * read, so only they are flushed and verified. Anything else is skipped rather
 * than treated as a failure: a `handshake` entry exists only on the node that
 * issued the MEET, so failing on it would make the result depend on which
 * seed was listed (#458).
 *
 * Views are merged by node id. A node counts as a slot owner if any seed says
 * so, and a replica is covered if any seed puts it under such an owner, so a
 * seed with a stale view cannot shrink the flush.
 */
export function planCluster(views: readonly ClusterNode[][]): ClusterPlan {
  const seedIds = new Set<string>()
  const owners = new Set<string>()
  const reports = new Map<string, ClusterNode[]>()

  for (const view of views) {
    for (const node of view) {
      if (node.flags.includes('myself')) {
        seedIds.add(node.id)
      }
      if (node.ownsSlots && !isReplica(node)) {
        owners.add(node.id)
      }
      reports.set(node.id, [...(reports.get(node.id) ?? []), node])
    }
  }

  const members: ClusterNode[] = []
  const skipped: ClusterNode[] = []

  for (const [id, nodeReports] of reports) {
    if (seedIds.has(id)) {
      continue
    }
    const covered =
      owners.has(id) ||
      nodeReports.some(
        node => node.masterId !== null && owners.has(node.masterId),
      )
    // Prefer a report that carries a usable address: seeds can disagree while
    // a node's address propagates.
    const node =
      nodeReports.findLast(report => !isAddressless(report)) ??
      nodeReports[nodeReports.length - 1]
    ;(covered ? members : skipped).push(node)
  }

  return { seedIds, members, skipped }
}

/**
 * Judge a replica against its master before looking at its own keyspace.
 * Returns null when the master was flushed and the replica can be checked.
 *
 * A master that was part of this run but failed — unreachable, FLUSHALL
 * erroring or leaving keys, CLUSTER NODES unreadable — has already been
 * reported against itself with the hint that fits. Its replicas point back at
 * it instead of claiming the run skipped their master and suggesting a
 * replica resync that would not help (#458). Only a master the run never knew
 * about gets the stale-replica diagnosis.
 *
 * @param failedMasters port → label of every master this run tried and failed.
 */
export function replicaMasterProblem(
  masterPort: number,
  flushedPorts: ReadonlySet<number>,
  failedMasters: ReadonlyMap<number, string>,
): Failure | null {
  if (flushedPorts.has(masterPort)) {
    return null
  }

  const master = failedMasters.get(masterPort)
  if (master !== undefined) {
    return {
      message:
        `replica of port ${masterPort}, not verified: its master failed ` +
        `(see ${master}) — fix the master first`,
    }
  }

  return {
    message:
      `replica of port ${masterPort}, which this run did not flush — ` +
      `nothing emptied it, and it refuses FLUSHALL itself`,
    hint: HINT.staleReplica,
  }
}
