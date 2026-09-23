/**
 * The decisions `scripts/flush-redis.ts` makes about a cluster, kept free of
 * I/O so they can be unit-tested against CLUSTER NODES fixtures instead of by
 * breaking a real cluster (#458):
 *
 *  - which nodes the flush has to cover (`planCluster`),
 *  - which masters were flushed and which failed (`masterOutcomes`), and
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
 * `[N->-id]` / `[N-<-id]` entries describe a migration in progress, appear only
 * on the `myself` line, and do not by themselves mean the node serves a slot.
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

/**
 * A report of a node with no real server behind it to flush or check:
 *
 *  - a `handshake` entry is a CLUSTER MEET in progress, under a temporary id
 *    only the node that issued the MEET knows — so it shows up in one seed's
 *    view and not another's;
 *  - an addressless (`noaddr`, port 0) entry that serves no slots cannot be
 *    connected to and routes no keys.
 *
 * Every other node is covered, slots or not. A master with no slots can still
 * hold keys a client reads: during a slot migration, keys land on the
 * importing node before it is given the slot, and clients reach them through
 * -ASK. Real Redis prints the migration markers only on the `myself` line, so
 * from any other seed that node looks like a plain empty master — slot
 * ownership cannot tell the two apart.
 */
function isPhantom(node: ClusterNode): boolean {
  return (
    node.flags.includes('handshake') || (isAddressless(node) && !node.ownsSlots)
  )
}

export type ClusterPlan = {
  /** The `myself` report of every seed, by id. */
  seeds: Map<string, ClusterNode>
  /** Non-seed nodes the flush must cover. */
  members: ClusterNode[]
  /** Non-seed nodes with no real server behind them (see `isPhantom`). */
  skipped: ClusterNode[]
}

/**
 * Decide which nodes the flush covers, given the CLUSTER NODES view of every
 * seed that answered.
 *
 * Views are merged by node id, and a node is skipped only if every report of
 * it is a phantom, so a seed with a stale view cannot shrink the flush. The
 * rule looks at each node alone — never at its master or at which seed
 * reported it — so a node is covered or skipped the same way whichever seed
 * is used (#458). Seeds answered, so they are always covered.
 */
export function planCluster(views: readonly ClusterNode[][]): ClusterPlan {
  const seeds = new Map<string, ClusterNode>()
  const reports = new Map<string, ClusterNode[]>()

  for (const view of views) {
    for (const node of view) {
      if (node.flags.includes('myself')) {
        seeds.set(node.id, node)
      }
      reports.set(node.id, [...(reports.get(node.id) ?? []), node])
    }
  }

  const members: ClusterNode[] = []
  const skipped: ClusterNode[] = []

  for (const [id, nodeReports] of reports) {
    if (seeds.has(id)) {
      continue
    }
    // Prefer a report that carries a usable address: seeds can disagree while
    // a node's address propagates.
    const node =
      nodeReports.findLast(report => !isAddressless(report)) ??
      nodeReports[nodeReports.length - 1]
    ;(nodeReports.every(isPhantom) ? skipped : members).push(node)
  }

  return { seeds, members, skipped }
}

/**
 * The keys a master is known by, most specific first. Cluster nodes are matched
 * by node id: a replica's CLUSTER NODES line names its master's id even when
 * that master is `noaddr` (port 0), while `INFO replication` still reports the
 * old port. The port is the fallback for what has no known id — a standalone
 * server, or a seed whose CLUSTER NODES could not be read.
 */
function keysOf(id: string | null | undefined, port: number): string[] {
  return id != null ? [`id:${id}`, `port:${port}`] : [`port:${port}`]
}

/** One endpoint of the run, as far as master bookkeeping is concerned. */
export type EndpointOutcome = {
  label: string
  port: number
  /** Its CLUSTER NODES report, for cluster nodes. */
  node?: ClusterNode
  /**
   * The `INFO replication` role, or undefined when the endpoint could not be
   * connected to. CLUSTER NODES then decides whether it is a master.
   */
  role?: 'master' | 'replica'
  failed: boolean
}

export type MasterOutcomes = {
  /** Keys of the masters this run flushed without a failure so far. */
  flushed: Set<string>
  /** key → label of every master this run tried and failed, reachable or not. */
  failed: Map<string, string>
}

/** Sort every master of the run into flushed or failed, by id and by port. */
export function masterOutcomes(
  endpoints: readonly EndpointOutcome[],
): MasterOutcomes {
  const flushed = new Set<string>()
  const failed = new Map<string, string>()

  for (const endpoint of endpoints) {
    const isMaster =
      endpoint.role !== undefined
        ? endpoint.role === 'master'
        : endpoint.node !== undefined && endpoint.node.masterId === null
    if (!isMaster) {
      continue
    }
    for (const key of keysOf(endpoint.node?.id, endpoint.port)) {
      if (endpoint.failed) {
        failed.set(key, endpoint.label)
      } else {
        flushed.add(key)
      }
    }
  }

  return { flushed, failed }
}

/**
 * Judge a replica against its master before looking at its own keyspace.
 * Returns null when the master was flushed and the replica can be checked.
 *
 * A master that was part of this run but failed — unreachable, addressless,
 * FLUSHALL erroring or leaving keys, CLUSTER NODES unreadable — has already
 * been reported against itself with the hint that fits. Its replicas point
 * back at it instead of claiming the run skipped their master and suggesting
 * a replica resync that would not help (#458). Only a master the run never
 * knew about gets the stale-replica diagnosis.
 *
 * @param masterPort `master_port` from the replica's INFO replication.
 * @param masterId the master id from the replica's own CLUSTER NODES line,
 *   when it is a cluster node and that is known.
 */
export function replicaMasterProblem(
  masterPort: number,
  masterId: string | null | undefined,
  masters: MasterOutcomes,
): Failure | null {
  // By id first; the port only when the master's id was never learned.
  for (const key of keysOf(masterId, masterPort)) {
    if (masters.flushed.has(key)) {
      return null
    }
    const master = masters.failed.get(key)
    if (master !== undefined) {
      return {
        message:
          `replica of port ${masterPort}, not verified: its master failed ` +
          `(see ${master}) — fix the master first`,
      }
    }
  }

  return {
    message:
      `replica of port ${masterPort}, which this run did not flush — ` +
      `nothing emptied it, and it refuses FLUSHALL itself`,
    hint: HINT.staleReplica,
  }
}
