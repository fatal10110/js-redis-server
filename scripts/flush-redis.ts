/**
 * Flush every real Redis endpoint the `TEST_BACKEND=real` integration suites
 * talk to, and fail loudly if any of them is not demonstrably empty afterwards.
 *
 * This replaces the old `clean:redis` one-liner:
 *
 *   redis-cli --cluster call 127.0.0.1:30000 FLUSHALL || redis-cli FLUSHALL \
 *     || echo 'Redis cleanup attempted'
 *
 * which required `redis-cli` on PATH. CI runs Redis as docker-compose services
 * and never installs the client, so both branches failed, the trailing `echo`
 * exited 0, and the keyspace was silently never flushed (#395).
 *
 * Using ioredis (already a devDependency, and the client the suites themselves
 * use) means no external binary — and no `docker compose exec`, whose
 * availability varies between the Compose v1 shim and the CLI plugin.
 *
 * "Flushed" is only ever claimed on evidence, never on a command not erroring:
 *
 *  - REDIS_CLUSTER_PORTS are seeds, not the whole cluster. The harness's
 *    cluster clients discover every node from any one seed, so a run pointed at
 *    a single port still uses every master. The flush therefore reads CLUSTER
 *    NODES from each seed and covers every node it finds; flushing only the
 *    listed ports would leave the unlisted masters full and still exit 0. The
 *    exceptions are entries with no server behind them — a `handshake` (a
 *    CLUSTER MEET in progress, which only the node that issued it lists) or an
 *    addressless entry serving no slots — which are skipped, so the result
 *    does not depend on which seed happened to see them (#458). Masters
 *    without slots are still flushed: mid-migration, the importing node holds
 *    keys clients reach through -ASK before it owns the slot;
 *  - masters are flushed and then asserted empty with DBSIZE;
 *  - replicas refuse FLUSHALL with -READONLY, so they are checked rather than
 *    trusted: their master must be one of the endpoints this run flushed, and
 *    their own DBSIZE must reach 0. A replica whose master link is down keeps a
 *    full stale keyspace while happily refusing the write. When the master
 *    itself failed, the replica points back at it rather than at itself;
 *  - FLUSHALL and DBSIZE are keyless, so they answer normally on a cluster that
 *    is down. Every cluster node therefore also has to report
 *    `cluster_state:ok`, or the next keyed command the suite runs would get
 *    -CLUSTERDOWN against a keyspace we just declared clean.
 *
 * Endpoints mirror `tests-integration/test-config.ts` via the shared
 * `tests-integration/redis-endpoints.ts`:
 *  - the cluster, seeded from REDIS_CLUSTER_PORTS (default 30000-30005)
 *  - the standalone server, when REDIS_STANDALONE_PORT is set
 *  - the requirepass standalone, when REDIS_STANDALONE_AUTH_PORT is set
 */
import { Redis } from 'ioredis'
import {
  realClusterPorts,
  realStandaloneAuthPort,
  realStandalonePort,
  STANDALONE_AUTH_PASSWORD,
} from '../tests-integration/redis-endpoints'
import {
  type ClusterNode,
  type Failure,
  HINT,
  isAddressless,
  type MasterOutcomes,
  masterOutcomes,
  parseClusterNodes,
  planCluster,
  replicaMasterProblem,
} from './flush-redis-topology'

const CONNECT_TIMEOUT_MS = 5000

/** How long a replica gets to catch up with its freshly flushed master. */
const REPLICA_SYNC_TIMEOUT_MS = 5000
const REPLICA_POLL_INTERVAL_MS = 100

/** A failure with a message and, when there is one, the hint that fits it. */
class FlushFailure extends Error {
  constructor(
    message: string,
    readonly hint?: string,
  ) {
    super(message)
  }
}

type Endpoint = {
  label: string
  host: string
  port: number
  /** Cluster members must additionally report `cluster_state:ok`. */
  clustered: boolean
  /**
   * This node's CLUSTER NODES report: set up front for a discovered node, and
   * from its own `myself` line for a seed once discovery has read it.
   */
  node?: ClusterNode
  password?: string
}

type Replication =
  | { role: 'master' }
  | { role: 'replica'; masterPort: number; linkStatus: string }

/** A connected endpoint plus what we have established about it so far. */
type Target = Endpoint & {
  client: Redis
  replication: Replication
  note: string
  failure: Failure | null
}

/**
 * A configured or discovered endpoint that could not even be connected to.
 * `node` (when discovered) tells whether it is a master, so its replicas can
 * point back at it.
 */
type Unreachable = {
  label: string
  failure: Failure
  port: number
  node?: ClusterNode
}

function configuredEndpoints(): Endpoint[] {
  // De-duplicated: listing a seed twice must not flush one node twice and
  // report it as two endpoints.
  const seeds = [...new Set(realClusterPorts())]
  const targets: Endpoint[] = seeds.map(port => ({
    label: `cluster node 127.0.0.1:${port}`,
    host: '127.0.0.1',
    port,
    clustered: true,
  }))

  // Only flush the standalone servers the harness was actually pointed at —
  // these are the same env vars (and the same parser) test-config.ts uses to
  // decide whether to connect to docker-compose or spawn a local child.
  const standalonePort = realStandalonePort()
  if (standalonePort !== undefined) {
    targets.push({
      label: `standalone 127.0.0.1:${standalonePort}`,
      host: '127.0.0.1',
      port: standalonePort,
      clustered: false,
    })
  }

  const authPort = realStandaloneAuthPort()
  if (authPort !== undefined) {
    targets.push({
      label: `standalone (auth) 127.0.0.1:${authPort}`,
      host: '127.0.0.1',
      port: authPort,
      clustered: false,
      password: STANDALONE_AUTH_PASSWORD,
    })
  }

  return targets
}

function describe(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason)
}

function toFailure(reason: unknown, fallbackHint?: string): Failure {
  if (reason instanceof FlushFailure) {
    return { message: reason.message, hint: reason.hint ?? fallbackHint }
  }
  return { message: describe(reason), hint: fallbackHint }
}

function parseInfo(text: string): Map<string, string> {
  const fields = new Map<string, string>()
  for (const line of text.split('\n')) {
    const trimmed = line.trim()
    if (trimmed === '' || trimmed.startsWith('#')) {
      continue
    }
    const separator = trimmed.indexOf(':')
    if (separator > 0) {
      fields.set(trimmed.slice(0, separator), trimmed.slice(separator + 1))
    }
  }
  return fields
}

function connect(endpoint: Endpoint): Redis {
  const client = new Redis({
    host: endpoint.host,
    port: endpoint.port,
    password: endpoint.password,
    lazyConnect: true,
    // Fail fast instead of retrying forever against a Redis that isn't there.
    connectTimeout: CONNECT_TIMEOUT_MS,
    commandTimeout: CONNECT_TIMEOUT_MS,
    maxRetriesPerRequest: 0,
    retryStrategy: () => null,
    enableOfflineQueue: false,
  })
  // ioredis emits 'error' on a failed connect; without a listener that becomes
  // an unhandled event and kills the process before we can report which
  // endpoint failed.
  client.on('error', () => {})
  return client
}

async function replicationOf(client: Redis): Promise<Replication> {
  const info = parseInfo(await client.info('replication'))
  if (info.get('role') !== 'slave') {
    return { role: 'master' }
  }
  return {
    role: 'replica',
    masterPort: Number(info.get('master_port')),
    linkStatus: info.get('master_link_status') ?? 'unknown',
  }
}

async function open(endpoint: Endpoint): Promise<Target> {
  const client = connect(endpoint)
  try {
    await client.connect()
    return {
      ...endpoint,
      client,
      replication: await replicationOf(client),
      note: '',
      failure: null,
    }
  } catch (err) {
    client.disconnect()
    throw err
  }
}

/** Open every endpoint, sorting them into connected targets and unreachable ones. */
async function openAll(
  endpoints: Endpoint[],
  hint: string,
): Promise<{ opened: Target[]; unreachable: Unreachable[] }> {
  const results = await Promise.allSettled(endpoints.map(open))
  const opened: Target[] = []
  const unreachable: Unreachable[] = []

  results.forEach((result, index) => {
    if (result.status === 'fulfilled') {
      opened.push(result.value)
    } else {
      const endpoint = endpoints[index]
      unreachable.push({
        label: endpoint.label,
        failure: toFailure(result.reason, hint),
        port: endpoint.port,
        node: endpoint.node,
      })
    }
  })

  return { opened, unreachable }
}

/**
 * Expand the cluster seeds into the full topology: every node any seed
 * reports in CLUSTER NODES, except entries with no server behind them (see
 * `planCluster`), which are listed as skipped. Nodes are matched by id, so a
 * seed is never opened twice under a second address. A covered node that
 * cannot be reached is a failure, never a silent omission — skipping it would
 * be a partial flush.
 */
async function discoverCluster(
  seeds: Target[],
): Promise<{ discovered: Target[]; unreachable: Unreachable[] }> {
  const views: ClusterNode[][] = []

  for (const seed of seeds) {
    try {
      const view = parseClusterNodes(
        (await seed.client.cluster('NODES')) as string,
      )
      seed.node = view.find(node => node.flags.includes('myself'))
      views.push(view)
    } catch (err) {
      seed.failure = {
        message: `cannot read CLUSTER NODES: ${describe(err)}`,
        hint: HINT.notCluster,
      }
    }
  }

  const { members, skipped } = planCluster(views)
  for (const node of skipped) {
    const address = isAddressless(node) ? '' : ` ${node.host}:${node.port}`
    console.log(
      `  cluster node ${node.id.slice(0, 12)}…${address}: skipped, no server ` +
        `behind this entry (flags: ${node.flags.join(',')})`,
    )
  }

  const addressless = members.filter(isAddressless)
  const reachable = members.filter(node => !isAddressless(node))

  const { opened, unreachable } = await openAll(
    reachable.map(node => {
      const host = node.host === '' ? '127.0.0.1' : node.host
      return {
        label: `cluster node ${host}:${node.port} (discovered)`,
        host,
        port: node.port,
        clustered: true,
        node,
      }
    }),
    HINT.clusterUnhealthy,
  )

  return {
    discovered: opened,
    unreachable: [
      ...unreachable,
      ...addressless.map(node => ({
        label: `cluster node ${node.id.slice(0, 12)}…`,
        port: node.port,
        node,
        failure: {
          message: `listed in CLUSTER NODES without a usable address (flags: ${node.flags.join(',')})`,
          hint: HINT.clusterUnhealthy,
        },
      })),
    ],
  }
}

/** Run one step per target, recording its note or its first failure. */
async function forEachTarget(
  targets: Target[],
  step: (target: Target) => Promise<string>,
): Promise<void> {
  const eligible = targets.filter(target => target.failure === null)
  const results = await Promise.allSettled(eligible.map(step))

  results.forEach((result, index) => {
    const target = eligible[index]
    if (result.status === 'fulfilled') {
      target.note = result.value
    } else {
      target.failure = toFailure(result.reason)
    }
  })
}

async function flushMaster(target: Target): Promise<string> {
  await target.client.flushall()
  const remaining = await target.client.dbsize()
  if (remaining !== 0) {
    throw new FlushFailure(
      `FLUSHALL left ${remaining} key(s) behind`,
      HINT.leftKeys,
    )
  }
  return 'flushed'
}

/**
 * Verify a replica rather than trusting its -READONLY refusal: its master has
 * to be one of the endpoints we just emptied, and it has to actually catch up.
 * A replica whose master failed is reported against that master (see
 * `replicaMasterProblem`).
 *
 * Masters are matched by node id where CLUSTER NODES gave one, else by port:
 * every endpoint here is on the local host, but a replica may report its
 * master under a different name for that host (e.g. `host.docker.internal`),
 * so comparing host strings would be wrong.
 */
async function verifyReplica(
  target: Target,
  masters: MasterOutcomes,
): Promise<string> {
  const replication = target.replication
  if (replication.role !== 'replica') {
    throw new Error('not a replica')
  }

  const problem = replicaMasterProblem(
    replication.masterPort,
    target.node?.masterId,
    masters,
  )
  if (problem !== null) {
    throw new FlushFailure(problem.message, problem.hint)
  }

  const deadline = Date.now() + REPLICA_SYNC_TIMEOUT_MS
  let remaining = await target.client.dbsize()
  while (remaining !== 0 && Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, REPLICA_POLL_INTERVAL_MS))
    remaining = await target.client.dbsize()
  }

  if (remaining !== 0) {
    throw new FlushFailure(
      `replica of port ${replication.masterPort} still holds ${remaining} key(s) ` +
        `after its master was flushed (master_link_status:${replication.linkStatus}) — ` +
        `its keyspace is stale, not clean`,
      HINT.staleReplica,
    )
  }

  return `replica of port ${replication.masterPort}, empty`
}

async function verifyClusterHealthy(target: Target): Promise<string> {
  const info = parseInfo((await target.client.cluster('INFO')) as string)
  const state = info.get('cluster_state')
  if (state !== 'ok') {
    throw new FlushFailure(
      `cluster_state:${state ?? 'unknown'} — FLUSHALL and DBSIZE are keyless and answer ` +
        `anyway, but the next keyed command would get -CLUSTERDOWN`,
      HINT.clusterUnhealthy,
    )
  }
  return `${target.note}, cluster_state:ok`
}

async function main(): Promise<void> {
  const configured = await openAll(configuredEndpoints(), HINT.unreachable)
  const seeds = configured.opened.filter(target => target.clustered)
  const { discovered, unreachable: lost } = await discoverCluster(seeds)

  const targets = [...configured.opened, ...discovered]
  const unreachable = [...configured.unreachable, ...lost]

  try {
    // Masters first: a replica can only be verified once whatever feeds it has
    // been emptied, so these two steps cannot overlap.
    const masters = targets.filter(
      t => t.failure === null && t.replication.role === 'master',
    )
    await forEachTarget(masters, flushMaster)

    // Every master this run flushed, or tried and failed — including the ones
    // it could not even connect to — so replicas of a failed master blame the
    // master, not themselves.
    const outcomes = masterOutcomes([
      ...unreachable.map(({ label, port, node }) => ({
        label,
        port,
        node,
        failed: true,
      })),
      ...targets.map(({ label, port, node, replication, failure }) => ({
        label,
        port,
        node,
        role: replication.role,
        failed: failure !== null,
      })),
    ])

    const replicas = targets.filter(t => t.replication.role === 'replica')
    await forEachTarget(replicas, target => verifyReplica(target, outcomes))

    // Last: a mid-flush cluster can legitimately report a transient state.
    await forEachTarget(
      targets.filter(t => t.clustered),
      verifyClusterHealthy,
    )

    for (const target of targets) {
      if (target.failure === null) {
        console.log(`  ${target.label}: ${target.note}`)
      }
    }
  } finally {
    for (const target of targets) {
      target.client.disconnect()
    }
  }

  const failures: { label: string; failure: Failure }[] = [
    ...unreachable,
    ...targets
      .filter(t => t.failure !== null)
      .map(t => ({ label: t.label, failure: t.failure as Failure })),
  ]

  if (failures.length === 0) {
    return
  }

  const total = targets.length + unreachable.length
  console.error(
    `clean:redis failed — ${failures.length} of ${total} endpoint(s) are not verifiably empty:`,
  )
  for (const { label, failure } of failures) {
    console.error(`  ${label}: ${failure.message}`)
  }

  const hints = [
    ...new Set(failures.map(f => f.failure.hint).filter(h => h !== undefined)),
  ]
  if (hints.length > 0) {
    console.error('What to do:')
    for (const hint of hints) {
      console.error(`  - ${hint}`)
    }
  }
  process.exitCode = 1
}

main().catch(err => {
  console.error('clean:redis failed:', describe(err))
  process.exitCode = 1
})
