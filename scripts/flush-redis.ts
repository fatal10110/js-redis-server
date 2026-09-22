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
 *    NODES from each seed and covers the whole topology it finds; flushing only
 *    the listed ports would leave the unlisted masters full and still exit 0;
 *  - masters are flushed and then asserted empty with DBSIZE;
 *  - replicas refuse FLUSHALL with -READONLY, so they are checked rather than
 *    trusted: their master must be one of the endpoints this run flushed, and
 *    their own DBSIZE must reach 0. A replica whose master link is down keeps a
 *    full stale keyspace while happily refusing the write;
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

const CONNECT_TIMEOUT_MS = 5000

/** How long a replica gets to catch up with its freshly flushed master. */
const REPLICA_SYNC_TIMEOUT_MS = 5000
const REPLICA_POLL_INTERVAL_MS = 100

/** What to actually do about each kind of failure — never one generic line. */
const HINT = {
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
  password?: string
}

type Replication =
  | { role: 'master' }
  | { role: 'replica'; masterPort: number; linkStatus: string }

type Failure = { message: string; hint?: string }

/** A connected endpoint plus what we have established about it so far. */
type Target = Endpoint & {
  client: Redis
  replication: Replication
  note: string
  failure: Failure | null
}

type ClusterNode = {
  id: string
  host: string
  port: number
  flags: string[]
}

/** A configured or discovered endpoint that could not even be connected to. */
type Unreachable = { label: string; failure: Failure }

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

/**
 * Parse CLUSTER NODES: `<id> <ip:port@cport[,hostname]> <flags> ...` per line.
 * An empty ip (a node that has not learned its own address yet) is left empty
 * so the caller can decide how to treat it.
 */
function parseClusterNodes(text: string): ClusterNode[] {
  return text
    .split('\n')
    .map(line => line.trim())
    .filter(line => line !== '')
    .map(line => {
      const [id, address = '', flags = ''] = line.split(' ')
      const hostPort = address.split('@')[0]
      const separator = hostPort.lastIndexOf(':')
      return {
        id,
        host: hostPort.slice(0, separator),
        port: Number(hostPort.slice(separator + 1)),
        flags: flags.split(','),
      }
    })
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
      unreachable.push({
        label: endpoints[index].label,
        failure: toFailure(result.reason, hint),
      })
    }
  })

  return { opened, unreachable }
}

/**
 * Expand the cluster seeds into the full topology. Every node any seed reports
 * in CLUSTER NODES becomes a target; nodes are matched by id, so a seed is
 * never opened twice under a second address. A node that is listed but cannot
 * be reached is a failure, never a silent omission — skipping it would be a
 * partial flush.
 */
async function discoverCluster(
  seeds: Target[],
): Promise<{ discovered: Target[]; unreachable: Unreachable[] }> {
  const topology = new Map<string, ClusterNode>()
  const seedIds = new Set<string>()

  for (const seed of seeds) {
    try {
      const nodes = parseClusterNodes(
        (await seed.client.cluster('NODES')) as string,
      )
      for (const node of nodes) {
        if (node.flags.includes('myself')) {
          seedIds.add(node.id)
        }
        topology.set(node.id, node)
      }
    } catch (err) {
      seed.failure = {
        message: `cannot read CLUSTER NODES: ${describe(err)}`,
        hint: HINT.notCluster,
      }
    }
  }

  const unlisted = [...topology.values()].filter(node => !seedIds.has(node.id))
  const addressless = unlisted.filter(
    node => node.port <= 0 || node.flags.includes('noaddr'),
  )
  const reachable = unlisted.filter(node => !addressless.includes(node))

  const { opened, unreachable } = await openAll(
    reachable.map(node => {
      const host = node.host === '' ? '127.0.0.1' : node.host
      return {
        label: `cluster node ${host}:${node.port} (discovered)`,
        host,
        port: node.port,
        clustered: true,
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
 *
 * Masters are matched by port: every endpoint here is on the local host, but a
 * replica may report its master under a different name for that host (e.g.
 * `host.docker.internal`), so comparing host strings would be wrong.
 */
async function verifyReplica(
  target: Target,
  flushedPorts: ReadonlySet<number>,
): Promise<string> {
  const replication = target.replication
  if (replication.role !== 'replica') {
    throw new Error('not a replica')
  }

  if (!flushedPorts.has(replication.masterPort)) {
    throw new FlushFailure(
      `replica of port ${replication.masterPort}, which this run did not flush — ` +
        `nothing emptied it, and it refuses FLUSHALL itself`,
      HINT.staleReplica,
    )
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

    const flushedPorts = new Set(
      masters.filter(t => t.failure === null).map(t => t.port),
    )
    const replicas = targets.filter(t => t.replication.role === 'replica')
    await forEachTarget(replicas, target => verifyReplica(target, flushedPorts))

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

  const failures: Unreachable[] = [
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
