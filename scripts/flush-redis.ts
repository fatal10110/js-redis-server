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
 *  - masters are flushed and then asserted empty with DBSIZE;
 *  - replicas refuse FLUSHALL with -READONLY, so they are checked rather than
 *    trusted: their master must be one of the endpoints this run flushed, and
 *    their own DBSIZE must reach 0. A replica whose master link is down keeps a
 *    full stale keyspace while happily refusing the write;
 *  - FLUSHALL and DBSIZE are keyless, so they answer normally on a cluster that
 *    is down. Every cluster endpoint therefore also has to report
 *    `cluster_state:ok`, or the next keyed command the suite runs would get
 *    -CLUSTERDOWN against a keyspace we just declared clean.
 *
 * Endpoints mirror `tests-integration/test-config.ts` via the shared
 * `tests-integration/redis-endpoints.ts`:
 *  - the cluster nodes (REDIS_CLUSTER_PORTS, default 30000-30005)
 *  - the standalone server, when REDIS_STANDALONE_PORT is set
 *  - the requirepass standalone, when REDIS_STANDALONE_AUTH_PORT is set
 */
import { Redis } from 'ioredis'
import {
  parsePort,
  realClusterPorts,
  STANDALONE_AUTH_PASSWORD,
} from '../tests-integration/redis-endpoints'

const CONNECT_TIMEOUT_MS = 5000

/** How long a replica gets to catch up with its freshly flushed master. */
const REPLICA_SYNC_TIMEOUT_MS = 5000
const REPLICA_POLL_INTERVAL_MS = 100

type Endpoint = {
  label: string
  port: number
  /** Cluster members must additionally report `cluster_state:ok`. */
  clustered: boolean
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
  failure: string | null
}

function endpoints(): Endpoint[] {
  const targets: Endpoint[] = realClusterPorts().map(port => ({
    label: `cluster node 127.0.0.1:${port}`,
    port,
    clustered: true,
  }))

  // Only flush the standalone servers the harness was actually pointed at —
  // these are the same env vars test-config.ts uses to decide whether to
  // connect to docker-compose or spawn a local redis-server child.
  const standalonePort = process.env.REDIS_STANDALONE_PORT
  if (standalonePort) {
    const port = parsePort('REDIS_STANDALONE_PORT', standalonePort)
    targets.push({
      label: `standalone 127.0.0.1:${port}`,
      port,
      clustered: false,
    })
  }

  const authPort = process.env.REDIS_STANDALONE_AUTH_PORT
  if (authPort) {
    const port = parsePort('REDIS_STANDALONE_AUTH_PORT', authPort)
    targets.push({
      label: `standalone (auth) 127.0.0.1:${port}`,
      port,
      clustered: false,
      password: STANDALONE_AUTH_PASSWORD,
    })
  }

  return targets
}

function describe(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason)
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
    host: '127.0.0.1',
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
      target.failure = describe(result.reason)
    }
  })
}

async function flushMaster(target: Target): Promise<string> {
  await target.client.flushall()
  const remaining = await target.client.dbsize()
  if (remaining !== 0) {
    throw new Error(`FLUSHALL left ${remaining} key(s) behind`)
  }
  return 'flushed'
}

/**
 * Verify a replica rather than trusting its -READONLY refusal: its master has
 * to be one of the endpoints we just emptied, and it has to actually catch up.
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
    throw new Error(
      `replica of 127.0.0.1:${replication.masterPort}, which this run did not flush — ` +
        `nothing emptied it, and it refuses FLUSHALL itself`,
    )
  }

  const deadline = Date.now() + REPLICA_SYNC_TIMEOUT_MS
  let remaining = await target.client.dbsize()
  while (remaining !== 0 && Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, REPLICA_POLL_INTERVAL_MS))
    remaining = await target.client.dbsize()
  }

  if (remaining !== 0) {
    throw new Error(
      `replica of 127.0.0.1:${replication.masterPort} still holds ${remaining} key(s) ` +
        `after its master was flushed (master_link_status:${replication.linkStatus}) — ` +
        `its keyspace is stale, not clean`,
    )
  }

  return `replica of 127.0.0.1:${replication.masterPort}, empty`
}

async function verifyClusterHealthy(target: Target): Promise<string> {
  const info = parseInfo((await target.client.cluster('INFO')) as string)
  const state = info.get('cluster_state')
  if (state !== 'ok') {
    throw new Error(
      `cluster_state:${state ?? 'unknown'} — FLUSHALL and DBSIZE are keyless and answer ` +
        `anyway, but the next keyed command would get -CLUSTERDOWN`,
    )
  }
  return `${target.note}, cluster_state:ok`
}

async function main(): Promise<void> {
  const configured = endpoints()
  const targets: Target[] = []
  const unreachable: string[] = []

  const opened = await Promise.allSettled(
    configured.map(async endpoint => {
      const client = connect(endpoint)
      try {
        await client.connect()
        return {
          ...endpoint,
          client,
          replication: await replicationOf(client),
          note: '',
          failure: null,
        } satisfies Target
      } catch (err) {
        client.disconnect()
        throw err
      }
    }),
  )

  opened.forEach((result, index) => {
    if (result.status === 'fulfilled') {
      targets.push(result.value)
    } else {
      unreachable.push(
        `  ${configured[index].label}: ${describe(result.reason)}`,
      )
    }
  })

  try {
    // Masters first: a replica can only be verified once whatever feeds it has
    // been emptied, so these two steps cannot overlap.
    const masters = targets.filter(t => t.replication.role === 'master')
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

  const failures = [
    ...unreachable,
    ...targets
      .filter(t => t.failure !== null)
      .map(t => `  ${t.label}: ${t.failure}`),
  ]

  if (failures.length > 0) {
    console.error(
      `clean:redis failed — ${failures.length} of ${configured.length} endpoint(s) ` +
        `are not verifiably empty:`,
    )
    for (const failure of failures) {
      console.error(failure)
    }
    console.error(
      'Start the test backends first: docker compose -f docker-compose.test.yml up -d --wait',
    )
    process.exitCode = 1
  }
}

main().catch(err => {
  console.error('clean:redis failed:', describe(err))
  process.exitCode = 1
})
