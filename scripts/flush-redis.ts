/**
 * Flush every real Redis endpoint the `TEST_BACKEND=real` integration suites
 * talk to, and fail loudly if any of them cannot be reached.
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
 * Endpoints mirror `tests-integration/test-config.ts`:
 *  - the 6-node cluster on 30000-30005 (override with REDIS_CLUSTER_PORTS)
 *  - the standalone server, when REDIS_STANDALONE_PORT is set
 *  - the requirepass standalone, when REDIS_STANDALONE_AUTH_PORT is set
 */
import { Redis } from 'ioredis'

const DEFAULT_CLUSTER_PORTS = [30000, 30001, 30002, 30003, 30004, 30005]

/** Matches docker-compose.test.yml's `--requirepass` for redis-standalone-auth. */
const DEFAULT_AUTH_PASSWORD = 'testpass'

const CONNECT_TIMEOUT_MS = 5000

type Endpoint = {
  label: string
  port: number
  password?: string
  /** Cluster replicas reject FLUSHALL with -READONLY; they follow their master. */
  allowReadonly: boolean
}

function clusterPorts(): number[] {
  const configured = process.env.REDIS_CLUSTER_PORTS
  if (!configured) {
    return DEFAULT_CLUSTER_PORTS
  }

  const ports = configured
    .split(',')
    .map(part => Number(part.trim()))
    .filter(port => Number.isInteger(port) && port > 0)

  if (ports.length === 0) {
    throw new Error(
      `REDIS_CLUSTER_PORTS is set but holds no valid ports: "${configured}"`,
    )
  }

  return ports
}

function endpoints(): Endpoint[] {
  const targets: Endpoint[] = clusterPorts().map(port => ({
    label: `cluster node 127.0.0.1:${port}`,
    port,
    allowReadonly: true,
  }))

  // Only flush the standalone servers the harness was actually pointed at —
  // these are the same env vars test-config.ts uses to decide whether to
  // connect to docker-compose or spawn a local redis-server child.
  const standalonePort = process.env.REDIS_STANDALONE_PORT
  if (standalonePort) {
    targets.push({
      label: `standalone 127.0.0.1:${standalonePort}`,
      port: Number(standalonePort),
      allowReadonly: false,
    })
  }

  const authPort = process.env.REDIS_STANDALONE_AUTH_PORT
  if (authPort) {
    targets.push({
      label: `standalone (auth) 127.0.0.1:${authPort}`,
      port: Number(authPort),
      password:
        process.env.REDIS_STANDALONE_AUTH_PASSWORD ?? DEFAULT_AUTH_PASSWORD,
      allowReadonly: false,
    })
  }

  return targets
}

function isReadonlyReplicaError(err: unknown): boolean {
  return err instanceof Error && err.message.startsWith('READONLY')
}

/** Flush one endpoint. Throws (loudly) on anything other than a replica refusal. */
async function flush(endpoint: Endpoint): Promise<string> {
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

  try {
    await client.connect()

    try {
      await client.flushall()
    } catch (err) {
      if (endpoint.allowReadonly && isReadonlyReplicaError(err)) {
        return `${endpoint.label}: replica, flushed via its master`
      }
      throw err
    }

    const remaining = await client.dbsize()
    if (remaining !== 0) {
      throw new Error(`FLUSHALL left ${remaining} key(s) behind`)
    }

    return `${endpoint.label}: flushed`
  } finally {
    client.disconnect()
  }
}

async function main(): Promise<void> {
  const targets = endpoints()
  const results = await Promise.allSettled(targets.map(flush))
  const failures: string[] = []

  results.forEach((result, index) => {
    if (result.status === 'fulfilled') {
      console.log(`  ${result.value}`)
      return
    }

    const reason =
      result.reason instanceof Error
        ? result.reason.message
        : String(result.reason)
    failures.push(`  ${targets[index].label}: ${reason}`)
  })

  if (failures.length > 0) {
    console.error(
      `clean:redis failed — could not flush ${failures.length} of ${targets.length} endpoint(s):`,
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
  console.error('clean:redis failed:', err)
  process.exitCode = 1
})
