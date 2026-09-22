/**
 * Where the real Redis backends live, and how to parse the env vars that point
 * at them.
 *
 * Shared by the test harness (`tests-integration/test-config.ts`) and the
 * `clean:redis` script (`scripts/flush-redis.ts`) so the two can never disagree
 * about which endpoints exist — a flush that cleans a different set of nodes
 * than the suite then talks to is the same class of silent failure as #395.
 */

/** Ports docker-compose.test.yml publishes the cluster on. */
export const DEFAULT_CLUSTER_PORTS: readonly number[] = [
  30000, 30001, 30002, 30003, 30004, 30005,
]

/** Password for the password-protected standalone (docker-compose `--requirepass`). */
export const STANDALONE_AUTH_PASSWORD = 'testpass'

const MIN_PORT = 1
const MAX_PORT = 65535

function invalidPort(name: string, entry: string, raw: string): Error {
  return new Error(
    `${name} entry "${entry}" is not a TCP port between ${MIN_PORT} and ${MAX_PORT} ` +
      `(got "${raw}"). Pass a comma-separated list of individual ports — ranges ` +
      `such as "30000-30005" are not supported, spell every port out.`,
  )
}

/** Parse one port-valued env var, rejecting anything that is not a TCP port. */
export function parsePort(name: string, raw: string): number {
  const port = Number(raw.trim())
  if (!Number.isInteger(port) || port < MIN_PORT || port > MAX_PORT) {
    throw invalidPort(name, raw.trim(), raw)
  }
  return port
}

/**
 * Parse `REDIS_CLUSTER_PORTS` — a comma-separated list of every cluster node's
 * port. Unset (or empty) means the docker-compose default.
 *
 * Every entry must be a valid port: one malformed entry throws rather than
 * being dropped. Silently skipping entries would let `"30000,30001-30005"`
 * collapse to a single node, so the flush would clean one node of six and
 * still report success — precisely the failure #395 is about.
 */
export function parseClusterPorts(configured: string | undefined): number[] {
  const raw = configured?.trim() ?? ''
  if (raw === '') {
    return [...DEFAULT_CLUSTER_PORTS]
  }

  return raw.split(',').map(entry => {
    const trimmed = entry.trim()
    if (trimmed === '') {
      throw invalidPort('REDIS_CLUSTER_PORTS', trimmed, raw)
    }
    return parsePort('REDIS_CLUSTER_PORTS', trimmed)
  })
}

/** The real cluster's node ports for this process. */
export function realClusterPorts(): number[] {
  return parseClusterPorts(process.env.REDIS_CLUSTER_PORTS)
}
