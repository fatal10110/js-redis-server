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

function isPort(value: number): boolean {
  return Number.isInteger(value) && value >= MIN_PORT && value <= MAX_PORT
}

/** Parse one port-valued env var, rejecting anything that is not a TCP port. */
export function parsePort(name: string, raw: string): number {
  const port = Number(raw.trim())
  if (!isPort(port)) {
    throw new Error(
      `${name}="${raw}" is not a TCP port between ${MIN_PORT} and ${MAX_PORT}`,
    )
  }
  return port
}

/**
 * Read an optional single-port env var. Unset or empty means "not configured"
 * (the harness then spawns its own server); anything else must be a valid
 * port, so a typo fails loudly instead of silently connecting somewhere else.
 */
function optionalPort(name: string): number | undefined {
  const raw = process.env[name]?.trim() ?? ''
  return raw === '' ? undefined : parsePort(name, raw)
}

/**
 * Parse `REDIS_CLUSTER_PORTS` — a comma-separated list of cluster node ports.
 * Unset (or empty) means the docker-compose default.
 *
 * These are seeds: the harness's cluster clients discover every node from any
 * one of them, and `clean:redis` likewise flushes the whole topology it finds,
 * so listing a single node is enough. Every entry must still be a valid port —
 * one malformed entry throws rather than being dropped, because silently
 * skipping entries (`"30000,30001-30005"` collapsing to `[30000]`) is exactly
 * the kind of quiet misconfiguration #395 is about.
 */
export function parseClusterPorts(configured: string | undefined): number[] {
  const raw = configured?.trim() ?? ''
  if (raw === '') {
    return [...DEFAULT_CLUSTER_PORTS]
  }

  return raw.split(',').map(entry => {
    const trimmed = entry.trim()
    const port = Number(trimmed)
    if (trimmed === '' || !isPort(port)) {
      throw new Error(
        `REDIS_CLUSTER_PORTS entry "${trimmed}" is not a TCP port between ` +
          `${MIN_PORT} and ${MAX_PORT} (got "${raw}"). Pass a comma-separated ` +
          `list of individual ports — ranges such as "30000-30005" are not ` +
          `supported, spell every port out.`,
      )
    }
    return port
  })
}

/** The real cluster's seed ports for this process. */
export function realClusterPorts(): number[] {
  return parseClusterPorts(process.env.REDIS_CLUSTER_PORTS)
}

/** Port of the docker-compose standalone server, if the run is pointed at one. */
export function realStandalonePort(): number | undefined {
  return optionalPort('REDIS_STANDALONE_PORT')
}

/** Port of the requirepass standalone server, if the run is pointed at one. */
export function realStandaloneAuthPort(): number | undefined {
  return optionalPort('REDIS_STANDALONE_AUTH_PORT')
}
