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
          `list of individual ports — for a range such as "30000-30005", set ` +
          `REDIS_CLUSTER_PORT_RANGE instead.`,
      )
    }
    return port
  })
}

/** How many nodes the docker-compose cluster runs (3 masters + 3 replicas). */
export const CLUSTER_NODE_COUNT = 6

/**
 * Each node's cluster bus listens on its client port + 10000, so that sum has
 * to be a port too.
 */
const CLUSTER_BUS_PORT_OFFSET = 10000

/**
 * Parse `REDIS_CLUSTER_PORT_RANGE` — `first-last`, the six consecutive client
 * ports of one docker-compose cluster. docker-compose.test.yml publishes the
 * cluster on this range (and `docker/redis-cluster-init.sh` binds its nodes to
 * it), so one variable moves the stack and the harness together: a second
 * checkout or worktree can run its own stack beside the default one instead of
 * sharing it and flushing it under a concurrent run (#497).
 *
 * Accepts exactly the ranges the init script accepts — no surrounding
 * whitespace either, since the script (and compose's port mapping) gets the
 * value verbatim. tests/redis-endpoints.test.ts runs the script beside this
 * parser to keep the two in step.
 */
export function parseClusterPortRange(raw: string): number[] {
  const match = /^([1-9]\d*)-([1-9]\d*)$/.exec(raw)
  const first = match ? Number(match[1]) : NaN
  const last = match ? Number(match[2]) : NaN
  const span = CLUSTER_NODE_COUNT - 1

  if (!isPort(first) || !isPort(last) || last - first !== span) {
    throw new Error(
      `REDIS_CLUSTER_PORT_RANGE="${raw}" is not a range of ` +
        `${CLUSTER_NODE_COUNT} consecutive ports — write it as first-last, ` +
        `e.g. "31000-${31000 + span}".`,
    )
  }
  if (last + CLUSTER_BUS_PORT_OFFSET > MAX_PORT) {
    throw new Error(
      `REDIS_CLUSTER_PORT_RANGE="${raw}" puts the cluster bus (client port + ` +
        `${CLUSTER_BUS_PORT_OFFSET}) past port ${MAX_PORT}; pick a range ` +
        `ending at or below ${MAX_PORT - CLUSTER_BUS_PORT_OFFSET}.`,
    )
  }

  return Array.from({ length: CLUSTER_NODE_COUNT }, (_, i) => first + i)
}

/**
 * Resolve the cluster seed ports from the two env vars that can name them.
 * `REDIS_CLUSTER_PORTS` (explicit seeds) wins; otherwise
 * `REDIS_CLUSTER_PORT_RANGE` (the range a compose stack was started on);
 * otherwise the docker-compose default. When both are set, every seed must lie
 * inside the range — seeds pointing at some other cluster than the stack the
 * range describes are a misconfiguration, not a preference.
 */
export function resolveClusterPorts(
  ports: string | undefined,
  range: string | undefined,
): number[] {
  // Unset or empty means the default, as ${REDIS_CLUSTER_PORT_RANGE:-...} does
  // in docker-compose.test.yml; anything else, whitespace included, is parsed.
  const rangePorts =
    range === undefined || range === ''
      ? undefined
      : parseClusterPortRange(range)

  if ((ports?.trim() ?? '') === '') {
    return rangePorts ?? [...DEFAULT_CLUSTER_PORTS]
  }

  const seeds = parseClusterPorts(ports)
  const outside = seeds.filter(
    port => rangePorts !== undefined && !rangePorts.includes(port),
  )
  if (outside.length > 0) {
    throw new Error(
      `REDIS_CLUSTER_PORTS="${ports}" names port(s) ${outside.join(', ')} ` +
        `outside REDIS_CLUSTER_PORT_RANGE="${range}". Unset one of them, or ` +
        `make the seeds part of the range.`,
    )
  }
  return seeds
}

/** The real cluster's seed ports for this process. */
export function realClusterPorts(): number[] {
  return resolveClusterPorts(
    process.env.REDIS_CLUSTER_PORTS,
    process.env.REDIS_CLUSTER_PORT_RANGE,
  )
}

/** Port of the docker-compose standalone server, if the run is pointed at one. */
export function realStandalonePort(): number | undefined {
  return optionalPort('REDIS_STANDALONE_PORT')
}

/** Port of the requirepass standalone server, if the run is pointed at one. */
export function realStandaloneAuthPort(): number | undefined {
  return optionalPort('REDIS_STANDALONE_AUTH_PORT')
}
