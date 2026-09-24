import { asciiLowerCase } from '../ascii-case'
import {
  gateSatisfied,
  type CompatibilityProfile,
  type VersionGate,
} from './profile'

/** Present on every profile that has per-subcommand command-table entries. */
const ALWAYS: VersionGate = { redis: '7.0.0', valkey: '7.2.0' }
const REDIS_72: VersionGate = { redis: '7.2.0', valkey: '7.2.0' }
const VALKEY_80: VersionGate = { valkey: '8.0.0' }
const VALKEY_90: VersionGate = { valkey: '9.0.0' }
// Newer than every preset, so these only matter for a custom
// `{ flavor, version }` spec. Taken from the upstream command tables.
const REDIS_84: VersionGate = { redis: '8.4.0' }

/**
 * The real command-table subcommands of every container this server
 * registers, as real servers list them in `COMMAND INFO` — including the ones
 * this server does not implement. From Redis 7.0 command lookup resolves
 * `container|subcommand` against this table before anything else sees the
 * command, so a real-but-unimplemented subcommand (`ACL CAT`, `CLIENT PAUSE`)
 * passes lookup (and a `noscript` one is still refused from a script); only a
 * name absent from the real table fails lookup as an unknown subcommand.
 *
 * Each entry is `[since, arity]`: the versions that have it, and its
 * command-table arity (command name and subcommand included, negated for a
 * minimum) as `COMMAND INFO` reports it. Lookup checks a call against that
 * arity before the container runs.
 *
 * Captured from redis 7.0.15, 7.2.16, 7.4.11, 8.0.6, 8.4.7 and valkey 7.2.14,
 * 8.0.11, 8.1.0, 9.0.6; the version differences in arity are in
 * {@link containerSubcommandArity}.
 * Redis 6.2 has no subcommand entries at all, so these gates are only
 * consulted on profiles with `error.unknown-subcommand-dispatch-timing` (see
 * `CommandExecutor.plan()`) or `script.per-subcommand-noscript`.
 */
const CONTAINER_SUBCOMMANDS: Record<
  string,
  Record<string, readonly [VersionGate, number]>
> = {
  acl: {
    cat: [ALWAYS, -2],
    deluser: [ALWAYS, -3],
    dryrun: [ALWAYS, -4],
    genpass: [ALWAYS, -2],
    getuser: [ALWAYS, 3],
    help: [ALWAYS, 2],
    list: [ALWAYS, 2],
    load: [ALWAYS, 2],
    log: [ALWAYS, -2],
    save: [ALWAYS, 2],
    setuser: [ALWAYS, -3],
    users: [ALWAYS, 2],
    whoami: [ALWAYS, 2],
  },
  client: {
    caching: [ALWAYS, 3],
    capa: [VALKEY_80, -3],
    getname: [ALWAYS, 2],
    getredir: [ALWAYS, 2],
    help: [ALWAYS, 2],
    id: [ALWAYS, 2],
    'import-source': [{ valkey: '8.1.0' }, 3],
    info: [ALWAYS, 2],
    kill: [ALWAYS, -3],
    list: [ALWAYS, -2],
    'no-evict': [ALWAYS, 3],
    'no-touch': [REDIS_72, 3],
    pause: [ALWAYS, -3],
    reply: [ALWAYS, 3],
    setinfo: [REDIS_72, 4],
    setname: [ALWAYS, 3],
    tracking: [ALWAYS, -3],
    trackinginfo: [ALWAYS, 2],
    unblock: [ALWAYS, -3],
    unpause: [ALWAYS, 2],
  },
  cluster: {
    addslots: [ALWAYS, -3],
    addslotsrange: [ALWAYS, -4],
    bumpepoch: [ALWAYS, 2],
    cancelslotmigrations: [VALKEY_90, 2],
    'count-failure-reports': [ALWAYS, 3],
    countkeysinslot: [ALWAYS, 3],
    delslots: [ALWAYS, -3],
    delslotsrange: [ALWAYS, -4],
    failover: [ALWAYS, -2],
    flushslot: [VALKEY_90, -3],
    flushslots: [ALWAYS, 2],
    forget: [ALWAYS, 3],
    getkeysinslot: [ALWAYS, 4],
    getslotmigrations: [VALKEY_90, 2],
    help: [ALWAYS, 2],
    info: [ALWAYS, 2],
    keyslot: [ALWAYS, 3],
    links: [ALWAYS, 2],
    meet: [ALWAYS, -4],
    migration: [REDIS_84, -4],
    migrateslots: [VALKEY_90, -4],
    myid: [ALWAYS, 2],
    myshardid: [REDIS_72, 2],
    nodes: [ALWAYS, 2],
    replicas: [ALWAYS, 3],
    replicate: [ALWAYS, 3],
    reset: [ALWAYS, -2],
    saveconfig: [ALWAYS, 2],
    'set-config-epoch': [ALWAYS, 3],
    setslot: [ALWAYS, -4],
    shards: [ALWAYS, 2],
    slaves: [ALWAYS, 3],
    'slot-stats': [{ redis: '8.2.0', valkey: '8.0.0' }, -4],
    slots: [ALWAYS, 2],
    syncslots: [{ redis: '8.4.0', valkey: '9.0.0' }, -3],
  },
  command: {
    count: [ALWAYS, 2],
    docs: [ALWAYS, -2],
    getkeys: [ALWAYS, -3],
    getkeysandflags: [ALWAYS, -3],
    help: [ALWAYS, 2],
    info: [ALWAYS, -2],
    list: [ALWAYS, -2],
  },
  config: {
    get: [ALWAYS, -3],
    help: [ALWAYS, 2],
    resetstat: [ALWAYS, 2],
    rewrite: [ALWAYS, 2],
    set: [ALWAYS, -4],
  },
  pubsub: {
    channels: [ALWAYS, -2],
    help: [ALWAYS, 2],
    numpat: [ALWAYS, 2],
    numsub: [ALWAYS, -2],
    shardchannels: [ALWAYS, -2],
    shardnumsub: [ALWAYS, -2],
  },
  script: {
    debug: [ALWAYS, 3],
    exists: [ALWAYS, -3],
    flush: [ALWAYS, -2],
    help: [ALWAYS, 2],
    kill: [ALWAYS, 2],
    load: [ALWAYS, 3],
    show: [VALKEY_80, 3],
  },
  function: {
    delete: [ALWAYS, 3],
    dump: [ALWAYS, 2],
    flush: [ALWAYS, -2],
    help: [ALWAYS, 2],
    kill: [ALWAYS, 2],
    list: [ALWAYS, -2],
    load: [ALWAYS, -3],
    restore: [ALWAYS, -3],
    stats: [ALWAYS, 2],
  },
  slowlog: {
    get: [ALWAYS, -2],
    help: [ALWAYS, 2],
    len: [ALWAYS, 2],
    reset: [ALWAYS, 2],
  },
  xgroup: {
    create: [ALWAYS, -5],
    createconsumer: [ALWAYS, 5],
    delconsumer: [ALWAYS, 5],
    destroy: [ALWAYS, 4],
    help: [ALWAYS, 2],
    setid: [ALWAYS, -5],
  },
  xinfo: {
    consumers: [ALWAYS, 4],
    groups: [ALWAYS, 3],
    help: [ALWAYS, 2],
    stream: [ALWAYS, -3],
  },
}

/**
 * Whether real Redis/Valkey at `profile` has a `container|subcommand` table
 * entry. `subcommand` is matched ASCII-case-insensitively, as real lookup
 * does. `undefined` when `container` is not a container this table models.
 */
export function containerSubcommandExists(
  container: string,
  subcommand: Buffer | string,
  profile: CompatibilityProfile,
): boolean | undefined {
  const subcommands = CONTAINER_SUBCOMMANDS[asciiLowerCase(container)]
  if (!subcommands) {
    return undefined
  }

  // latin1 keeps one code unit per byte, so a non-ASCII name can never fold
  // onto a table entry.
  const name = asciiLowerCase(
    Buffer.isBuffer(subcommand) ? subcommand.toString('latin1') : subcommand,
  )
  const entry = Object.prototype.hasOwnProperty.call(subcommands, name)
    ? subcommands[name]
    : undefined
  return entry !== undefined && gateSatisfied(entry[0], profile)
}

/**
 * The command-table arity of `container|subcommand` at `profile`, or
 * `undefined` when the table has no such entry (lookup then checks the
 * container's own arity). Only meaningful on profiles with per-subcommand
 * table entries (Redis 7.0+ / Valkey 7.2+).
 */
export function containerSubcommandArity(
  container: string,
  subcommand: Buffer | string,
  profile: CompatibilityProfile,
): number | undefined {
  if (containerSubcommandExists(container, subcommand, profile) !== true) {
    return undefined
  }

  const containerName = asciiLowerCase(container)
  const name = asciiLowerCase(
    Buffer.isBuffer(subcommand) ? subcommand.toString('latin1') : subcommand,
  )
  // Redis 7.0 alone required a target command plus an argument (7.0.15:
  // -4); 7.2 relaxed it back to -3. Valkey 9.0 made CLUSTER REPLICATE take
  // an optional argument (-3).
  if (
    containerName === 'command' &&
    (name === 'getkeys' || name === 'getkeysandflags') &&
    !profile.has('command.getkeys-single-arg')
  ) {
    return -4
  }
  if (
    containerName === 'cluster' &&
    name === 'replicate' &&
    profile.flavor === 'valkey' &&
    gateSatisfied({ valkey: '9.0.0' }, profile)
  ) {
    return -3
  }

  return CONTAINER_SUBCOMMANDS[containerName][name][1]
}
