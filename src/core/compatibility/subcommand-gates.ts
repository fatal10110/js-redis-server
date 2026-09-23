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
 * Captured from redis 7.0.15, 7.2.16, 7.4.11, 8.0.6 and valkey 7.2.14,
 * 8.0.11, 8.1.0, 9.0.6; the redis 8.2 / 8.4 entries come from upstream.
 * Redis 6.2 has no subcommand entries at all, so these gates are only
 * consulted on profiles with `error.unknown-subcommand-dispatch-timing` (see
 * `CommandExecutor.plan()`) or `script.per-subcommand-noscript`.
 */
const CONTAINER_SUBCOMMANDS: Record<string, Record<string, VersionGate>> = {
  acl: {
    cat: ALWAYS,
    deluser: ALWAYS,
    dryrun: ALWAYS,
    genpass: ALWAYS,
    getuser: ALWAYS,
    help: ALWAYS,
    list: ALWAYS,
    load: ALWAYS,
    log: ALWAYS,
    save: ALWAYS,
    setuser: ALWAYS,
    users: ALWAYS,
    whoami: ALWAYS,
  },
  client: {
    caching: ALWAYS,
    capa: VALKEY_80,
    getname: ALWAYS,
    getredir: ALWAYS,
    help: ALWAYS,
    id: ALWAYS,
    'import-source': { valkey: '8.1.0' },
    info: ALWAYS,
    kill: ALWAYS,
    list: ALWAYS,
    'no-evict': ALWAYS,
    'no-touch': REDIS_72,
    pause: ALWAYS,
    reply: ALWAYS,
    setinfo: REDIS_72,
    setname: ALWAYS,
    tracking: ALWAYS,
    trackinginfo: ALWAYS,
    unblock: ALWAYS,
    unpause: ALWAYS,
  },
  cluster: {
    addslots: ALWAYS,
    addslotsrange: ALWAYS,
    bumpepoch: ALWAYS,
    cancelslotmigrations: VALKEY_90,
    'count-failure-reports': ALWAYS,
    countkeysinslot: ALWAYS,
    delslots: ALWAYS,
    delslotsrange: ALWAYS,
    failover: ALWAYS,
    flushslot: VALKEY_90,
    flushslots: ALWAYS,
    forget: ALWAYS,
    getkeysinslot: ALWAYS,
    getslotmigrations: VALKEY_90,
    help: ALWAYS,
    info: ALWAYS,
    keyslot: ALWAYS,
    links: ALWAYS,
    meet: ALWAYS,
    migration: REDIS_84,
    migrateslots: VALKEY_90,
    myid: ALWAYS,
    myshardid: REDIS_72,
    nodes: ALWAYS,
    replicas: ALWAYS,
    replicate: ALWAYS,
    reset: ALWAYS,
    saveconfig: ALWAYS,
    'set-config-epoch': ALWAYS,
    setslot: ALWAYS,
    shards: ALWAYS,
    slaves: ALWAYS,
    'slot-stats': { redis: '8.2.0', valkey: '8.0.0' },
    slots: ALWAYS,
    syncslots: { redis: '8.4.0', valkey: '9.0.0' },
  },
  command: {
    count: ALWAYS,
    docs: ALWAYS,
    getkeys: ALWAYS,
    getkeysandflags: ALWAYS,
    help: ALWAYS,
    info: ALWAYS,
    list: ALWAYS,
  },
  config: {
    get: ALWAYS,
    help: ALWAYS,
    resetstat: ALWAYS,
    rewrite: ALWAYS,
    set: ALWAYS,
  },
  pubsub: {
    channels: ALWAYS,
    help: ALWAYS,
    numpat: ALWAYS,
    numsub: ALWAYS,
    shardchannels: ALWAYS,
    shardnumsub: ALWAYS,
  },
  script: {
    debug: ALWAYS,
    exists: ALWAYS,
    flush: ALWAYS,
    help: ALWAYS,
    kill: ALWAYS,
    load: ALWAYS,
    show: VALKEY_80,
  },
  function: {
    delete: ALWAYS,
    dump: ALWAYS,
    flush: ALWAYS,
    help: ALWAYS,
    kill: ALWAYS,
    list: ALWAYS,
    load: ALWAYS,
    restore: ALWAYS,
    stats: ALWAYS,
  },
  slowlog: {
    get: ALWAYS,
    help: ALWAYS,
    len: ALWAYS,
    reset: ALWAYS,
  },
  xgroup: {
    create: ALWAYS,
    createconsumer: ALWAYS,
    delconsumer: ALWAYS,
    destroy: ALWAYS,
    help: ALWAYS,
    setid: ALWAYS,
  },
  xinfo: {
    consumers: ALWAYS,
    groups: ALWAYS,
    help: ALWAYS,
    stream: ALWAYS,
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
  const gate = Object.prototype.hasOwnProperty.call(subcommands, name)
    ? subcommands[name]
    : undefined
  return gate !== undefined && gateSatisfied(gate, profile)
}

/**
 * Each real `container|subcommand` entry's command-table arity (command name
 * and subcommand included, negated for a minimum), as `COMMAND INFO` reports
 * it. Command lookup checks a call against this before the container runs, so
 * inside MULTI a count it rejects is refused at queue time, and from a script
 * it is the scripting layer's arity error. Captured from redis-server 8.0.6,
 * with the Valkey-only entries from valkey-server 9.0.6; the version
 * differences are in {@link containerSubcommandArity}.
 */
const SUBCOMMAND_ARITY: Record<string, Record<string, number>> = {
  acl: {
    cat: -2,
    deluser: -3,
    dryrun: -4,
    genpass: -2,
    getuser: 3,
    help: 2,
    list: 2,
    load: 2,
    log: -2,
    save: 2,
    setuser: -3,
    users: 2,
    whoami: 2,
  },
  client: {
    caching: 3,
    capa: -3,
    getname: 2,
    getredir: 2,
    help: 2,
    id: 2,
    'import-source': 3,
    info: 2,
    kill: -3,
    list: -2,
    'no-evict': 3,
    'no-touch': 3,
    pause: -3,
    reply: 3,
    setinfo: 4,
    setname: 3,
    tracking: -3,
    trackinginfo: 2,
    unblock: -3,
    unpause: 2,
  },
  cluster: {
    addslots: -3,
    addslotsrange: -4,
    bumpepoch: 2,
    cancelslotmigrations: 2,
    'count-failure-reports': 3,
    countkeysinslot: 3,
    delslots: -3,
    delslotsrange: -4,
    failover: -2,
    flushslot: -3,
    flushslots: 2,
    forget: 3,
    getkeysinslot: 4,
    getslotmigrations: 2,
    help: 2,
    info: 2,
    keyslot: 3,
    links: 2,
    meet: -4,
    migrateslots: -4,
    myid: 2,
    myshardid: 2,
    nodes: 2,
    replicas: 3,
    replicate: 3,
    reset: -2,
    saveconfig: 2,
    'set-config-epoch': 3,
    setslot: -4,
    shards: 2,
    slaves: 3,
    'slot-stats': -4,
    slots: 2,
    syncslots: -3,
  },
  command: {
    count: 2,
    docs: -2,
    getkeys: -3,
    getkeysandflags: -3,
    help: 2,
    info: -2,
    list: -2,
  },
  config: {
    get: -3,
    help: 2,
    resetstat: 2,
    rewrite: 2,
    set: -4,
  },
  function: {
    delete: 3,
    dump: 2,
    flush: -2,
    help: 2,
    kill: 2,
    list: -2,
    load: -3,
    restore: -3,
    stats: 2,
  },
  pubsub: {
    channels: -2,
    help: 2,
    numpat: 2,
    numsub: -2,
    shardchannels: -2,
    shardnumsub: -2,
  },
  script: {
    debug: 3,
    exists: -3,
    flush: -2,
    help: 2,
    kill: 2,
    load: 3,
    show: 3,
  },
  slowlog: {
    get: -2,
    help: 2,
    len: 2,
    reset: 2,
  },
  xgroup: {
    create: -5,
    createconsumer: 5,
    delconsumer: 5,
    destroy: 4,
    help: 2,
    setid: -5,
  },
  xinfo: {
    consumers: 4,
    groups: 3,
    help: 2,
    stream: -3,
  },
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

  const arities = SUBCOMMAND_ARITY[containerName]
  return arities && Object.prototype.hasOwnProperty.call(arities, name)
    ? arities[name]
    : undefined
}
