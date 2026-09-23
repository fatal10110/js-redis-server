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
 * 8.0.11, 9.0.6. Redis 6.2 has no subcommand entries at all, so these gates
 * are only consulted on profiles with `error.unknown-subcommand-dispatch-timing`
 * (see `CommandExecutor.plan()`) or `script.per-subcommand-noscript`.
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
    'import-source': VALKEY_90,
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
    'slot-stats': VALKEY_80,
    slots: ALWAYS,
    syncslots: VALKEY_90,
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
