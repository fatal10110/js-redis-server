import {
  gateSatisfied,
  type CompatibilityProfile,
  type VersionGate,
} from './profile'

/** Present on every profile that has per-subcommand command-table entries. */
const ALWAYS: VersionGate = { redis: '7.0.0', valkey: '7.2.0' }
const REDIS_72: VersionGate = { redis: '7.2.0', valkey: '7.2.0' }

/**
 * The real command-table subcommands of every `noscript` container, as real
 * servers list them in `COMMAND INFO` — including the ones this server does
 * not implement. A Redis 7.0+ script resolves `container|subcommand` before
 * checking `noscript`, so a real-but-unimplemented subcommand (`ACL CAT`,
 * `CLIENT PAUSE`) is still refused as not allowed; only a name absent from
 * the real table fails lookup as an unknown command.
 *
 * Captured from redis 7.0.15, 7.2.16, 7.4.11, 8.0.6 and valkey 7.2.14,
 * 8.0.11, 9.0.6. Redis 6.2 has no subcommand entries at all, so these gates
 * are only consulted on profiles with `script.per-subcommand-noscript`.
 */
const NOSCRIPT_CONTAINER_SUBCOMMANDS: Record<
  string,
  Record<string, VersionGate>
> = {
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
    capa: { valkey: '8.0.0' },
    getname: ALWAYS,
    getredir: ALWAYS,
    help: ALWAYS,
    id: ALWAYS,
    'import-source': { valkey: '9.0.0' },
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
  config: {
    get: ALWAYS,
    help: ALWAYS,
    resetstat: ALWAYS,
    rewrite: ALWAYS,
    set: ALWAYS,
  },
  script: {
    debug: ALWAYS,
    exists: ALWAYS,
    flush: ALWAYS,
    help: ALWAYS,
    kill: ALWAYS,
    load: ALWAYS,
    show: { valkey: '8.0.0' },
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
}

/**
 * Whether real Redis/Valkey at `profile` has a `container|subcommand` table
 * entry for a `noscript` container. `undefined` when the container is not
 * one this table models.
 */
export function noscriptSubcommandExists(
  container: string,
  subcommand: string,
  profile: CompatibilityProfile,
): boolean | undefined {
  const subcommands = NOSCRIPT_CONTAINER_SUBCOMMANDS[container.toLowerCase()]
  if (!subcommands) {
    return undefined
  }

  const gate = Object.prototype.hasOwnProperty.call(subcommands, subcommand)
    ? subcommands[subcommand]
    : undefined
  return gate !== undefined && gateSatisfied(gate, profile)
}
