import { FEATURE_GATES } from './feature-gates'

export type RedisFlavor = 'redis' | 'valkey'

/**
 * The first version of each flavor that has a behavior: one minimum (`'7.0.0'`:
 * that version and every later one), or per-branch minimums for a change
 * backported to patch releases (`['7.2.16', '7.4.11', '8.2.9']`: from that
 * patch on within each listed `major.minor` branch, and every version of a
 * branch newer than the last one listed). A version in a branch older than the
 * last entry that is not listed (8.0.x above) does not have it. A missing
 * flavor never does. See {@link gateSatisfied}.
 */
export type VersionGate = {
  redis?: string | readonly string[]
  valkey?: string | readonly string[]
}

export type FeatureId =
  | 'expire.conditions'
  | 'set.get'
  | 'set.nx-get'
  | 'set.exat-pxat'
  | 'command.docs'
  | 'command.info-extended-fields'
  | 'command.getkeysandflags'
  | 'command.list'
  | 'command.help-valkey-wording'
  | 'command.info-subcommands-array'
  | 'acl.dryrun'
  | 'config.set.failure-message'
  | 'config.set.multi-pair'
  | 'config.memory-value.reject-overflow'
  | 'config.redis7-table'
  | 'config.get.multi-pattern'
  | 'config.loglevel-nothing'
  | 'config.maxmemory-samples-max-64'
  | 'notify.keyspace.new-key-class'
  | 'notify.keyspace.keymiss-store-sources'
  | 'notify.keyspace.keymiss-hgetex'
  | 'protocol.multibulk-count-int-max'
  | 'protocol.header-scan-past-nul'
  | 'protocol.inline-adjacent-quotes'
  | 'client.no-evict'
  | 'client.kill.maxage'
  | 'client.kill.id-positive'
  | 'client.kill.id-list'
  | 'client.kill.type-primary'
  | 'client.setinfo'
  | 'client.no-touch'
  | 'client.pause.extra-args'
  | 'timeout.ms-overflow-check'
  | 'client.list.redis7-fields'
  | 'client.list.watch'
  | 'client.list.io-thread'
  | 'client.list.net-stats'
  | 'client.list.capa'
  | 'info.keyspace.subexpiry'
  | 'info.keyspace.volatile-items'
  | 'error.unknown-subcommand-wording'
  | 'error.unknown-subcommand-dispatch-timing'
  | 'reply.help-print-wording'
  | 'xgroup.help-entriesread'
  | 'error.unknown-command-wording'
  | 'docs.summary-7.2-wording'
  | 'stream.xread-unbalanced-wording'
  | 'stream.xread-unbalanced-plus-wording'
  | 'error.odd-pairs-arity-wording'
  | 'error.replica-keyspace-wording'
  | 'error.subscribed-context-wording'
  | 'info.multi-section'
  | 'shutdown.now-force-abort'
  | 'pubsub.sharded'
  | 'pubsub.resp3-publish-reply-first'
  | 'stream.xautoclaim-deleted-ids'
  | 'stream.consumer-group-lag'
  | 'stream.consumer-active-time'
  | 'bit.byte-bit-range'
  | 'bitcount.start-only'
  | 'hscan.novalues'
  | 'zscan.noscores'
  | 'scan.type-unknown-error'
  | 'xread.plus-id'
  | 'stream.implicit-consumer-event'
  | 'stream.group-lag-tombstone-check'
  | 'stream.group-lag-read-first-id-check'
  | 'stream.group-lag-shortcuts'
  | 'cluster.multi-db'
  | 'cluster.standalone-readwrite-refused'
  | 'cluster.standalone-readonly-allowed'
  | 'cluster.info-links-buffer-line'
  | 'cluster.info-node-fail-counts'
  | 'cluster.crossslot-before-unserved'
  | 'cluster.ask-at-exec'
  | 'cluster.migrating-mixed-keys-tryagain'
  | 'cluster.setslot-extra-args-ignored'
  | 'script.cluster-slot-not-served'
  | 'script.cluster-crossslot-command-wording'
  | 'script.cluster-tryagain-wording'
  | 'sort.cluster-pattern-slot'
  | 'sort.cluster-get-hash'
  | 'reply.double-fpconv'
  | 'zset.listpack-integer-scores'
  | 'zset.add-size-hint'
  | 'zset.add-longest-member-hint'
  | 'zset.rangestore-unknown-length-skiplist'
  | 'zset.rangestore-unknown-length-listpack'
  | 'geo.coord-d2string'
  | 'command.quit-table-entry'
  | 'script.abort-error-suffix'
  | 'script.unknown-command-valkey-wording'
  | 'script.not-allowed-valkey-wording'
  | 'script.redis-version-props'
  | 'script.noscript-short-wording'
  | 'script.big-number-verbatim-returns'
  | 'script.shebang'
  | 'script.shebang-engine-lookup'
  | 'command.getkeys-single-arg'
  | 'zrank.withscore'
  | 'xsetid.entries-added'
  | 'set.listpack-encoding'
  | 'set.union-diff-hashtable'
  | 'set.past-deadline-deletes'
  | 'notify.past-deadline-expired-event'
  | 'script.stream-block-allowed'
  | 'may-replicate.redis7-table'
  | 'may-replicate.cluster-setslot'
  | 'may-replicate.cluster-syncslots'
  | 'double.fast-float'
  | 'double.valkey-strtod'
  | 'help.client-redis7-text'
  | 'help.client-capa'
  | 'help.client-valkey81-filters'
  | 'help.client-valkey90-filters'
  | 'help.slowlog-get-all'
  | 'help.acl-setuser-rules'
  | 'help.script-valkey-text'
  | 'help.function-no-library-description'
  | 'error.lpos-rank-zero-hint'
  | 'error.expire-time-quoted-wording'
  | 'error.no-multi-named-wording'
  | 'zset.pop-count-positive'
  | 'pubsub.unsubscribe-nosub'
  | 'pubsub.unsubscribe-nosub-dropped'
  | 'transaction.refusal-empties-queue'
  | 'transaction.watch-dirty-stops-queue'
  | 'timeout.ceil-ms'
  | 'lpos.rank-range-check'
  | 'geo.unit-case-folding'
  | 'geo.store-with-options-wording'
  | 'geo.distance-same-lon-shortcut'
  | 'geo.distance-lon-epsilon'
  | 'geo.missing-member-wording'
  | 'rdb.version-10'
  | 'rdb.version-11'
  | 'rdb.version-12'
  | 'rdb.version-80'
  | 'list.large-element-plain-node'
  | 'command.incomplete-stream-geo-key-specs'
  | 'protocol.strict-crlf'
  | 'zset.numkeys-quoted-wording'
  | 'hello.noauth-proto-wording'
  | 'command.georadius-variable-flags'
  | 'client.info-multi-lazy-state'
  | 'protocol.inline-drops-0xff'
  | 'hello.setname-after-auth'
  | 'migrate.connect-timeout-error'

export interface CompatibilityProfile {
  readonly flavor: RedisFlavor
  readonly version: string
  readonly versionNum: number
  has(feature: FeatureId): boolean
}

export type CompatibilitySpec =
  | CompatibilityProfile
  | { flavor?: RedisFlavor; version?: string }
  | 'redis-6.2'
  | 'redis-7.0'
  | 'redis-7.2'
  | 'redis-7.4'
  | 'redis-8.0'
  | 'valkey-8.0'
  | 'valkey-9.0'

const PRESETS: Record<
  Exclude<
    CompatibilitySpec,
    CompatibilityProfile | { flavor?: RedisFlavor; version?: string }
  >,
  { flavor: RedisFlavor; version: string }
> = {
  'redis-6.2': { flavor: 'redis', version: '6.2.14' },
  'redis-7.0': { flavor: 'redis', version: '7.0.15' },
  'redis-7.2': { flavor: 'redis', version: '7.2.4' },
  'redis-7.4': { flavor: 'redis', version: '7.4.4' },
  'redis-8.0': { flavor: 'redis', version: '8.0.0' },
  'valkey-8.0': { flavor: 'valkey', version: '8.0.0' },
  'valkey-9.0': { flavor: 'valkey', version: '9.0.0' },
}

const DEFAULT_SPEC = 'redis-8.0'

/**
 * The Redis version every Valkey release reports for compatibility: INFO's
 * `redis_version` and a script's `redis.REDIS_VERSION` (Valkey forked Redis
 * 7.2.4). Its own version is `valkey_version` / `VALKEY_VERSION`.
 */
export const VALKEY_REDIS_COMPAT_VERSION = '7.2.4'

export function parseVersion(version: string): number {
  const [majorRaw, minorRaw = '0', patchRaw = '0'] = version.split('.')
  const major = parseVersionPart(majorRaw, version)
  const minor = parseVersionPart(minorRaw, version)
  const patch = parseVersionPart(patchRaw, version)
  return major * 10000 + minor * 100 + patch
}

export function resolveCompatibilityProfile(
  spec: CompatibilitySpec = DEFAULT_SPEC,
): CompatibilityProfile {
  if (typeof spec === 'object' && 'has' in spec) {
    return spec
  }

  if (typeof spec === 'string' && !(spec in PRESETS)) {
    throw new Error(`Unknown compatibility profile ${spec}`)
  }

  const resolved = typeof spec === 'string' ? PRESETS[spec] : spec
  const flavor = resolved.flavor ?? 'redis'
  const version = resolved.version ?? PRESETS[DEFAULT_SPEC].version
  const versionNum = parseVersion(version)

  return {
    flavor,
    version,
    versionNum,
    has: feature =>
      gateSatisfied(FEATURE_GATES[feature], { flavor, versionNum }),
  }
}

/**
 * Whether `profile` has the behavior `gate` describes. A single minimum is
 * met by that version and every later one. Per-branch minimums are met from
 * the listed patch within each listed `major.minor` branch and by every
 * version of a branch newer than the last listed one; a branch older than
 * that and not listed is not (a single minimum is a one-entry list).
 */
export function gateSatisfied(
  gate: VersionGate,
  profile: Pick<CompatibilityProfile, 'flavor' | 'versionNum'>,
): boolean {
  const minimums = gate[profile.flavor]
  if (minimums === undefined) {
    return false
  }

  const branch = Math.floor(profile.versionNum / 100)
  let latest = -1
  for (const minimum of typeof minimums === 'string' ? [minimums] : minimums) {
    const minimumNum = parseVersion(minimum)
    if (Math.floor(minimumNum / 100) === branch) {
      return profile.versionNum >= minimumNum
    }
    latest = Math.max(latest, minimumNum)
  }
  return latest !== -1 && profile.versionNum >= latest
}

function parseVersionPart(raw: string | undefined, version: string): number {
  if (raw === undefined || !/^\d+$/.test(raw)) {
    throw new Error(`Invalid compatibility version ${version}`)
  }
  return Number(raw)
}
