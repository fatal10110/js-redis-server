import type { FeatureId, VersionGate } from './profile'

export const FEATURE_GATES: Record<FeatureId, VersionGate> = {
  'expire.conditions': { redis: '7.0.0', valkey: '7.2.0' },
  'set.get': { redis: '6.2.0', valkey: '7.2.0' },
  'set.nx-get': { redis: '7.0.0', valkey: '7.2.0' },
  'set.exat-pxat': { redis: '6.2.0', valkey: '7.2.0' },
  'command.docs': { redis: '7.0.0', valkey: '7.2.0' },
  'command.getkeysandflags': { redis: '7.0.0', valkey: '7.2.0' },
  'acl.dryrun': { redis: '7.0.0', valkey: '7.2.0' },
  'client.no-evict': { redis: '7.0.0', valkey: '7.2.0' },
  'client.kill.maxage': { redis: '7.4.0', valkey: '9.0.0' },
  'client.setinfo': { redis: '7.2.0', valkey: '7.2.0' },
  'client.setinfo.unknown-subcommand-error': {
    redis: '7.0.0',
    valkey: '7.2.0',
  },
  'info.multi-section': { redis: '7.0.0', valkey: '7.2.0' },
  'shutdown.now-force-abort': { redis: '7.0.0', valkey: '7.2.0' },
  'pubsub.sharded': { redis: '7.0.0', valkey: '7.2.0' },
  'pubsub.resp3-publish-reply-first': { redis: '7.2.0', valkey: '8.0.0' },
  'stream.xautoclaim-deleted-ids': { redis: '7.0.0', valkey: '7.2.0' },
  // BITCOUNT/BITPOS BYTE|BIT range modifier — Redis 7.0 / Valkey 7.2.
  'bit.byte-bit-range': { redis: '7.0.0', valkey: '7.2.0' },
  'hscan.novalues': { redis: '7.4.0', valkey: '9.0.0' },
  'xread.plus-id': { redis: '7.4.0' },
  'cluster.multi-db': { valkey: '9.0.0' },
  // SORT BY/GET in cluster mode: compare each pattern's slot against the sort
  // key's slot (`patternHashSlot()`) instead of refusing every pattern, and use
  // the longer "...may be in different slots." wording — Redis 7.4 / Valkey 8.0.
  'sort.cluster-pattern-slot': { redis: '7.4.0', valkey: '8.0.0' },
  // SORT GET '#' (the element itself) is exempt from that slot comparison.
  // Bisected on single-node clusters: redis 7.4.0/7.4.1 refuse and 7.4.2+
  // accept; valkey 8.0.0/8.0.1 refuse and 8.0.2+ accept. Before that it is
  // hashed like any other pattern and therefore denied.
  'sort.cluster-get-hash': { redis: '7.4.2', valkey: '8.0.2' },
}
