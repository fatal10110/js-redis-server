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
  // Redis 7.0 moved container commands into the command table, which replaced
  // `Unknown subcommand or wrong number of arguments for '%s'. Try %s HELP.`
  // with `unknown subcommand '%s'. Try %s HELP.` and added `%.128s` truncation
  // of the echoed name. Verified against redis-server 6.2.24, 7.0.15 and 8.0.6.
  // Valkey forked at 7.2, so every Valkey profile has the newer wording.
  'error.unknown-subcommand-wording': { redis: '7.0.0', valkey: '7.2.0' },
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
}
