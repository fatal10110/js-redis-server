import type { FeatureId, VersionGate } from './profile'

export const FEATURE_GATES: Record<FeatureId, VersionGate> = {
  'expire.conditions': { redis: '7.0.0', valkey: '7.2.0' },
  'set.get': { redis: '6.2.0', valkey: '7.2.0' },
  'set.nx-get': { redis: '7.0.0', valkey: '7.2.0' },
  'set.exat-pxat': { redis: '6.2.0', valkey: '7.2.0' },
  'command.docs': { redis: '7.0.0', valkey: '7.2.0' },
  'command.getkeysandflags': { redis: '7.0.0', valkey: '7.2.0' },
  'client.no-evict': { redis: '7.0.0', valkey: '7.2.0' },
  'client.setinfo': { redis: '7.2.0', valkey: '7.2.0' },
  'client.setinfo.unknown-subcommand-error': {
    redis: '7.0.0',
    valkey: '7.2.0',
  },
  'info.multi-section': { redis: '7.0.0', valkey: '7.2.0' },
  'pubsub.sharded': { redis: '7.0.0', valkey: '7.2.0' },
  'stream.xautoclaim-deleted-ids': { redis: '7.0.0', valkey: '7.2.0' },
  'cluster.multi-db': { valkey: '9.0.0' },
  // Redis 7.0 reimplemented script globals protection as a readonly table
  // ("Attempt to modify a readonly table"); earlier versions reported
  // "Script attempted to create global variable '<name>'". Valkey (>= 7.2)
  // inherits the readonly-table behavior.
  'script.globals-readonly-table': { redis: '7.0.0', valkey: '7.2.0' },
}
