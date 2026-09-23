import type { CompatibilityProfile, FeatureId } from './profile'

/**
 * Container subcommands that exist only from some profile on, keyed by their
 * `container|subcommand` command-table name. Anything not listed exists on
 * every profile that has the container itself.
 */
const SUBCOMMAND_FEATURES: Record<string, FeatureId> = {
  'acl|dryrun': 'acl.dryrun',
  'command|docs': 'command.docs',
  'command|getkeysandflags': 'command.getkeysandflags',
  'client|no-evict': 'client.no-evict',
  'client|setinfo': 'client.setinfo',
  'pubsub|shardchannels': 'pubsub.sharded',
  'pubsub|shardnumsub': 'pubsub.sharded',
}

/**
 * Whether the `container|subcommand` table entry exists on `profile`. Shared
 * by `COMMAND` introspection and the Lua script command lookup.
 */
export function subcommandSupported(
  name: string,
  profile: CompatibilityProfile,
): boolean {
  const feature = SUBCOMMAND_FEATURES[name.toLowerCase()]
  return feature === undefined || profile.has(feature)
}
