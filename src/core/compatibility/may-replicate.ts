import type { CompatibilityProfile, FeatureId } from './profile'

/**
 * The commands real Redis / Valkey flag `CMD_MAY_REPLICATE`, by the name
 * command lookup resolves them to (`container|subcommand` from 7.0), each
 * with the gate it needs on the profile (`from`) or must not have (`until`).
 *
 * From 7.0 `COMMAND INFO` no longer reports the flag, so the captured command
 * table cannot tell; the set is read from the servers' own tables:
 *  - redis 6.2.14 server.c: PUBLISH, EVAL, EVALSHA, SCRIPT (the whole
 *    command) and PFCOUNT;
 *  - redis 7.0.15 commands.c, 7.2.4 / 7.4.4 / 8.0.0 and valkey 7.2.5
 *    commands.def: PFCOUNT, PUBLISH, SPUBLISH, EVAL, EVALSHA and FCALL
 *    (SCRIPT became a container whose subcommands lack the flag);
 *  - valkey 8.0.0 commands.def adds CLUSTER SETSLOT, and 9.0.0 CLUSTER
 *    SYNCSLOTS.
 */
const MAY_REPLICATE: readonly {
  name: string
  from?: FeatureId
  until?: FeatureId
}[] = [
  { name: 'publish' },
  { name: 'eval' },
  { name: 'evalsha' },
  { name: 'pfcount' },
  { name: 'script', until: 'may-replicate.redis7-table' },
  { name: 'spublish', from: 'may-replicate.redis7-table' },
  { name: 'fcall', from: 'may-replicate.redis7-table' },
  { name: 'cluster|setslot', from: 'may-replicate.cluster-setslot' },
  { name: 'cluster|syncslots', from: 'may-replicate.cluster-syncslots' },
]

/**
 * The commands the profile's server flags may-replicate (#568), the single
 * source for every caller that needs the flag: a MONITOR connection is
 * refused them like readonly and write commands, and a read-only script
 * refuses them like writes. Each caller filters what it cannot reach.
 */
export function mayReplicateCommands(
  profile: CompatibilityProfile,
): ReadonlySet<string> {
  return new Set(
    MAY_REPLICATE.filter(
      ({ from, until }) =>
        (from === undefined || profile.has(from)) &&
        (until === undefined || !profile.has(until)),
    ).map(({ name }) => name),
  )
}

/**
 * Whether the command `name` (lowercase, `container|subcommand` for a
 * subcommand) is flagged may-replicate on the profile's server.
 */
export function mayReplicate(
  name: string,
  profile: CompatibilityProfile,
): boolean {
  return mayReplicateCommands(profile).has(name)
}
