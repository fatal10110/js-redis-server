import { equalsAscii } from './ascii-case'
import type { CommandPlan } from './command-definition'
import type { CompatibilityProfile } from './compatibility'
import type { RedisExecutionContext } from './redis-context'

/**
 * Publish `keymiss` for each key {@link readLookupKeys} names that does not
 * exist, when `notify-keyspace-events` enables the class. Runs as the
 * command is called, after the policies: a command queued by MULTI
 * announces its misses when EXEC runs it.
 */
export function announceKeyMisses(
  plan: CommandPlan,
  ctx: RedisExecutionContext,
): void {
  const flags = ctx.server.notifyKeyspaceEvents
  if (!flags.has('m') || (!flags.has('K') && !flags.has('E'))) {
    return
  }
  const db = ctx.db
  for (const key of readLookupKeys(plan, ctx.server.profile)) {
    if (db.getType(key) !== null) {
      continue
    }
    db.announceKeyMiss(key)
    // XREADGROUP answers NOGROUP at the first stream that is missing.
    if (plan.definition.name === 'xreadgroup') {
      return
    }
  }
}

/**
 * The keys a command looks up for reading, in lookup order: a missing one is
 * a `keymiss` keyspace notification (real Redis publishes it from
 * `lookupKeyRead`, never from `lookupKeyWrite`). The executor announces the
 * misses just before the command runs, so they precede the command's own
 * events, as in real Redis (`SET k v GET` on a missing key: `keymiss`,
 * `new`, `set`).
 *
 * A `readonly` command reads every key it names, with the exceptions in
 * {@link READONLY_OVERRIDES}; a write command reads none, except the ones in
 * {@link WRITE_READ_KEYS}. Verified against redis-server 6.2.14, 7.0.15,
 * 7.2.4, 8.0.0 and Valkey 8.0 / 9.0 (#514).
 */
export function readLookupKeys(
  plan: CommandPlan,
  profile: CompatibilityProfile,
): readonly Buffer[] {
  const name = plan.definition.name
  const keys = plan.keys
  if (plan.definition.flags.includes('readonly')) {
    const override = READONLY_OVERRIDES[name]
    return override ? override(keys, profile, plan.rawArgs) : keys
  }
  return WRITE_READ_KEYS[name]?.(keys, profile, plan.rawArgs) ?? []
}

type ReadKeys = (
  keys: readonly Buffer[],
  profile: CompatibilityProfile,
  rawArgs: readonly Buffer[],
) => readonly Buffer[]

const none: ReadKeys = () => []
const first: ReadKeys = keys => keys.slice(0, 1)
const sources: ReadKeys = keys => keys.slice(1)
// 6.2 looks these sources up with `lookupKeyWrite`.
const storeSources: ReadKeys = (keys, profile) =>
  profile.has('notify.keyspace.keymiss-store-sources') ? keys.slice(1) : []

const READONLY_OVERRIDES: Readonly<Record<string, ReadKeys>> = {
  // WATCH checks a key without looking it up; OBJECT and MEMORY USAGE look
  // up with LOOKUP_NONOTIFY or not at all; a script's keys are only names.
  watch: none,
  object: none,
  memory: none,
  eval_ro: none,
  evalsha_ro: none,
  fcall_ro: none,
  // A single-key PFCOUNT is a write lookup on 6.2 (it may update the cached
  // cardinality); several keys are read on every version.
  pfcount: (keys, profile) =>
    keys.length === 1 && !profile.has('notify.keyspace.keymiss-store-sources')
      ? []
      : keys,
  // XREAD looks every stream up while parsing the IDs and again while
  // serving them.
  xread: keys => [...keys, ...keys],
}

const WRITE_READ_KEYS: Readonly<Record<string, ReadKeys>> = {
  // The GET family reads the old value with `lookupKeyRead`.
  getset: first,
  getdel: first,
  getex: first,
  set: (keys, _profile, rawArgs) =>
    rawArgs.slice(2).some(arg => equalsAscii(arg, 'get')) ? keys : [],
  // Every source; PFMERGE's loop starts at the destination.
  bitop: sources,
  pfmerge: keys => keys,
  georadius: first,
  georadiusbymember: first,
  geosearchstore: sources,
  sinterstore: storeSources,
  sunionstore: storeSources,
  sdiffstore: storeSources,
  zunionstore: storeSources,
  zinterstore: storeSources,
  zdiffstore: storeSources,
  zrangestore: storeSources,
  copy: (keys, profile) =>
    profile.has('notify.keyspace.keymiss-store-sources')
      ? keys.slice(0, 1)
      : [],
  // SORT reads its key; with STORE, 6.2 looks it up for writing.
  sort: (keys, profile) =>
    keys.length > 1 && !profile.has('notify.keyspace.keymiss-store-sources')
      ? []
      : keys.slice(0, 1),
  // Up to the first missing stream (see announceKeyMisses).
  xreadgroup: keys => keys,
}
