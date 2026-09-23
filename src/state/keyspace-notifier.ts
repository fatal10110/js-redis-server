import type { RedisDataValue } from './data-types'
import type { RedisMutationEvent } from './mutation-events'
import type { RedisPubSubBroker } from './pubsub-broker'

/** An event class a mutation is published under. */
type NotifyClass = 'g' | '$' | 'l' | 's' | 'h' | 'z' | 'x' | 'e' | 't'

/**
 * One `notify-keyspace-events` flag character: an event class, module (`d`),
 * key-miss (`m`), new-key (`n`), or a delivery channel (`K`/`E`). `A` is not a
 * flag — it is shorthand, expanded at parse time.
 */
export type KeyspaceNotifyFlag = NotifyClass | 'd' | 'm' | 'n' | 'K' | 'E'

/**
 * Parsed `notify-keyspace-events`: the set of enabled flags, with 'A' already
 * expanded. `K`/`E` select the delivery channels (`__keyspace@<db>__:<key>` /
 * `__keyevent@<db>__:<event>`); the rest gate which event classes are
 * published. Empty disables notifications.
 */
export type KeyspaceNotifyFlags = ReadonlySet<KeyspaceNotifyFlag>

// The classes 'A' (NOTIFY_ALL) expands to, in Redis' render order: everything
// except key-miss (m) and new-key (n). Module (d) IS included, so `Ad`
// collapses to `A` while `g$lshzxet` (no d) stays expanded.
const ALL_CLASSES: readonly KeyspaceNotifyFlag[] = [
  'g',
  '$',
  'l',
  's',
  'h',
  'z',
  'x',
  'e',
  't',
  'd',
]
const VALID_FLAGS: ReadonlySet<string> = new Set<KeyspaceNotifyFlag>([
  ...ALL_CLASSES,
  'm',
  'n',
  'K',
  'E',
])

function isNotifyFlag(char: string): char is KeyspaceNotifyFlag {
  return VALID_FLAGS.has(char)
}

/** CONFIG SET's failure detail for an unrecognized flag character. */
export const INVALID_NOTIFY_FLAG_DETAIL =
  "Invalid event class character. Use 'Ag$lshzxeKEtmdn'."

/**
 * Parse a raw `notify-keyspace-events` value (e.g. `"KEA"`, `"Ex"`). Returns
 * `undefined` on an unrecognized character — the caller owns the error, whose
 * wording is profile-specific.
 *
 * `newKeyClass: false` rejects `n`, which Redis only added in 7.0 (6.2 treats
 * it as an unknown character).
 */
export function parseKeyspaceNotifyFlags(
  value: string,
  { newKeyClass = true }: { newKeyClass?: boolean } = {},
): KeyspaceNotifyFlags | undefined {
  const flags = new Set<KeyspaceNotifyFlag>()
  for (const char of value) {
    if (char === 'A') {
      for (const cls of ALL_CLASSES) flags.add(cls)
    } else if (isNotifyFlag(char) && (newKeyClass || char !== 'n')) {
      flags.add(char)
    } else {
      return undefined
    }
  }
  return flags
}

/**
 * Render flags in Redis' canonical form, mirroring 7.2's
 * `keyspaceEventsFlagsToString` rule by rule:
 *
 * 1. Classes. If every class 'A' covers (`g$lshzxetd`) is set, emit a single
 *    `A` — and nothing else from this group, so `n` is dropped (`And` → `A`).
 *    Otherwise emit each set class in `g$lshzxetd` order, then `n`.
 * 2. Delivery channels: `K`, then `E`.
 * 3. Key-miss: `m`, always last, whether or not the classes collapsed.
 *
 * Examples: `KEA` → `AKE`, `KEgnd$` → `g$dnKE`, `KEn` → `nKE`, `Adm` → `Am`.
 */
export function keyspaceNotifyFlagsToString(
  flags: KeyspaceNotifyFlags,
): string {
  const has = (flag: KeyspaceNotifyFlag): boolean => flags.has(flag)
  const classes = ALL_CLASSES.every(has)
    ? 'A'
    : [...ALL_CLASSES, 'n' as const].filter(has).join('')
  const channels = (['K', 'E'] as const).filter(has).join('')
  const keyMiss = has('m') ? 'm' : ''
  return classes + channels + keyMiss
}

type ResolvedNotification = {
  database: number
  key: Buffer
  event: string
  eventClass: NotifyClass
}

// Commands whose write event name differs from the command name (Redis names
// notifications after a canonical operation, not the literal command).
const WRITE_EVENT_OVERRIDES: Readonly<Record<string, string>> = {
  setnx: 'set',
  setex: 'set',
  psetex: 'set',
  getset: 'set',
  mset: 'set',
  msetnx: 'set',
  incr: 'incrby',
  incrby: 'incrby',
  decr: 'incrby',
  decrby: 'incrby',
  lpushx: 'lpush',
  rpushx: 'rpush',
  hmset: 'hset',
  hsetnx: 'hset',
  zincrby: 'zincr',
  rename: 'rename_to',
  renamenx: 'rename_to',
  copy: 'copy_to',
}

// Commands that delete a key as part of a rename emit a dedicated event
// instead of the default `del`.
const DELETE_EVENT_OVERRIDES: Readonly<Record<string, string>> = {
  rename: 'rename_from',
  renamenx: 'rename_from',
}

// Write events whose class is generic (g) rather than the value's data type.
const GENERIC_WRITE_COMMANDS = new Set(['rename', 'renamenx', 'copy'])

const CLASS_FOR_TYPE: Readonly<Record<RedisDataValue['type'], NotifyClass>> = {
  string: '$',
  list: 'l',
  set: 's',
  hash: 'h',
  zset: 'z',
  stream: 't',
}

/**
 * Bridges keyspace mutation events to the Pub/Sub broker as Redis keyspace and
 * keyevent notifications.
 *
 * Lifecycle events (`del`, `expire`, `persist`, `expired`) are derived purely
 * from the mutation type, so they are always correct. Write event names
 * (`set`, `lpush`, `hset`, ...) — for both `write` and notification-only
 * `notify` mutations — come from the originating command the mutation carries
 * (`event.command`). Commands that map one logical operation onto several
 * mutations with special names (RENAME → rename_from / rename_to) are handled
 * via the override tables above.
 */
export class KeyspaceNotifier {
  constructor(private readonly broker: RedisPubSubBroker) {}

  handle(event: RedisMutationEvent, flags: KeyspaceNotifyFlags): void {
    const keyspace = flags.has('K')
    const keyevent = flags.has('E')
    if (!keyspace && !keyevent) {
      return
    }

    const notification = this.resolve(event)
    if (!notification || !flags.has(notification.eventClass)) {
      return
    }

    const { database, key, event: name } = notification
    if (keyspace) {
      this.broker.publish(
        Buffer.concat([Buffer.from(`__keyspace@${database}__:`), key]),
        Buffer.from(name),
      )
    }
    if (keyevent) {
      this.broker.publish(
        Buffer.from(`__keyevent@${database}__:${name}`),
        Buffer.from(key),
      )
    }
  }

  private resolve(event: RedisMutationEvent): ResolvedNotification | null {
    const command = event.command
    switch (event.type) {
      case 'write':
      case 'notify': {
        if (!command) {
          return null
        }
        const name = WRITE_EVENT_OVERRIDES[command] ?? command
        const eventClass = GENERIC_WRITE_COMMANDS.has(command)
          ? 'g'
          : CLASS_FOR_TYPE[event.valueType]
        return {
          database: event.database,
          key: event.key,
          event: name,
          eventClass,
        }
      }
      case 'delete': {
        const name = (command && DELETE_EVENT_OVERRIDES[command]) ?? 'del'
        return {
          database: event.database,
          key: event.key,
          event: name,
          eventClass: 'g',
        }
      }
      case 'expire':
        return {
          database: event.database,
          key: event.key,
          event: 'expire',
          eventClass: 'g',
        }
      case 'persist':
        return {
          database: event.database,
          key: event.key,
          event: 'persist',
          eventClass: 'g',
        }
      case 'evict':
        return {
          database: event.database,
          key: event.key,
          event: 'expired',
          eventClass: 'x',
        }
      case 'flush':
        return null
    }
  }
}
