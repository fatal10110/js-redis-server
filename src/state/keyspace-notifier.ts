import type { RedisDataValue } from './data-types'
import type { RedisMutationEvent } from './mutation-events'
import type { RedisPubSubBroker } from './pubsub-broker'

/**
 * Parsed `notify-keyspace-events`: the set of enabled flag characters, with
 * 'A' already expanded. `K`/`E` select the delivery channels
 * (`__keyspace@<db>__:<key>` / `__keyevent@<db>__:<event>`); the rest gate
 * which event classes are published. Empty disables notifications.
 */
export type KeyspaceNotifyFlags = ReadonlySet<string>

// The classes 'A' (NOTIFY_ALL) expands to, in Redis' render order: everything
// except key-miss (m) and new-key (n). Module (d) IS included, so `Ad`
// collapses to `A` while `g$lshzxet` (no d) stays expanded.
const ALL_CLASSES = 'g$lshzxetd'
const VALID_FLAGS = new Set(`${ALL_CLASSES}KEmn`)

/** CONFIG SET's failure detail for an unrecognized flag character. */
export const INVALID_NOTIFY_FLAG_DETAIL =
  "Invalid event class character. Use 'Ag$lshzxeKEtmdn'."

/**
 * Parse a raw `notify-keyspace-events` value (e.g. `"KEA"`, `"Ex"`). Returns
 * `undefined` on an unrecognized character — the caller owns the error, whose
 * wording is profile-specific.
 */
export function parseKeyspaceNotifyFlags(
  value: string,
): KeyspaceNotifyFlags | undefined {
  const flags = new Set<string>()
  for (const char of value) {
    if (char === 'A') {
      for (const cls of ALL_CLASSES) flags.add(cls)
    } else if (VALID_FLAGS.has(char)) {
      flags.add(char)
    } else {
      return undefined
    }
  }
  return flags
}

/**
 * Render flags in Redis' canonical form (7.2 `keyspaceEventsFlagsToString`).
 * When every class 'A' covers is set they collapse to 'A' and `n` is dropped;
 * otherwise the classes plus `n` are emitted in order. `K`, `E`, then `m`
 * always follow. Examples: `KEA` → `AKE`, `KEgnd$` → `g$dnKE`, `Adm` → `Am`.
 */
export function keyspaceNotifyFlagsToString(
  flags: KeyspaceNotifyFlags,
): string {
  const all = [...ALL_CLASSES].every(cls => flags.has(cls))
  const order = all ? 'AKEm' : `${ALL_CLASSES}nKEm`
  return [...order].filter(flag => flag === 'A' || flags.has(flag)).join('')
}

type NotifyClass = 'g' | '$' | 'l' | 's' | 'h' | 'z' | 'x' | 'e' | 't'

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
 * (`set`, `lpush`, `hset`, ...) depend on the originating command, which the
 * mutation bus does not carry — so the executor records the active command name
 * on the database and it is passed in here. Commands that map one logical
 * operation onto several mutations with special names (RENAME → rename_from /
 * rename_to) are handled via the override tables above.
 */
export class KeyspaceNotifier {
  constructor(private readonly broker: RedisPubSubBroker) {}

  handle(
    event: RedisMutationEvent,
    activeCommand: string | null,
    flags: KeyspaceNotifyFlags,
  ): void {
    const keyspace = flags.has('K')
    const keyevent = flags.has('E')
    if (!keyspace && !keyevent) {
      return
    }

    const notification = this.resolve(event, activeCommand)
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

  private resolve(
    event: RedisMutationEvent,
    activeCommand: string | null,
  ): ResolvedNotification | null {
    switch (event.type) {
      case 'write': {
        if (!activeCommand) {
          return null
        }
        const name = WRITE_EVENT_OVERRIDES[activeCommand] ?? activeCommand
        const eventClass = GENERIC_WRITE_COMMANDS.has(activeCommand)
          ? 'g'
          : CLASS_FOR_TYPE[event.value.type]
        return {
          database: event.database,
          key: event.key,
          event: name,
          eventClass,
        }
      }
      case 'delete': {
        const name =
          (activeCommand && DELETE_EVENT_OVERRIDES[activeCommand]) ?? 'del'
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
