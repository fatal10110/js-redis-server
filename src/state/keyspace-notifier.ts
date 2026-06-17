import type { RedisDataValue } from './data-types'
import type { RedisMutationEvent } from './mutation-events'
import type { RedisPubSubBroker } from './pubsub-broker'
import { RedisCommandError } from '../core/redis-error'

/**
 * Parsed `notify-keyspace-events` configuration, mirroring Redis' per-class
 * notification flags. `keyspace`/`keyevent` select the delivery channels
 * (`__keyspace@<db>__:<key>` / `__keyevent@<db>__:<event>`); the remaining
 * booleans gate which event classes are published.
 */
export type KeyspaceNotifyFlags = {
  keyspace: boolean // K
  keyevent: boolean // E
  generic: boolean // g
  string: boolean // $
  list: boolean // l
  set: boolean // s
  hash: boolean // h
  zset: boolean // z
  expired: boolean // x
  evicted: boolean // e
  stream: boolean // t
  keyMiss: boolean // m
  newKey: boolean // n
  module: boolean // d
}

// The classes that 'A' (NOTIFY_ALL) expands to — everything except the
// key-miss (m), new-key (n) and module (d) classes, matching Redis.
const ALL_CLASS_KEYS = [
  'generic',
  'string',
  'list',
  'set',
  'hash',
  'zset',
  'expired',
  'evicted',
  'stream',
] as const satisfies readonly (keyof KeyspaceNotifyFlags)[]

function emptyFlags(): KeyspaceNotifyFlags {
  return {
    keyspace: false,
    keyevent: false,
    generic: false,
    string: false,
    list: false,
    set: false,
    hash: false,
    zset: false,
    expired: false,
    evicted: false,
    stream: false,
    keyMiss: false,
    newKey: false,
    module: false,
  }
}

/**
 * Parse a raw `notify-keyspace-events` flag string (e.g. `"KEA"`, `"Ex"`) into
 * structured flags. Throws the exact Redis error on an unrecognized character.
 */
export function parseKeyspaceNotifyFlags(value: string): KeyspaceNotifyFlags {
  const flags = emptyFlags()
  for (const char of value) {
    switch (char) {
      case 'A':
        for (const key of ALL_CLASS_KEYS) flags[key] = true
        break
      case 'K':
        flags.keyspace = true
        break
      case 'E':
        flags.keyevent = true
        break
      case 'g':
        flags.generic = true
        break
      case '$':
        flags.string = true
        break
      case 'l':
        flags.list = true
        break
      case 's':
        flags.set = true
        break
      case 'h':
        flags.hash = true
        break
      case 'z':
        flags.zset = true
        break
      case 'x':
        flags.expired = true
        break
      case 'e':
        flags.evicted = true
        break
      case 't':
        flags.stream = true
        break
      case 'm':
        flags.keyMiss = true
        break
      case 'n':
        flags.newKey = true
        break
      case 'd':
        flags.module = true
        break
      default:
        throw new RedisCommandError(
          'CONFIG SET failed (possibly related to argument ' +
            "'notify-keyspace-events') - Invalid event class character. " +
            "Use 'Ag$lshzxeKEtmdn'.",
        )
    }
  }
  return flags
}

/**
 * Render flags back to Redis' canonical string form. When every class that 'A'
 * covers is set, those classes collapse to a single 'A' (matching Redis'
 * `keyspaceEventsFlagsToString`), e.g. parsing `"KEA"` normalizes to `"AKE"`.
 */
export function keyspaceNotifyFlagsToString(
  flags: KeyspaceNotifyFlags,
): string {
  let result = ''
  if (ALL_CLASS_KEYS.every(key => flags[key])) {
    result += 'A'
  } else {
    if (flags.generic) result += 'g'
    if (flags.string) result += '$'
    if (flags.list) result += 'l'
    if (flags.set) result += 's'
    if (flags.hash) result += 'h'
    if (flags.zset) result += 'z'
    if (flags.expired) result += 'x'
    if (flags.evicted) result += 'e'
    if (flags.stream) result += 't'
  }
  if (flags.keyMiss) result += 'm'
  if (flags.newKey) result += 'n'
  if (flags.module) result += 'd'
  if (flags.keyspace) result += 'K'
  if (flags.keyevent) result += 'E'
  return result
}

/**
 * Validate and normalize a `notify-keyspace-events` value for CONFIG SET.
 * Returns the canonical string; throws {@link RedisCommandError} on bad input.
 */
export function normalizeKeyspaceNotifyConfig(value: string): string {
  return keyspaceNotifyFlagsToString(parseKeyspaceNotifyFlags(value))
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
  move: 'move_to',
}

// Commands that delete a key as part of a rename/move emit a dedicated event
// instead of the default `del`.
const DELETE_EVENT_OVERRIDES: Readonly<Record<string, string>> = {
  rename: 'rename_from',
  renamenx: 'rename_from',
  move: 'move_from',
}

// Write events whose class is generic (g) rather than the value's data type.
const GENERIC_WRITE_COMMANDS = new Set(['rename', 'renamenx', 'copy', 'move'])

function classForType(type: RedisDataValue['type']): NotifyClass {
  switch (type) {
    case 'string':
      return '$'
    case 'list':
      return 'l'
    case 'set':
      return 's'
    case 'hash':
      return 'h'
    case 'zset':
      return 'z'
    case 'stream':
      return 't'
  }
}

function classEnabled(
  flags: KeyspaceNotifyFlags,
  eventClass: NotifyClass,
): boolean {
  switch (eventClass) {
    case 'g':
      return flags.generic
    case '$':
      return flags.string
    case 'l':
      return flags.list
    case 's':
      return flags.set
    case 'h':
      return flags.hash
    case 'z':
      return flags.zset
    case 'x':
      return flags.expired
    case 'e':
      return flags.evicted
    case 't':
      return flags.stream
  }
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
    rawFlags: string,
  ): void {
    if (rawFlags.length === 0) {
      return
    }

    const flags = parseKeyspaceNotifyFlags(rawFlags)
    if (!flags.keyspace && !flags.keyevent) {
      return
    }

    const notification = this.resolve(event, activeCommand)
    if (!notification) {
      return
    }

    if (!classEnabled(flags, notification.eventClass)) {
      return
    }

    const { database, key, event: name } = notification
    if (flags.keyspace) {
      this.broker.publish(
        Buffer.concat([Buffer.from(`__keyspace@${database}__:`), key]),
        Buffer.from(name),
      )
    }
    if (flags.keyevent) {
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
          : classForType(event.value.type)
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
