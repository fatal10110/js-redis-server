/**
 * Redis command metadata following Redis COMMAND specification
 * @see https://redis.io/commands/command
 */
export interface CommandMetadata {
  /** Command name (lowercase) */
  name: string

  /**
   * Number of arguments
   * Positive: exact count (including command name)
   * Negative: minimum count (abs value), variable args
   * Example: GET = 2 (GET key), MGET = -2 (MGET key [key ...])
   */
  arity: number

  /** Command flags */
  flags: CommandFlags

  /**
   * First key position (0-indexed in args, not including command name)
   * -1 means no keys
   */
  firstKey: number

  /**
   * Last key position
   * -1 means last argument is key
   * -2 means second-to-last, etc.
   */
  lastKey: number

  /**
   * Step between keys (usually 1)
   * Example: MGET has step 1, MSET has step 2 (key val key val)
   */
  keyStep: number

  /**
   * Divisor for remaining args when lastKey=-1
   * @see https://redis.io/docs/latest/develop/reference/key-specs/
   * 0 or 1: no limit (all remaining args are keys)
   * 2: half the remaining args are keys (e.g., MSET key val key val)
   * 3: one-third are keys, etc.
   */
  limit: number

  /** Redis command categories */
  categories: CommandCategory[]
}

/**
 * Command flags following Redis specification
 */
export interface CommandFlags {
  /** Command doesn't modify data (safe for replicas) */
  readonly?: boolean

  /** Command modifies data */
  write?: boolean

  /** Deny command when used memory > maxmemory */
  denyoom?: boolean

  /** Administrative command */
  admin?: boolean

  /** Not allowed in Lua scripts */
  noscript?: boolean

  /** Returns random/non-deterministic results */
  random?: boolean

  /** Blocking operation (BLPOP, BRPOP, etc.) */
  blocking?: boolean

  /** O(1) time complexity */
  fast?: boolean

  /** Keys are not in fixed positions (requires key extraction) */
  movablekeys?: boolean

  /** Transaction-related command */
  transaction?: boolean
}

/**
 * Redis command categories
 * @see https://redis.io/commands#command-categories
 */
export enum CommandCategory {
  STRING = '@string',
  HASH = '@hash',
  LIST = '@list',
  SET = '@set',
  ZSET = '@zset',
  KEYS = '@keys',
  GENERIC = '@generic',
  SCRIPT = '@scripting',
  SERVER = '@server',
  CONNECTION = '@connection',
  CLUSTER = '@cluster',
  TRANSACTION = '@transactions',
  PUBSUB = '@pubsub',
  STREAM = '@stream',
}

/**
 * Helper to create metadata with defaults
 */
export function defineCommand(
  name: string,
  options: {
    arity: number
    flags: CommandFlags
    firstKey?: number
    lastKey?: number
    keyStep?: number
    limit?: number
    categories: CommandCategory[]
  },
): CommandMetadata {
  return {
    name: name.toLowerCase(),
    arity: options.arity,
    flags: options.flags,
    firstKey: options.firstKey ?? -1,
    lastKey: options.lastKey ?? -1,
    keyStep: options.keyStep ?? 1,
    limit: options.limit ?? 0,
    categories: options.categories,
  }
}
