import type { CompatibilityProfile } from './compatibility'

export class RedisCommandError extends Error {
  /**
   * The byte-exact reply body, set only when the error was built from a
   * Buffer. `Error.message` is a `string`, so an error that echoes raw client
   * bytes (an unknown subcommand, say) would lose them to U+FFFD on the way to
   * the wire; this keeps the original alongside the lossy `message` used for
   * logging and assertions. Read it through {@link errorReplyBody}.
   */
  readonly messageBytes?: Buffer

  constructor(
    message: string | Buffer,
    public readonly code = 'ERR',
  ) {
    super(typeof message === 'string' ? message : message.toString())
    this.name = code
    if (typeof message !== 'string') {
      this.messageBytes = message
    }
  }
}

/**
 * The reply body to put on the wire for a {@link RedisCommandError} — its raw
 * bytes when it carries any, otherwise its message. Every conversion from a
 * caught error to a reply goes through this (or {@link RedisResult.fromError},
 * which wraps it); reading `error.message` directly drops the bytes.
 */
export function errorReplyBody(error: RedisCommandError): string | Buffer {
  return error.messageBytes ?? error.message
}

/** {@link errorReplyBody} for the callers that need a Buffer either way. */
export function errorReplyBytes(error: RedisCommandError): Buffer {
  return error.messageBytes ?? Buffer.from(error.message)
}

// A subclass is kept only when (a) code tells it apart with `instanceof`
// (WrongNumberOfArguments, UnknownRedisCommand and UnknownSubcommand in the
// executor and Lua bridge; WrongType in tests), or (b) the pipeline raises it
// around a command
// rather than a command raising it: the state layer's WRONGTYPE, ClusterPolicy's
// MOVED / CROSSSLOT / CLUSTERDOWN, AuthPolicy's NOAUTH, and the executor's
// EXECABORT for a malformed EXEC. Every error a command raises itself is a
// plain RedisCommandError from `errors` below, whatever its code prefix.

export class WrongNumberOfArgumentsError extends RedisCommandError {
  constructor(commandName: string) {
    super(`wrong number of arguments for '${commandName}' command`)
  }
}

/** A command was issued before authenticating on a password-protected server. */
export class NoAuthError extends RedisCommandError {
  constructor(message = 'Authentication required.') {
    super(message, 'NOAUTH')
  }
}

export class WrongTypeRedisError extends RedisCommandError {
  constructor() {
    super(
      'Operation against a key holding the wrong kind of value',
      'WRONGTYPE',
    )
  }
}

export class RedisCrossSlotError extends RedisCommandError {
  constructor() {
    super(`Keys in request don't hash to the same slot`, 'CROSSSLOT')
  }
}

export class RedisMovedError extends RedisCommandError {
  constructor(slot: number, host: string, port: number) {
    super(`${slot} ${host}:${port}`, 'MOVED')
  }
}

export class RedisClusterDownError extends RedisCommandError {
  constructor() {
    super('Hash slot not served', 'CLUSTERDOWN')
  }
}

/** EXEC itself is malformed (e.g. wrong arity) — discards the transaction immediately. */
export class ExecCommandAbortError extends RedisCommandError {
  constructor(reason: string) {
    super(`Transaction discarded because of: ${reason}`, 'EXECABORT')
  }
}

/** The args part stops growing once it reaches this many bytes. */
const UNKNOWN_COMMAND_ARGS_BUDGET = 128

/**
 * The two shapes of the reply, per `error.unknown-command-wording`. Redis
 * 7.0+ (and every Valkey) prints
 *
 *     unknown command '%.128s', with args beginning with: %s
 *
 * growing the args part by `'%.*s' ` per argument; Redis 6.2 prints
 *
 *     unknown command `%s`, with args beginning with: %s
 *
 * growing it by `` `%.*s`, `` instead, and does not cap the name. Either way
 * the precision is `128 - <bytes so far>` and args are added while the part is
 * under 128 bytes, so the quoting and separator bytes count against the
 * budget: 6.2, spending one more byte per arg, echoes fewer of them.
 */
const UNKNOWN_COMMAND_FORMATS = {
  current: { quote: "'", separator: ' ', nameCap: 128 },
  legacy: { quote: '`', separator: ', ', nameCap: Infinity },
} as const

/**
 * The name and args are echoed as raw bytes, each cut at its first NUL (`%s`
 * reads a C string); the cut is by byte, so it can split a UTF-8 sequence, and
 * only the byte budget, not an argument count, ends the list. CR/LF are mapped
 * to spaces by the encoder, as for every error reply (#384).
 */
export class UnknownRedisCommandError extends RedisCommandError {
  constructor(
    commandName: string | Buffer,
    args: readonly Buffer[],
    profile?: CompatibilityProfile,
  ) {
    const { quote, separator, nameCap } =
      profile && !profile.has('error.unknown-command-wording')
        ? UNKNOWN_COMMAND_FORMATS.legacy
        : UNKNOWN_COMMAND_FORMATS.current
    const open = Buffer.from(quote)
    const close = Buffer.from(`${quote}${separator}`)
    const parts: Buffer[] = []
    let argsLength = 0
    for (
      let i = 0;
      i < args.length && argsLength < UNKNOWN_COMMAND_ARGS_BUDGET;
      i++
    ) {
      const arg = cString(args[i], UNKNOWN_COMMAND_ARGS_BUDGET - argsLength)
      parts.push(open, arg, close)
      argsLength += open.length + arg.length + close.length
    }

    const bytes = Buffer.concat([
      Buffer.from(`unknown command ${quote}`),
      cString(Buffer.from(commandName), nameCap),
      Buffer.from(`${quote}, with args beginning with: `),
      ...parts,
    ])
    // Pass a string when it round-trips: a Buffer message also sets
    // `messageBytes`, which rides into the RedisValue, and callers that compare
    // error values structurally would then see a different shape for the same
    // wire text.
    const text = bytes.toString()
    super(Buffer.from(text).equals(bytes) ? text : bytes)
  }
}

/**
 * A container was given a subcommand it does not have (`CONFIG BOGUS`). The
 * body is profile-specific and built only by `unknownSubcommandError` in
 * src/core/subcommand-errors.ts; the class exists so the Lua runtime can tell a
 * failed 7.0+ subcommand *lookup* in `CommandExecutor.plan()` apart from other
 * planning errors, and answer it like an unknown command (#439).
 */
export class UnknownSubcommandError extends RedisCommandError {}

/** `%.<precision>s` over raw bytes: stop at the first NUL or `precision`. */
function cString(value: Buffer, precision: number): Buffer {
  const nul = value.indexOf(0)
  const end = nul === -1 ? value.length : nul
  return value.subarray(0, Math.min(end, precision))
}

/**
 * Fixed-text client-visible errors. Each call returns a fresh
 * {@link RedisCommandError}; the strings are the exact wire text of real
 * Redis and are compat-tested byte for byte, so change them only against a
 * real server.
 */
export const errors = Object.freeze({
  syntax: () => new RedisCommandError('syntax error'),

  // Auth / handshake
  /** `AUTH <password>` (single-arg) when the server has no `requirepass` set. */
  noPasswordConfigured: () =>
    new RedisCommandError(
      'AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?',
    ),
  /** Wrong username/password pair on AUTH or HELLO AUTH. */
  wrongPass: () =>
    new RedisCommandError(
      'invalid username-password pair or user is disabled.',
      'WRONGPASS',
    ),
  /** `HELLO <version>` with a syntactically valid but unsupported protocol version. */
  noProto: () =>
    new RedisCommandError('unsupported protocol version', 'NOPROTO'),
  /** `HELLO <version>` where version is not a valid integer at all. */
  helloProtocolNotInteger: () =>
    new RedisCommandError('Protocol version is not an integer or out of range'),

  // Numbers
  expectedInteger: () =>
    new RedisCommandError('value is not an integer or out of range'),
  incrDecrOverflow: () =>
    new RedisCommandError('increment or decrement would overflow'),
  decrOverflow: () => new RedisCommandError('decrement would overflow'),
  expectedFloat: () => new RedisCommandError('value is not a valid float'),
  incrByFloatNanOrInfinity: () =>
    new RedisCommandError('increment would produce NaN or Infinity'),
  /** A non-ALPHA SORT met an element that does not parse as a double. */
  sortScoreNotDouble: () =>
    new RedisCommandError("One or more scores can't be converted into double"),
  resultingScoreNaN: () =>
    new RedisCommandError('resulting score is not a number (NaN)'),
  minMaxNotFloat: () => new RedisCommandError('min or max is not a float'),
  positiveCount: () =>
    new RedisCommandError('value is out of range, must be positive'),
  offsetOutOfRange: () => new RedisCommandError('offset is out of range'),
  hashValueNotInteger: () =>
    new RedisCommandError('hash value is not an integer'),
  hashValueNotFloat: () => new RedisCommandError('hash value is not a float'),

  // Lists / blocking
  lposRankZero: () =>
    new RedisCommandError(
      "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
    ),
  lposCountNegative: () => new RedisCommandError("COUNT can't be negative"),
  lposMaxlenNegative: () => new RedisCommandError("MAXLEN can't be negative"),
  timeoutNotFloat: () =>
    new RedisCommandError('timeout is not a float or out of range'),
  timeoutNegative: () => new RedisCommandError('timeout is negative'),
  indexOutOfRange: () => new RedisCommandError('index out of range'),
  noSuchKey: () => new RedisCommandError('no such key'),

  // Option conflicts
  zaddNxXxConflict: () =>
    new RedisCommandError(
      'XX and NX options at the same time are not compatible',
    ),
  zaddGtLtNxConflict: () =>
    new RedisCommandError(
      'GT, LT, and/or NX options at the same time are not compatible',
    ),
  expireNxXxGtLtConflict: () =>
    new RedisCommandError(
      'NX and XX, GT or LT options at the same time are not compatible',
    ),
  expireGtLtConflict: () =>
    new RedisCommandError(
      'GT and LT options at the same time are not compatible',
    ),
  unsupportedOption: (option: string) =>
    new RedisCommandError(`Unsupported option ${option}`),
  zaddIncrPair: () =>
    new RedisCommandError(
      'INCR option supports a single increment-element pair',
    ),
  invalidExpireTime: (commandName: string) =>
    new RedisCommandError(`invalid expire time in '${commandName}' command`),

  // Geo
  invalidLongitudeLatitude: (lon: number, lat: number) =>
    new RedisCommandError(
      `invalid longitude,latitude pair ${lon.toFixed(6)},${lat.toFixed(6)}`,
    ),
  geoUnsupportedUnit: () =>
    new RedisCommandError(
      'unsupported unit provided. please use M, KM, FT, MI',
    ),
  geoMissingMember: () =>
    new RedisCommandError('could not decode requested zset member'),
  geoCountNotPositive: () => new RedisCommandError('COUNT must be > 0'),
  geoAnyRequiresCount: () =>
    new RedisCommandError('the ANY argument requires COUNT argument'),
  geoRadiusNotNumeric: () => new RedisCommandError('need numeric radius'),
  geoRadiusNegative: () => new RedisCommandError('radius cannot be negative'),
  geoWidthNotNumeric: () => new RedisCommandError('need numeric width'),
  geoHeightNotNumeric: () => new RedisCommandError('need numeric height'),
  geoBoxNegative: () =>
    new RedisCommandError('height or width cannot be negative'),
  geoRadiusStoreWithOptions: () =>
    new RedisCommandError(
      'STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options',
    ),
  geoSearchStoreWithOptions: () =>
    new RedisCommandError(
      'GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options',
    ),

  // Strings / bits
  /**
   * A command that grows a string value (APPEND/SETRANGE) would push it past
   * `proto-max-bulk-len`. Redis refuses instead of allocating the value.
   */
  stringExceedsMaxSize: () =>
    new RedisCommandError(
      'string exceeds maximum allowed size (proto-max-bulk-len)',
    ),
  /** SETBIT/GETBIT/BITFIELD offset that is negative, non-integer, or >= 2^32. */
  bitOffset: () =>
    new RedisCommandError('bit offset is not an integer or out of range'),
  /** SETBIT value that is not exactly 0 or 1. */
  bitValue: () =>
    new RedisCommandError('bit is not an integer or out of range'),
  /** BITPOS bit argument that is not 0 or 1. */
  bitPosBit: () => new RedisCommandError('The bit argument must be 1 or 0.'),
  /** BITOP NOT invoked with more than one source key. */
  bitOpNotSingleKey: () =>
    new RedisCommandError('BITOP NOT must be called with a single source key.'),
  /** BITFIELD type token that is malformed or u64 (only i64 is allowed at 64 bits). */
  bitfieldType: () =>
    new RedisCommandError(
      'Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.',
    ),
  /** BITFIELD OVERFLOW with a mode other than WRAP/SAT/FAIL. */
  bitfieldOverflowType: () =>
    new RedisCommandError('Invalid OVERFLOW type specified'),
  /** A non-GET subcommand passed to BITFIELD_RO. */
  bitfieldRoGetOnly: () =>
    new RedisCommandError('BITFIELD_RO only supports the GET subcommand'),
  /** A string-type key whose contents are not a valid HyperLogLog encoding. */
  invalidHll: () =>
    new RedisCommandError(
      'Key is not a valid HyperLogLog string value.',
      'WRONGTYPE',
    ),

  // Scripting
  scriptFlushOption: () =>
    new RedisCommandError('SCRIPT FLUSH only support SYNC|ASYNC option'),
  functionFlushOption: () =>
    new RedisCommandError('FUNCTION FLUSH only support SYNC|ASYNC option'),
  scriptDebugMode: () => new RedisCommandError('Use SCRIPT DEBUG YES/SYNC/NO'),
  /**
   * A script's `redis.call` named a command (or, from 7.0, a container
   * subcommand) that command lookup cannot find. Valkey 8.0+ drops the product
   * name; without a profile the Redis wording is used.
   */
  scriptUnknownCommand: (profile?: CompatibilityProfile) =>
    new RedisCommandError(
      profile?.has('script.unknown-command-valkey-wording')
        ? 'Unknown command called from script'
        : 'Unknown Redis command called from script',
    ),
  scriptNotAllowedCommand: () =>
    new RedisCommandError('This Redis command is not allowed from script'),
  scriptCallNoCommand: () =>
    new RedisCommandError(
      'Please specify at least one argument for this redis lib call',
    ),
  noScript: () =>
    new RedisCommandError('No matching script. Please use EVAL.', 'NOSCRIPT'),

  // numkeys / multi-key
  wrongNumberOfKeys: () =>
    new RedisCommandError(
      `Number of keys can't be greater than number of args`,
    ),
  numKeysGreaterThanZero: () =>
    new RedisCommandError('numkeys should be greater than 0'),
  /** ZUNION/ZINTER/ZDIFF family when numkeys <= 0. */
  atLeastOneInputKey: (commandName: string) =>
    new RedisCommandError(
      `at least 1 input key is needed for '${commandName}' command`,
    ),
  weightNotFloat: () => new RedisCommandError('weight value is not a float'),
  countGreaterThanZero: () =>
    new RedisCommandError('count should be greater than 0'),
  limitCantBeNegative: () => new RedisCommandError(`LIMIT can't be negative`),

  // Sorted-set ranges
  invalidLexRange: () =>
    new RedisCommandError('min or max not valid string range item'),
  zrangeLimitWithoutBy: () =>
    new RedisCommandError(
      'syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX',
    ),
  zrangeWithScoresByLex: () =>
    new RedisCommandError(
      'syntax error, WITHSCORES not supported in combination with BYLEX',
    ),

  // Transactions
  execWithoutMulti: () => new RedisCommandError('EXEC without MULTI'),
  discardWithoutMulti: () => new RedisCommandError('DISCARD without MULTI'),
  watchInsideMulti: () =>
    new RedisCommandError('WATCH inside MULTI is not allowed'),
  transactionDiscarded: () =>
    new RedisCommandError(
      'Transaction discarded because of previous errors.',
      'EXECABORT',
    ),

  // Keyspace / databases
  /** `COPY src dst` (and `SELECT`) when source and destination resolve to the same object. */
  sameObject: () =>
    new RedisCommandError('source and destination objects are the same'),
  /** A database index outside `0 .. databaseCount - 1` (e.g. `COPY ... DB 99`, `SELECT 99`). */
  dbIndexOutOfRange: () => new RedisCommandError('DB index is out of range'),

  // Streams
  streamLimitRequiresApprox: () =>
    new RedisCommandError(
      'syntax error, LIMIT cannot be used without the special ~ option',
    ),
  streamLimitNegative: () =>
    new RedisCommandError('The LIMIT argument must be >= 0.'),
  invalidStreamId: () =>
    new RedisCommandError(
      'Invalid stream ID specified as stream command argument',
    ),
  streamIdEqualOrSmaller: () =>
    new RedisCommandError(
      'The ID specified in XADD is equal or smaller than the target stream top item',
    ),
  streamIdNotGreaterThanZero: () =>
    new RedisCommandError('The ID specified in XADD must be greater than 0-0'),
  xsetidSmallerThanTop: () =>
    new RedisCommandError(
      'The ID specified in XSETID is smaller than the target stream top item',
    ),
  xgroupCreateMissingKey: () =>
    new RedisCommandError(
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    ),
  busyStreamGroup: () =>
    new RedisCommandError('Consumer Group name already exists', 'BUSYGROUP'),
  noSuchStreamGroup: (key: Buffer, group: Buffer, commandName?: string) =>
    new RedisCommandError(
      `No such key '${key.toString()}' or consumer group '${group.toString()}'${
        commandName === 'XREADGROUP' ? ' in XREADGROUP with GROUP option' : ''
      }`,
      'NOGROUP',
    ),
  streamIdExhausted: () =>
    new RedisCommandError(
      'The stream has exhausted the last possible ID, unable to add more items',
    ),
})
