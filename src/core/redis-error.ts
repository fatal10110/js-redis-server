export class RedisCommandError extends Error {
  constructor(
    message: string,
    public readonly code = 'ERR',
  ) {
    super(message)
    this.name = code
  }
}

export class WrongNumberOfArgumentsError extends RedisCommandError {
  constructor(commandName: string) {
    super(`wrong number of arguments for '${commandName}' command`)
  }
}

export class RedisSyntaxError extends RedisCommandError {
  constructor() {
    super('syntax error')
  }
}

/** `AUTH <password>` (single-arg) when the server has no `requirepass` set. */
export class NoPasswordConfiguredError extends RedisCommandError {
  constructor() {
    super(
      'AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?',
    )
  }
}

/** Wrong username/password pair on AUTH or HELLO AUTH. */
export class WrongPassError extends RedisCommandError {
  constructor() {
    super('invalid username-password pair or user is disabled.', 'WRONGPASS')
  }
}

/** A command was issued before authenticating on a password-protected server. */
export class NoAuthError extends RedisCommandError {
  constructor(message = 'Authentication required.') {
    super(message, 'NOAUTH')
  }
}

export class ExpectedIntegerError extends RedisCommandError {
  constructor() {
    super('value is not an integer or out of range')
  }
}

export class ExpectedFloatError extends RedisCommandError {
  constructor() {
    super('value is not a valid float')
  }
}

export class ResultingScoreNaNError extends RedisCommandError {
  constructor() {
    super('resulting score is not a number (NaN)')
  }
}

export class MinMaxNotFloatError extends RedisCommandError {
  constructor() {
    super('min or max is not a float')
  }
}

export class PositiveCountError extends RedisCommandError {
  constructor() {
    super('value is out of range, must be positive')
  }
}

export class LposRankZeroError extends RedisCommandError {
  constructor() {
    super(
      "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
    )
  }
}

export class LposCountNegativeError extends RedisCommandError {
  constructor() {
    super("COUNT can't be negative")
  }
}

export class LposMaxlenNegativeError extends RedisCommandError {
  constructor() {
    super("MAXLEN can't be negative")
  }
}

export class TimeoutNotFloatError extends RedisCommandError {
  constructor() {
    super('timeout is not a float or out of range')
  }
}

export class TimeoutNegativeError extends RedisCommandError {
  constructor() {
    super('timeout is negative')
  }
}

export class ZaddNxXxConflictError extends RedisCommandError {
  constructor() {
    super('XX and NX options at the same time are not compatible')
  }
}

export class ZaddGtLtNxConflictError extends RedisCommandError {
  constructor() {
    super('GT, LT, and/or NX options at the same time are not compatible')
  }
}

export class ExpireNxXxGtLtConflictError extends RedisCommandError {
  constructor() {
    super('NX and XX, GT or LT options at the same time are not compatible')
  }
}

export class ExpireGtLtConflictError extends RedisCommandError {
  constructor() {
    super('GT and LT options at the same time are not compatible')
  }
}

export class UnsupportedOptionError extends RedisCommandError {
  constructor(option: string) {
    super(`Unsupported option ${option}`)
  }
}

export class ZaddIncrPairError extends RedisCommandError {
  constructor() {
    super('INCR option supports a single increment-element pair')
  }
}

export class OffsetOutOfRangeError extends RedisCommandError {
  constructor() {
    super('offset is out of range')
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

export class InvalidExpireTimeError extends RedisCommandError {
  constructor(commandName: string) {
    super(`invalid expire time in '${commandName}' command`)
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
  constructor(slot: number) {
    super(`Hash slot ${slot} is not served`, 'CLUSTERDOWN')
  }
}

export class UnknownScriptSubcommandError extends RedisCommandError {
  constructor(subcommand: string | Buffer) {
    super(`unknown subcommand '${subcommand}'. Try SCRIPT HELP.`)
  }
}

export class UnknownClusterSubcommandError extends RedisCommandError {
  constructor(subcommand: string | Buffer) {
    super(`unknown subcommand '${subcommand}'. Try CLUSTER HELP.`)
  }
}

export class ScriptFlushOptionError extends RedisCommandError {
  constructor() {
    super('SCRIPT FLUSH only support SYNC|ASYNC option')
  }
}

export class ScriptDebugModeError extends RedisCommandError {
  constructor() {
    super('Use SCRIPT DEBUG YES/SYNC/NO')
  }
}

export class ScriptUnknownCommandError extends RedisCommandError {
  constructor() {
    super('Unknown Redis command called from script')
  }
}

export class ScriptNotAllowedCommandError extends RedisCommandError {
  constructor() {
    super('This Redis command is not allowed from script')
  }
}

export class ScriptCallNoCommandError extends RedisCommandError {
  constructor() {
    super('Please specify at least one argument for this redis lib call')
  }
}

export class WrongNumberOfKeysError extends RedisCommandError {
  constructor() {
    super(`Number of keys can't be greater than number of args`)
  }
}

export class NumKeysGreaterThanZeroError extends RedisCommandError {
  constructor() {
    super('numkeys should be greater than 0')
  }
}

export class CountGreaterThanZeroError extends RedisCommandError {
  constructor() {
    super('count should be greater than 0')
  }
}

export class LimitCantBeNegativeError extends RedisCommandError {
  constructor() {
    super(`LIMIT can't be negative`)
  }
}

export class StreamLimitRequiresApproxError extends RedisCommandError {
  constructor() {
    super('syntax error, LIMIT cannot be used without the special ~ option')
  }
}

export class StreamLimitNegativeError extends RedisCommandError {
  constructor() {
    super('The LIMIT argument must be >= 0.')
  }
}

export class InvalidLexRangeError extends RedisCommandError {
  constructor() {
    super('min or max not valid string range item')
  }
}

export class ZrangeLimitWithoutByError extends RedisCommandError {
  constructor() {
    super(
      'syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX',
    )
  }
}

export class ZrangeWithScoresByLexError extends RedisCommandError {
  constructor() {
    super('syntax error, WITHSCORES not supported in combination with BYLEX')
  }
}

export class NoScriptError extends RedisCommandError {
  constructor() {
    super('No matching script. Please use EVAL.', 'NOSCRIPT')
  }
}

export class ExecWithoutMultiError extends RedisCommandError {
  constructor() {
    super('EXEC without MULTI')
  }
}

export class DiscardWithoutMultiError extends RedisCommandError {
  constructor() {
    super('DISCARD without MULTI')
  }
}

export class WatchInsideMultiError extends RedisCommandError {
  constructor() {
    super('WATCH inside MULTI is not allowed')
  }
}

export class TransactionDiscardedError extends RedisCommandError {
  constructor() {
    super('Transaction discarded because of previous errors.', 'EXECABORT')
  }
}

/** EXEC itself is malformed (e.g. wrong arity) — discards the transaction immediately. */
export class ExecCommandAbortError extends RedisCommandError {
  constructor(reason: string) {
    super(`Transaction discarded because of: ${reason}`, 'EXECABORT')
  }
}

export class IndexOutOfRangeError extends RedisCommandError {
  constructor() {
    super('index out of range')
  }
}

export class NoSuchKeyError extends RedisCommandError {
  constructor() {
    super('no such key')
  }
}

/** `COPY src dst` (and `SELECT`) when source and destination resolve to the same object. */
export class SameObjectError extends RedisCommandError {
  constructor() {
    super('source and destination objects are the same')
  }
}

/** A database index outside `0 .. databaseCount - 1` (e.g. `COPY ... DB 99`, `SELECT 99`). */
export class DbIndexOutOfRangeError extends RedisCommandError {
  constructor() {
    super('DB index is out of range')
  }
}

export class InvalidStreamIdError extends RedisCommandError {
  constructor() {
    super('Invalid stream ID specified as stream command argument')
  }
}

export class StreamIdEqualOrSmallerError extends RedisCommandError {
  constructor() {
    super(
      'The ID specified in XADD is equal or smaller than the target stream top item',
    )
  }
}

export class StreamIdNotGreaterThanZeroError extends RedisCommandError {
  constructor() {
    super('The ID specified in XADD must be greater than 0-0')
  }
}

export class StreamElementTooLargeError extends RedisCommandError {
  constructor() {
    super('Elements are too large to be stored')
  }
}

export class StreamIdExhaustedError extends RedisCommandError {
  constructor() {
    super(
      'The stream has exhausted the last possible ID, unable to add more items',
    )
  }
}

export class HashValueNotIntegerError extends RedisCommandError {
  constructor() {
    super('hash value is not an integer')
  }
}

export class HashValueNotFloatError extends RedisCommandError {
  constructor() {
    super('hash value is not a float')
  }
}

export class UnknownRedisCommandError extends RedisCommandError {
  constructor(commandName: string | Buffer, args: readonly Buffer[]) {
    const argText = args
      .map(arg => `'${formatUnknownCommandArg(arg)}'`)
      .join(' ')
    const argsSuffix = argText.length > 0 ? `${argText} ` : ''
    super(
      `unknown command '${formatUnknownCommandName(commandName)}', with args beginning with: ${argsSuffix}`,
    )
  }
}

function formatUnknownCommandName(commandName: string | Buffer): string {
  return typeof commandName === 'string'
    ? commandName
    : formatUnknownCommandArg(commandName)
}

function formatUnknownCommandArg(arg: Buffer): string {
  const printable = arg.every(byte => byte >= 0x20 && byte <= 0x7e)
  const value = printable ? arg.toString() : `0x${arg.toString('hex')}`

  if (value.length <= 64) {
    return value
  }

  return `${value.slice(0, 61)}...`
}
