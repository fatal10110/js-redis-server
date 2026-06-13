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
