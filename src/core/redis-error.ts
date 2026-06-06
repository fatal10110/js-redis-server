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
    super(`invalid expire time in ${commandName} command`)
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
  constructor(sha: string) {
    super(
      `Unknown Redis command called from script script: ${sha}, on @user_script:1.`,
    )
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
