export class UserFacedError extends Error {}

export class WrongNumberOfArguments extends UserFacedError {
  constructor(cmdName: string) {
    super(`wrong number of arguments for '${cmdName}' command`)
    this.name = 'ERR'
  }
}

export class WrongType extends UserFacedError {
  constructor() {
    super(`Operation against a key holding the wrong kind of value`)
    this.name = 'WRONGTYPE'
  }
}

export class InvalidExpireTime extends UserFacedError {
  constructor(cmdName: string) {
    super(`invalid expire time in ${cmdName} command`)
    this.name = 'ERR'
  }
}

export class ExpectedInteger extends UserFacedError {
  constructor() {
    super('value is not an integer or out of range')
    this.name = 'ERR'
  }
}

export class ExpectedFloat extends UserFacedError {
  constructor() {
    super('value is not a valid float')
    this.name = 'ERR'
  }
}

export class RedisSyntaxError extends UserFacedError {
  constructor() {
    super('syntax error')
    this.name = 'ERR'
  }
}

export class UnknownCommand extends UserFacedError {
  constructor(cmdName: string | Buffer, args: Buffer[] | string[]) {
    super(
      `unknown command '${cmdName}', with args beginning with: ${args.join(' ')}`,
    )
    this.name = 'ERR'
  }
}

export class UnknownScriptCommand extends UserFacedError {
  constructor(sha: string) {
    super(
      `Unknown Redis command called from script script: ${sha}, on @user_script:1.`,
    )
    this.name = 'ERR'
  }
}

export class UnknwonClusterSubCommand extends UserFacedError {
  constructor(subCommand: string | Buffer) {
    super(`unknown subcommand '${subCommand}'. Try CLUSTER HELP.`)
    this.name = 'ERR'
  }
}

export class UnknowScriptSubCommand extends UserFacedError {
  constructor(subCommand: string | Buffer) {
    super(`unknown subcommand '${subCommand}'. Try SCRIPT HELP.`)
    this.name = 'ERR'
  }
}

export class UnknwonClientSubCommand extends UserFacedError {
  constructor(subCommand: string | Buffer) {
    super(`unknown subcommand '${subCommand}'. Try CLIENT HELP.`)
    this.name = 'ERR'
  }
}

export class ReadOnlyNode extends UserFacedError {
  constructor() {
    super(`The node suppory only read commands`)
    this.name = 'READONLY'
  }
}

export class CorssSlot extends UserFacedError {
  constructor() {
    super(`Keys in request don't hash to the same slot`)
    this.name = 'CROSSSLOT'
  }
}

export class MovedError extends UserFacedError {
  constructor(host: string, port: number, slot: number) {
    super(`${slot} ${host}:${port}`, {})
    this.name = 'MOVED'
  }
}

export class WrongNumberOfKeys extends UserFacedError {
  constructor() {
    super(`Number of keys can't be greater than number of args`)
    this.name = 'ERR'
  }
}

export class NoScript extends UserFacedError {
  constructor() {
    super(`No matching script. Please use EVAL.`)
    this.name = 'NOSCRIPT'
  }
}

export class NestedMulti extends UserFacedError {
  constructor() {
    super(`MULTI calls can not be nested`)
    this.name = 'ERR'
  }
}

export class TransactionDiscardedWithReson extends UserFacedError {
  constructor(reason: string) {
    super(`Transaction discarded because of: ${reason}`)
    this.name = 'EXECABORT'
  }
}

export class TransactionDiscardedWithError extends UserFacedError {
  constructor() {
    super(`Transaction discarded because of previous errors.`)
    this.name = 'EXECABORT'
  }
}

export class OutOfRangeIndex extends UserFacedError {
  constructor() {
    super('index out of range')
    this.name = 'ERR'
  }
}

export class NoMulti extends UserFacedError {
  constructor() {
    super(`EXEC without MULTI`)
    this.name = 'ERR'
  }
}

export class HashValueNotInteger extends UserFacedError {
  constructor() {
    super('hash value is not an integer')
    this.name = 'ERR'
  }
}

export class HashValueNotFloat extends UserFacedError {
  constructor() {
    super('hash value is not a float')
    this.name = 'ERR'
  }
}
