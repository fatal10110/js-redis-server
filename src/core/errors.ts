export class WrongNumberOfArguments extends Error {
  constructor(cmdName: string) {
    super(`ERR wrong number of arguments for '${cmdName}' command`)
  }
}

export class WrongType extends Error {
  constructor(keyName: string) {
    super(
      `WRONGTYPE Operation against ${keyName} key holding the wrong kind of value`,
    )
  }
}

export class ExpectedInteger extends Error {
  constructor() {
    super('ERR value is not an integer or out of range')
  }
}

export class RedisSyntaxError extends Error {
  constructor() {
    super('ERR syntax error')
  }
}

export class UnknownCommand extends Error {
  constructor(cmdName: string | Buffer, args: Buffer[] | string[]) {
    super(
      `ERR unknown command '${cmdName}', with args beginning with: ${args.join(' ')}`,
    )
  }
}
