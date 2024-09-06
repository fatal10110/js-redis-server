import { Discovery } from './cluster/network'

export class UserFacedError extends Error {}

export class WrongNumberOfArguments extends UserFacedError {
  constructor(cmdName: string) {
    super(`wrong number of arguments for '${cmdName}' command`)
    this.name = 'ERR'
  }
}

export class WrongType extends Error {
  constructor(keyName: string) {
    super(`Operation against ${keyName} key holding the wrong kind of value`)
    this.name = 'WRONGTYPE'
  }
}

export class ExpectedInteger extends UserFacedError {
  constructor() {
    super('value is not an integer or out of range')
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
  constructor(destination: Discovery, slot: number) {
    super(`${slot} ${destination.host}:${destination.port}`, {})
    this.name = 'MOVED'
  }
}
