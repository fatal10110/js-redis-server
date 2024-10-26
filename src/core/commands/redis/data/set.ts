import { Command, CommandResult, Node } from '../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import {
  ExpectedInteger,
  RedisSyntaxError,
  WrongNumberOfArguments,
} from '../../../errors'

export class SetCommand implements Command {
  constructor(private readonly node: Node) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (!args.length) {
      throw new WrongNumberOfArguments('set')
    }

    return [args[0]]
  }
  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    const [key, val, ...cmdArgs] = args

    if (!key || !val) {
      throw new WrongNumberOfArguments('set')
    }

    let xx = false
    let nx = false
    let get = false
    let argVal: null | number = null
    let argType: null | string = null
    let keepTTL = false

    for (let i = 0; i < cmdArgs.length; i++) {
      switch (cmdArgs[i].toString().toLowerCase()) {
        case 'xx':
          xx = true
          break
        case 'nx':
          nx = true
          break
        case 'keepttl':
          keepTTL = true
          break
        case 'get':
          get = true
          break
        case 'px':
        case 'pxat':
        case 'exat':
        case 'ex':
          if (argVal !== null) {
            throw new RedisSyntaxError()
          }

          i++

          argVal = Number(cmdArgs[i])

          if (isNaN(argVal)) {
            throw new ExpectedInteger()
          }
      }
    }

    if (nx && xx) {
      throw new RedisSyntaxError()
    }

    const existingData = this.node.db.get(key)

    if (!(existingData instanceof StringDataType)) {
      this.node.db.del(key)
    }

    // TODO handle flags
    this.node.db.set(key, new StringDataType(val))
    return { response: 'OK' }
  }
}

export default function (node: Node) {
  return function () {
    return new SetCommand(node)
  }
}
