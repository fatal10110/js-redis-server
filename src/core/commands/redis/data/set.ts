import { DataCommand } from '..'
import { StringDataType } from '../../../../data-structures/string'
import {
  ExpectedInteger,
  RedisSyntaxError,
  WrongNumberOfArguments,
} from '../../../errors'
import { Node } from '../../../node'
import del from './del'

export class SetCommand implements DataCommand {
  getKeys(args: Buffer[]): Buffer[] {
    if (!args.length) {
      throw new WrongNumberOfArguments('set')
    }

    return [args[0]]
  }
  run(node: Node, args: Buffer[]): unknown {
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

    const existingData = node.db.get(key)

    if (!(existingData instanceof StringDataType)) {
      node.db.del(key)
    }

    // TODO handle flags
    node.db.set(key, new StringDataType(val))
    return 'OK'
  }
}

export default new SetCommand()
