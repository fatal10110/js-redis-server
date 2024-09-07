import { DataCommand } from '..'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../db'
import {
  ExpectedInteger,
  RedisSyntaxError,
  WrongNumberOfArguments,
} from '../../../errors'
import del from './del'

export class SetCommand implements DataCommand {
  getKeys(args: Buffer[]): Buffer[] {
    if (!args.length) {
      throw new WrongNumberOfArguments('set')
    }

    return [args[0]]
  }
  run(db: DB, args: Buffer[]): unknown {
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

    const existingData = db.get(key)

    if (!(existingData instanceof StringDataType)) {
      del.run(db, [key])
    }

    // TODO handle flags
    db.set(key, new StringDataType(val))
    return 'OK'
  }
}

export default new SetCommand()
