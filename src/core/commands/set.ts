import { StringDataType } from '../../data-structures/string'
import { DB } from '../db'
import {
  ExpectedInteger,
  RedisSyntaxError,
  WrongNumberOfArguments,
} from '../errors'
import del from './del'

// TODO
export default function set(
  db: DB,
  [key, val, ...args]: Buffer[],
): null | Buffer | string {
  if (!key || !val) {
    throw new WrongNumberOfArguments('set')
  }

  let xx = false
  let nx = false
  let get = false
  let argVal: null | number = null
  let argType: null | string = null
  let keepTTL = false

  for (let i = 0; i < args.length; i++) {
    switch (args[i].toString().toLowerCase()) {
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

        argVal = Number(args[i])

        if (isNaN(argVal)) {
          throw new ExpectedInteger()
        }
    }
  }

  if (nx && xx) {
    throw new RedisSyntaxError()
  }

  const existingData = db.data.get(key)

  if (!(existingData instanceof StringDataType)) {
    del(db, [key])
  }

  return 'OK'
}
