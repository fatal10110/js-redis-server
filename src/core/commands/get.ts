import { StringDataType } from '../../data-structures/string'
import { DB } from '../db'
import { WrongNumberOfArguments, WrongType } from '../errors'

export default function get(db: DB, [key]: Buffer[]): null | Buffer {
  if (!key) {
    throw new WrongNumberOfArguments('get')
  }

  const val = db.get(key)

  if (!val) return null

  if (!(val instanceof StringDataType)) {
    throw new WrongType(key.toString())
  }

  return val.data
}
