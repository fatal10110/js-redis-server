import { DB } from '../db'
import { WrongNumberOfArguments, WrongType } from '../errors'

export default function get(db: DB, keys: Buffer[]): number {
  if (!keys.length) {
    throw new WrongNumberOfArguments('del')
  }

  let counter = 0

  for (const key of keys) {
    if (db.data.delete(key)) {
      counter++
    }

    db.timings.delete(key)
  }

  return counter
}
