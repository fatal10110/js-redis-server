import { HashDataType } from '../data-structures/hash'
import { ListDataType } from '../data-structures/list'
import { SetDataType } from '../data-structures/set'
import { StreamDataType } from '../data-structures/stream'
import { StringDataType } from '../data-structures/string'
import { SortedSetDataType } from '../data-structures/zset'

export type DataTypes =
  | HashDataType
  | ListDataType
  | SetDataType
  | StreamDataType
  | StringDataType
  | SortedSetDataType

export class DB {
  // TODO solve this, storing more than twice the size of the key
  private readonly mapping = new Map<string, Buffer>()
  private readonly timings = new Map<Buffer, number>()
  private readonly data = new Map<Buffer, DataTypes>()

  constructor() {}

  get(rawKey: Buffer) {
    const key = this.mapping.get(rawKey.toString('binary'))

    if (!key) {
      return null
    }

    const now = Date.now()
    const timing = this.timings.get(key)

    if (timing && timing <= now) {
      this.timings.delete(key)
      this.data.delete(key)
      return null
    }

    return this.data.get(key) || null
  }

  set(rawKey: Buffer, val: DataTypes, expiration?: number) {
    const stringifiedKey = rawKey.toString('binary')
    let key = this.mapping.get(stringifiedKey)

    if (!key) {
      key = rawKey
      this.mapping.set(stringifiedKey, rawKey)
    }

    this.data.set(key, val)

    if (expiration) {
      this.timings.set(key, expiration)
    }
  }

  del(rawKey: Buffer) {
    const stringifiedKey = rawKey.toString('binary')
    const existingRef = this.mapping.get(stringifiedKey)

    if (!existingRef) {
      return false
    }

    this.timings.delete(existingRef)
    this.data.delete(existingRef)

    return true
  }
}
