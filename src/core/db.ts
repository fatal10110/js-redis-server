import { HashDataType } from '../data-structures/hash'
import { ListDataType } from '../data-structures/list'
import { SetDataType } from '../data-structures/set'
import { StreamDataType } from '../data-structures/stream'
import { StringDataType } from '../data-structures/string'
import { SortedSetDataType } from '../data-structures/zset'
import { ReadOnlyNode } from './errors'

export type DataTypes =
  | HashDataType
  | ListDataType
  | SetDataType
  | StreamDataType
  | StringDataType
  | SortedSetDataType

export class DB {
  public readonly timings = new Map<Buffer, number>()
  public readonly data = new Map<Buffer, DataTypes>()

  constructor() {}

  get(key: Buffer) {
    const now = Date.now()
    const timing = this.timings.get(key)

    if (timing && timing <= now) {
      this.timings.delete(key)
      this.data.delete(key)
      return null
    }

    return this.data.get(key) || null
  }

  set(key: Buffer, val: DataTypes, expiration?: number) {
    this.data.set(key, val)

    if (expiration) {
      this.timings.set(key, expiration)
    }
  }
}
