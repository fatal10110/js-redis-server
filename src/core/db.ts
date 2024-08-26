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
  public readonly timings = new Map<Buffer, number>()
  public readonly data = new Map<Buffer, DataTypes>()

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
}
