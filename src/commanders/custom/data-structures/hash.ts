import { HashValueNotInteger, HashValueNotFloat } from '../../../core/errors'

export class HashDataType {
  private readonly data: Map<string, Buffer>

  constructor() {
    this.data = new Map<string, Buffer>()
  }

  hset(field: Buffer, value: Buffer): number {
    const fieldStr = field.toString()
    const existed = this.data.has(fieldStr)
    this.data.set(fieldStr, value)
    return existed ? 0 : 1
  }

  hget(field: Buffer): Buffer | null {
    const fieldStr = field.toString()
    return this.data.get(fieldStr) || null
  }

  hdel(field: Buffer): number {
    const fieldStr = field.toString()
    return this.data.delete(fieldStr) ? 1 : 0
  }

  hexists(field: Buffer): boolean {
    const fieldStr = field.toString()
    return this.data.has(fieldStr)
  }

  hgetall(): Buffer[] {
    const result: Buffer[] = []
    for (const [field, value] of this.data.entries()) {
      result.push(Buffer.from(field), value)
    }
    return result
  }

  hkeys(): Buffer[] {
    return Array.from(this.data.keys()).map(key => Buffer.from(key))
  }

  hvals(): Buffer[] {
    return Array.from(this.data.values())
  }

  hlen(): number {
    return this.data.size
  }

  hmget(fields: Buffer[]): (Buffer | null)[] {
    return fields.map(field => this.hget(field))
  }

  hincrby(field: Buffer, increment: number): number {
    const fieldStr = field.toString()
    const current = this.data.get(fieldStr)
    let value = 0

    if (current) {
      value = parseInt(current.toString())
      if (isNaN(value)) {
        throw new HashValueNotInteger()
      }
    }

    const newValue = value + increment
    this.data.set(fieldStr, Buffer.from(newValue.toString()))
    return newValue
  }

  hincrbyfloat(field: Buffer, increment: number): number {
    const fieldStr = field.toString()
    const current = this.data.get(fieldStr)
    let value = 0

    if (current) {
      value = parseFloat(current.toString())
      if (isNaN(value)) {
        throw new HashValueNotFloat()
      }
    }

    const newValue = value + increment
    this.data.set(fieldStr, Buffer.from(newValue.toString()))
    return newValue
  }
}
