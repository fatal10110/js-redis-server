export class ListDataType {
  private readonly data: Buffer[]

  constructor() {
    this.data = []
  }

  lpush(value: Buffer): number {
    this.data.unshift(value)
    return this.data.length
  }

  rpush(value: Buffer): number {
    this.data.push(value)
    return this.data.length
  }

  lpop(): Buffer | null {
    return this.data.shift() || null
  }

  rpop(): Buffer | null {
    return this.data.pop() || null
  }

  llen(): number {
    return this.data.length
  }

  lindex(index: number): Buffer | null {
    if (index < 0) {
      index = this.data.length + index
    }
    return this.data[index] || null
  }

  lrange(start: number, stop: number): Buffer[] {
    if (start < 0) {
      start = this.data.length + start
    }
    if (stop < 0) {
      stop = this.data.length + stop
    }

    if (start < 0) start = 0
    if (stop >= this.data.length) stop = this.data.length - 1

    if (start > stop) return []

    return this.data.slice(start, stop + 1)
  }

  lset(index: number, value: Buffer): boolean {
    if (index < 0) {
      index = this.data.length + index
    }

    if (index >= 0 && index < this.data.length) {
      this.data[index] = value
      return true
    }
    return false
  }

  lrem(count: number, value: Buffer): number {
    let removed = 0
    const valueStr = value.toString()

    if (count === 0) {
      // Remove all occurrences
      for (let i = this.data.length - 1; i >= 0; i--) {
        if (this.data[i].toString() === valueStr) {
          this.data.splice(i, 1)
          removed++
        }
      }
    } else if (count > 0) {
      // Remove first count occurrences
      for (let i = 0; i < this.data.length && removed < count; i++) {
        if (this.data[i].toString() === valueStr) {
          this.data.splice(i, 1)
          removed++
          i-- // Adjust index after removal
        }
      }
    } else {
      // Remove last |count| occurrences
      const absCount = Math.abs(count)
      for (let i = this.data.length - 1; i >= 0 && removed < absCount; i--) {
        if (this.data[i].toString() === valueStr) {
          this.data.splice(i, 1)
          removed++
        }
      }
    }

    return removed
  }

  ltrim(start: number, stop: number): void {
    if (start < 0) {
      start = this.data.length + start
    }
    if (stop < 0) {
      stop = this.data.length + stop
    }

    if (start < 0) start = 0
    if (stop >= this.data.length) stop = this.data.length - 1

    if (start > stop) {
      this.data.length = 0
      return
    }

    this.data.splice(0, start)
    this.data.splice(stop - start + 1)
  }
}
