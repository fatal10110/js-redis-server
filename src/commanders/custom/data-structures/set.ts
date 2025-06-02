export class SetDataType {
  private readonly data: Set<string>

  constructor() {
    this.data = new Set<string>()
  }

  sadd(value: Buffer): number {
    const valueStr = value.toString()
    const sizeBefore = this.data.size
    this.data.add(valueStr)
    return this.data.size - sizeBefore
  }

  srem(value: Buffer): number {
    const valueStr = value.toString()
    return this.data.delete(valueStr) ? 1 : 0
  }

  sismember(value: Buffer): boolean {
    const valueStr = value.toString()
    return this.data.has(valueStr)
  }

  smembers(): Buffer[] {
    return Array.from(this.data).map(member => Buffer.from(member))
  }

  scard(): number {
    return this.data.size
  }

  spop(): Buffer | null {
    if (this.data.size === 0) return null

    const members = Array.from(this.data)
    const randomIndex = Math.floor(Math.random() * members.length)
    const member = members[randomIndex]
    this.data.delete(member)
    return Buffer.from(member)
  }

  srandmember(): Buffer | null {
    if (this.data.size === 0) return null

    const members = Array.from(this.data)
    const randomIndex = Math.floor(Math.random() * members.length)
    return Buffer.from(members[randomIndex])
  }

  sdiff(otherSets: SetDataType[]): Buffer[] {
    const result = new Set(this.data)

    for (const otherSet of otherSets) {
      for (const member of otherSet.data) {
        result.delete(member)
      }
    }

    return Array.from(result).map(member => Buffer.from(member))
  }

  sinter(otherSets: SetDataType[]): Buffer[] {
    let result = new Set(this.data)

    for (const otherSet of otherSets) {
      const intersection = new Set<string>()
      for (const member of result) {
        if (otherSet.data.has(member)) {
          intersection.add(member)
        }
      }
      result = intersection
    }

    return Array.from(result).map(member => Buffer.from(member))
  }

  sunion(otherSets: SetDataType[]): Buffer[] {
    const result = new Set(this.data)

    for (const otherSet of otherSets) {
      for (const member of otherSet.data) {
        result.add(member)
      }
    }

    return Array.from(result).map(member => Buffer.from(member))
  }

  smove(destination: SetDataType, member: Buffer): boolean {
    const memberStr = member.toString()
    if (!this.data.has(memberStr)) {
      return false
    }

    this.data.delete(memberStr)
    destination.data.add(memberStr)
    return true
  }
}
