export class SortedSetDataType {
  private readonly memberToScore: Map<string, number> = new Map()
  private readonly scoreToMembers: Map<number, Set<string>> = new Map()

  zadd(score: number, member: Buffer): number {
    const memberStr = member.toString()
    const existed = this.memberToScore.has(memberStr)

    // Remove from old score if it existed
    if (existed) {
      const oldScore = this.memberToScore.get(memberStr)!
      const oldScoreSet = this.scoreToMembers.get(oldScore)!
      oldScoreSet.delete(memberStr)
      if (oldScoreSet.size === 0) {
        this.scoreToMembers.delete(oldScore)
      }
    }

    // Add to new score
    this.memberToScore.set(memberStr, score)
    if (!this.scoreToMembers.has(score)) {
      this.scoreToMembers.set(score, new Set())
    }
    this.scoreToMembers.get(score)!.add(memberStr)

    return existed ? 0 : 1
  }

  zrem(member: Buffer): number {
    const memberStr = member.toString()
    if (!this.memberToScore.has(memberStr)) {
      return 0
    }

    const score = this.memberToScore.get(memberStr)!
    this.memberToScore.delete(memberStr)

    const scoreSet = this.scoreToMembers.get(score)!
    scoreSet.delete(memberStr)
    if (scoreSet.size === 0) {
      this.scoreToMembers.delete(score)
    }

    return 1
  }

  zscore(member: Buffer): number | null {
    const memberStr = member.toString()
    return this.memberToScore.get(memberStr) || null
  }

  zcard(): number {
    return this.memberToScore.size
  }

  zrange(start: number, stop: number, withScores: boolean = false): Buffer[] {
    const sortedMembers = this.getSortedMembers()
    const length = sortedMembers.length

    // Handle negative indices
    if (start < 0) start = length + start
    if (stop < 0) stop = length + stop

    // Clamp to valid range
    start = Math.max(0, start)
    stop = Math.min(length - 1, stop)

    if (start > stop) return []

    const result: Buffer[] = []
    for (let i = start; i <= stop; i++) {
      const member = sortedMembers[i]
      result.push(Buffer.from(member))
      if (withScores) {
        const score = this.memberToScore.get(member)!
        result.push(Buffer.from(score.toString()))
      }
    }

    return result
  }

  zrevrange(
    start: number,
    stop: number,
    withScores: boolean = false,
  ): Buffer[] {
    const sortedMembers = this.getSortedMembers().reverse()
    const length = sortedMembers.length

    // Handle negative indices
    if (start < 0) start = length + start
    if (stop < 0) stop = length + stop

    // Clamp to valid range
    start = Math.max(0, start)
    stop = Math.min(length - 1, stop)

    if (start > stop) return []

    const result: Buffer[] = []
    for (let i = start; i <= stop; i++) {
      const member = sortedMembers[i]
      result.push(Buffer.from(member))
      if (withScores) {
        const score = this.memberToScore.get(member)!
        result.push(Buffer.from(score.toString()))
      }
    }

    return result
  }

  zincrby(member: Buffer, increment: number): number {
    const memberStr = member.toString()
    const currentScore = this.memberToScore.get(memberStr) || 0
    const newScore = currentScore + increment

    // Remove from old score if it existed
    if (this.memberToScore.has(memberStr)) {
      const oldScore = this.memberToScore.get(memberStr)!
      const oldScoreSet = this.scoreToMembers.get(oldScore)!
      oldScoreSet.delete(memberStr)
      if (oldScoreSet.size === 0) {
        this.scoreToMembers.delete(oldScore)
      }
    }

    // Add to new score
    this.memberToScore.set(memberStr, newScore)
    if (!this.scoreToMembers.has(newScore)) {
      this.scoreToMembers.set(newScore, new Set())
    }
    this.scoreToMembers.get(newScore)!.add(memberStr)

    return newScore
  }

  zrank(member: Buffer): number | null {
    const memberStr = member.toString()
    if (!this.memberToScore.has(memberStr)) {
      return null
    }

    const sortedMembers = this.getSortedMembers()
    return sortedMembers.indexOf(memberStr)
  }

  zrevrank(member: Buffer): number | null {
    const memberStr = member.toString()
    if (!this.memberToScore.has(memberStr)) {
      return null
    }

    const sortedMembers = this.getSortedMembers().reverse()
    return sortedMembers.indexOf(memberStr)
  }

  zrangebyscore(min: number, max: number): Buffer[] {
    const result: Buffer[] = []

    for (const [member, score] of this.memberToScore.entries()) {
      if (score >= min && score <= max) {
        result.push(Buffer.from(member))
      }
    }

    // Sort results by score (ascending order)
    result.sort((a, b) => {
      const scoreA = this.memberToScore.get(a.toString())!
      const scoreB = this.memberToScore.get(b.toString())!
      if (scoreA !== scoreB) {
        return scoreA - scoreB
      }
      return a.toString().localeCompare(b.toString())
    })

    return result
  }

  zremrangebyscore(min: number, max: number): number {
    let removedCount = 0
    const membersToRemove: string[] = []

    // Find all members within the score range
    for (const [member, score] of this.memberToScore.entries()) {
      if (score >= min && score <= max) {
        membersToRemove.push(member)
      }
    }

    // Remove all found members
    for (const member of membersToRemove) {
      const score = this.memberToScore.get(member)!
      this.memberToScore.delete(member)

      const scoreSet = this.scoreToMembers.get(score)!
      scoreSet.delete(member)
      if (scoreSet.size === 0) {
        this.scoreToMembers.delete(score)
      }

      removedCount++
    }

    return removedCount
  }

  private getSortedMembers(): string[] {
    const allMembers: Array<{ member: string; score: number }> = []

    for (const [member, score] of this.memberToScore.entries()) {
      allMembers.push({ member, score })
    }

    // Sort by score first, then lexicographically by member for stable sorting
    allMembers.sort((a, b) => {
      if (a.score !== b.score) {
        return a.score - b.score
      }
      return a.member.localeCompare(b.member)
    })

    return allMembers.map(item => item.member)
  }

  zcount(min: number, max: number): number {
    let count = 0
    for (const [, score] of this.memberToScore.entries()) {
      if (score >= min && score <= max) {
        count++
      }
    }
    return count
  }

  zpopmin(count: number = 1): Buffer[] {
    const sortedMembers = this.getSortedMembers()
    const result: Buffer[] = []
    const toRemove = Math.min(count, sortedMembers.length)

    for (let i = 0; i < toRemove; i++) {
      const member = sortedMembers[i]
      const score = this.memberToScore.get(member)!
      result.push(Buffer.from(member))
      result.push(Buffer.from(score.toString()))
      this.zrem(Buffer.from(member))
    }

    return result
  }

  zpopmax(count: number = 1): Buffer[] {
    const sortedMembers = this.getSortedMembers().reverse()
    const result: Buffer[] = []
    const toRemove = Math.min(count, sortedMembers.length)

    for (let i = 0; i < toRemove; i++) {
      const member = sortedMembers[i]
      const score = this.memberToScore.get(member)!
      result.push(Buffer.from(member))
      result.push(Buffer.from(score.toString()))
      this.zrem(Buffer.from(member))
    }

    return result
  }
}
