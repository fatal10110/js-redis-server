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
}
