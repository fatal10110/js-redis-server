import crypto from 'node:crypto'

export class RedisScriptCache {
  private readonly scripts = new Map<string, Buffer>()

  load(script: Buffer): string {
    const sha = crypto.createHash('sha1').update(script).digest('hex')
    this.scripts.set(sha, Buffer.from(script))
    return sha
  }

  get(sha: string): Buffer | null {
    const script = this.scripts.get(normalizeSha(sha))
    return script ? Buffer.from(script) : null
  }

  exists(sha: string): boolean {
    return this.scripts.has(normalizeSha(sha))
  }

  existsAll(shas: readonly string[]): boolean[] {
    return shas.map(sha => this.exists(sha))
  }

  flush(): void {
    this.scripts.clear()
  }

  size(): number {
    return this.scripts.size
  }
}

function normalizeSha(sha: string): string {
  return sha.toLowerCase()
}
