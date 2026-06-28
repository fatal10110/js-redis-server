// Pure helpers shared by the demo terminal: turning a native reply into
// redis-cli-style text, and splitting an input line into command args.

/** A decoded reply from the in-memory connection (mirrors RedisNativeReply). */
export type Reply =
  | string
  | number
  | bigint
  | boolean
  | null
  | Reply[]
  | { [key: string]: Reply }

/**
 * Split a command line into arguments, redis-cli style: whitespace-separated,
 * but double quotes group a token (so `EVAL "return 1" 0` is three args). A
 * backslash inside quotes escapes the next char.
 */
export function tokenize(line: string): string[] {
  const tokens: string[] = []
  let token = ''
  let inQuotes = false
  let started = false

  for (let i = 0; i < line.length; i++) {
    const ch = line[i]
    if (inQuotes) {
      if (ch === '\\' && i + 1 < line.length) {
        token += line[++i]
      } else if (ch === '"') {
        inQuotes = false
      } else {
        token += ch
      }
      continue
    }
    if (ch === '"') {
      inQuotes = true
      started = true
    } else if (ch === ' ' || ch === '\t') {
      if (started) {
        tokens.push(token)
        token = ''
        started = false
      }
    } else {
      token += ch
      started = true
    }
  }
  if (started) {
    tokens.push(token)
  }
  return tokens
}

/** Render a reply the way `redis-cli` prints it (recursive, indented lists). */
export function formatReply(reply: Reply): string {
  if (reply === null) {
    return '(nil)'
  }
  if (typeof reply === 'string') {
    return quote(reply)
  }
  if (typeof reply === 'number' || typeof reply === 'bigint') {
    return `(integer) ${reply}`
  }
  if (typeof reply === 'boolean') {
    return reply ? '(true)' : '(false)'
  }
  if (Array.isArray(reply)) {
    return formatList(reply)
  }
  // A map reply (e.g. HGETALL) — flatten to alternating key/value entries.
  return formatList(Object.entries(reply).flat() as Reply[])
}

function formatList(items: Reply[]): string {
  if (items.length === 0) {
    return '(empty array)'
  }
  const width = String(items.length).length
  return items
    .map((item, i) => {
      const prefix = `${String(i + 1).padStart(width)}) `
      const pad = ' '.repeat(prefix.length)
      const [head, ...rest] = formatReply(item).split('\n')
      return [prefix + head, ...rest.map(line => pad + line)].join('\n')
    })
    .join('\n')
}

function quote(value: string): string {
  return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`
}
