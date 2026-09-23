// Pure helpers shared by the demo terminal: turning a native reply into
// redis-cli-style text, and splitting an input line into command args.

/** A decoded reply from the in-memory connection (mirrors RedisNativeReply). */
export type Reply =
  | string
  | number
  | bigint
  | boolean
  | Buffer
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
  let quote: '"' | "'" | null = null
  let started = false

  for (let i = 0; i < line.length; i++) {
    const ch = line[i]
    if (quote === '"') {
      // Double quotes process backslash escapes (like redis-cli).
      if (ch === '\\' && i + 1 < line.length) {
        token += line[++i]
      } else if (ch === '"') {
        quote = null
      } else {
        token += ch
      }
      continue
    }
    if (quote === "'") {
      // Single quotes are literal, except \' is an escaped quote (redis-cli).
      if (ch === '\\' && line[i + 1] === "'") {
        token += "'"
        i++
      } else if (ch === "'") {
        quote = null
      } else {
        token += ch
      }
      continue
    }
    if (ch === '"' || ch === "'") {
      quote = ch
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
  // Checked as Uint8Array (Buffer's base class) so this needs no Buffer global;
  // must precede the map branch below, which would otherwise treat the bytes
  // as an object and print their indices.
  if (reply instanceof Uint8Array) {
    return quoteBytes(reply)
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

const BYTE_ESCAPES: Record<number, string> = {
  0x5c: '\\\\', // backslash
  0x22: '\\"', // double quote
  0x0a: '\\n',
  0x0d: '\\r',
  0x09: '\\t',
  0x07: '\\a',
  0x08: '\\b',
}

/**
 * Quote raw bytes exactly as `redis-cli` does (its `sdscatrepr`): printable
 * ASCII as-is, `\\` and `"` backslash-escaped, `\n` `\r` `\t` `\a` `\b` as
 * their C escapes, and every other byte as lowercase `\xNN` — so `éé` (UTF-8
 * `c3 a9 c3 a9`) renders `"\xc3\xa9\xc3\xa9"`.
 */
function quoteBytes(bytes: Uint8Array): string {
  let out = '"'
  for (const byte of bytes) {
    const escape = BYTE_ESCAPES[byte]
    if (escape !== undefined) {
      out += escape
    } else if (byte >= 0x20 && byte <= 0x7e) {
      out += String.fromCharCode(byte)
    } else {
      out += `\\x${byte.toString(16).padStart(2, '0')}`
    }
  }
  return out + '"'
}
