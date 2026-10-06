import type { CompatibilityProfile } from './compatibility'
import { RedisCommandError } from './redis-error'

/** The flags a `#!lua flags=...` shebang may declare (Redis 7.0+). */
export type ScriptShebangFlag =
  | 'no-writes'
  | 'allow-oom'
  | 'allow-stale'
  | 'no-cluster'
  | 'allow-cross-slot-keys'

const SCRIPT_SHEBANG_FLAGS: readonly ScriptShebangFlag[] = [
  'no-writes',
  'allow-oom',
  'allow-stale',
  'no-cluster',
  'allow-cross-slot-keys',
]

export function isScriptShebangFlag(flag: string): flag is ScriptShebangFlag {
  return (SCRIPT_SHEBANG_FLAGS as readonly string[]).includes(flag)
}

/**
 * A script split the way Redis 7.0+ compiles it (`evalExtractShebangFlags`).
 * `flags` is `null` for a script without a shebang, which runs in Redis's
 * backwards-compatible mode; a shebang script declares its flags, possibly
 * none.
 */
export type ParsedScript = {
  /** What the engine compiles: the shebang line blanked, its line feed kept. */
  body: Buffer
  flags: readonly ScriptShebangFlag[] | null
}

/**
 * Splits a script's `#!` shebang line from its body, on profiles that have
 * one (`script.shebang`); 6.2 compiles the whole script as Lua. The body
 * keeps the line feed, so Lua's line numbers still count the shebang line.
 * Throws a `RedisCommandError` with Redis's wording for a shebang it would
 * refuse, so neither EVAL nor SCRIPT LOAD caches the script.
 *
 * Redis 7.0 to Valkey 8.0 check the engine name first and only accept
 * `#!lua` exactly. Valkey 8.1 reads the options first and then looks the
 * engine up by name, ignoring case (`script.shebang-engine-lookup`).
 * Checked against redis-server 7.0.15 and the 7.0.15 / Valkey 8.1.0 / 9.0.0
 * sources.
 */
export function parseScriptShebang(
  script: Buffer,
  profile: CompatibilityProfile,
): ParsedScript {
  if (
    !profile.has('script.shebang') ||
    script[0] !== 0x23 /* # */ ||
    script[1] !== 0x21 /* ! */
  ) {
    return { body: script, flags: null }
  }

  // Redis finds the line feed with strchr(), which also stops at a NUL byte.
  const end = shebangEnd(script)
  if (end === -1) {
    throw new RedisCommandError('Invalid script shebang')
  }

  const parts = splitArgs(script.subarray(0, end))
  if (!parts || parts.length === 0) {
    throw new RedisCommandError('Invalid engine in script shebang')
  }

  const engineLookup = profile.has('script.shebang-engine-lookup')
  const engine = parts[0]
  if (!engineLookup && !engine.equals(LUA_SHEBANG)) {
    throw scriptError('Unexpected engine in script shebang: ', engine)
  }

  const flags: ScriptShebangFlag[] = []
  for (let i = 1; i < parts.length; i++) {
    const part = parts[i]
    if (!part.subarray(0, FLAGS_OPTION.length).equals(FLAGS_OPTION)) {
      throw scriptError('Unknown lua shebang option: ', part)
    }
    for (const name of splitFlags(part.subarray(FLAGS_OPTION.length))) {
      const flag = SCRIPT_SHEBANG_FLAGS.find(candidate =>
        name.equals(Buffer.from(candidate)),
      )
      if (!flag) {
        throw scriptError('Unexpected flag in script shebang: ', name)
      }
      flags.push(flag)
    }
  }

  if (engineLookup) {
    const name = engine.subarray(2)
    if (name.toString('latin1').toLowerCase() !== 'lua') {
      throw scriptError(
        "Could not find scripting engine '",
        Buffer.concat([name, Buffer.from("'")]),
      )
    }
  }

  return { body: script.subarray(end), flags }
}

const LUA_SHEBANG = Buffer.from('#!lua')
const FLAGS_OPTION = Buffer.from('flags=')

/** An error whose message echoes raw script bytes. */
function scriptError(prefix: string, echoed: Buffer): RedisCommandError {
  return new RedisCommandError(Buffer.concat([Buffer.from(prefix), echoed]))
}

/** `sdssplitlen(value, ",")`: an empty value has no elements. */
function splitFlags(value: Buffer): Buffer[] {
  if (value.length === 0) {
    return []
  }
  const names: Buffer[] = []
  let start = 0
  for (let i = 0; i <= value.length; i++) {
    if (i === value.length || value[i] === 0x2c /* , */) {
      names.push(value.subarray(start, i))
      start = i + 1
    }
  }
  return names
}

/** The index of the shebang's line feed, or -1 when a NUL or the end comes first. */
function shebangEnd(script: Buffer): number {
  for (let i = 0; i < script.length; i++) {
    if (script[i] === 0x0a) {
      return i
    }
    if (script[i] === 0x00) {
      return -1
    }
  }
  return -1
}

/**
 * Redis's `sdssplitargs()`: whitespace-separated tokens, with `"..."`
 * (C-style escapes) and `'...'` quoting. `null` for unbalanced quotes or a
 * closing quote not followed by a space, as Redis returns NULL there.
 */
function splitArgs(line: Buffer): Buffer[] | null {
  const args: Buffer[] = []
  let p = 0
  const at = (index: number) => (index < line.length ? line[index] : 0)

  for (;;) {
    while (p < line.length && isSpace(line[p])) {
      p++
    }
    if (p >= line.length) {
      return args
    }

    const current: number[] = []
    let inDouble = false
    let inSingle = false
    let done = false
    while (!done) {
      const c = at(p)
      if (inDouble) {
        if (
          c === 0x5c &&
          at(p + 1) === 0x78 &&
          isHex(at(p + 2)) &&
          isHex(at(p + 3))
        ) {
          current.push(parseInt(String.fromCharCode(at(p + 2), at(p + 3)), 16))
          p += 3
        } else if (c === 0x5c && at(p + 1) !== 0) {
          p++
          current.push(unescape(at(p)))
        } else if (c === 0x22) {
          if (at(p + 1) !== 0 && !isSpace(at(p + 1))) {
            return null
          }
          done = true
        } else if (c === 0) {
          return null
        } else {
          current.push(c)
        }
      } else if (inSingle) {
        if (c === 0x5c && at(p + 1) === 0x27) {
          p++
          current.push(0x27)
        } else if (c === 0x27) {
          if (at(p + 1) !== 0 && !isSpace(at(p + 1))) {
            return null
          }
          done = true
        } else if (c === 0) {
          return null
        } else {
          current.push(c)
        }
      } else if (
        c === 0x20 ||
        c === 0x0a ||
        c === 0x0d ||
        c === 0x09 ||
        c === 0
      ) {
        done = true
      } else if (c === 0x22) {
        inDouble = true
      } else if (c === 0x27) {
        inSingle = true
      } else {
        current.push(c)
      }
      if (c !== 0) {
        p++
      }
    }
    args.push(Buffer.from(current))
  }
}

function unescape(c: number): number {
  switch (c) {
    case 0x6e: // n
      return 0x0a
    case 0x72: // r
      return 0x0d
    case 0x74: // t
      return 0x09
    case 0x62: // b
      return 0x08
    case 0x61: // a
      return 0x07
    default:
      return c
  }
}

/** C's isspace() in the C locale. */
function isSpace(c: number): boolean {
  return c === 0x20 || (c >= 0x09 && c <= 0x0d)
}

function isHex(c: number): boolean {
  return (
    (c >= 0x30 && c <= 0x39) ||
    (c >= 0x41 && c <= 0x46) ||
    (c >= 0x61 && c <= 0x66)
  )
}
