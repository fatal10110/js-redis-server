import { equalsAscii } from './ascii-case'
import { lookupSubcommandEntry } from './command-arity'
import {
  introspectionFor,
  type CommandDefinition,
  type CommandIntrospection,
  type CommandKeySpec,
} from './command-definition'
import {
  isIntegerToken,
  schemaKeyRange,
  type CommandSchema,
} from './command-schema'
import type { CompatibilityProfile } from './compatibility'

/** A key with the flags of the key spec that found it. */
export type KeyWithFlags = { key: Buffer; flags: readonly string[] }

/**
 * The keys a command's key specs pick out of `argv` (the command name at
 * index 0), each with its spec's flags, following Redis's
 * `getKeysUsingKeySpecs` without partial results: a spec whose keyword is
 * absent adds nothing, and a spec that cannot be applied (a numkeys that is
 * not a non-negative integer, keys running past the end of the command, a
 * step below 1) makes the whole lookup fail with `null`. This is how
 * `COMMAND GETKEYSANDFLAGS` attributes flags to keys.
 */
export function keysFromKeySpecs(
  specs: readonly CommandKeySpec[],
  argv: readonly Buffer[],
): KeyWithFlags[] | null {
  const argc = argv.length
  const keys: KeyWithFlags[] = []
  for (const spec of specs) {
    const first = beginSearch(spec, argv)
    if (first === null) {
      continue
    }

    const range = findKeys(spec, argv, first)
    if (
      !range ||
      range.step < 1 ||
      range.last >= argc ||
      range.last < range.first
    ) {
      return null
    }

    for (let i = range.first; i <= range.last; i += range.step) {
      keys.push({ key: argv[i], flags: spec.flags })
    }
  }
  return keys
}

/**
 * The keys a queued command whose own parser failed is routed by in a
 * cluster, as Redis's `getKeysFromCommand` finds them without running the
 * command: the command's getkeys proc (`rawKeys`) when it has one, otherwise
 * the legacy first/last/step range `COMMAND INFO` reports for the entry
 * lookup resolves to (from 7.0 a container's `container|subcommand` entry).
 * A range that runs past the end of the command leaves it keyless.
 * `rawArgs` excludes the command name.
 */
export function rawCommandKeys(
  definition: CommandDefinition<unknown>,
  rawCommand: Buffer | string,
  rawArgs: readonly Buffer[],
  profile: CompatibilityProfile,
): Buffer[] {
  const argv = [Buffer.from(rawCommand), ...rawArgs]
  if (definition.rawKeys) {
    return [...definition.rawKeys(argv)]
  }

  const subcommand = lookupSubcommandEntry(definition, rawArgs, profile)
  const range = subcommand
    ? legacyKeyRange(introspectionFor(subcommand.introspection, profile))
    : legacyKeyRange(
        introspectionFor(definition.introspection, profile),
        definition.schema,
      )
  return legacyRangeKeys(range, argv)
}

/** The legacy first/last/step key range, as `COMMAND INFO` reports it. */
export type LegacyKeyRange = {
  firstKey: number
  lastKey: number
  keyStep: number
}

/**
 * The legacy first/last/step triple of a command-table entry. Declared key
 * specs win, folded the way Redis's `populateCommandLegacyRangeSpec` does:
 * only index + range specs count (a keyword or keynum spec makes the keys
 * movable), and several merge only while each is a plain step-1 range picking
 * up where the previous one ended. Without specs, the schema's key positions
 * stand in for them.
 */
export function legacyKeyRange(
  introspection: CommandIntrospection | undefined,
  schema?: CommandSchema<unknown>,
): LegacyKeyRange {
  const declared = introspection?.keySpecs ?? []
  if (declared.length === 0) {
    return schema
      ? schemaKeyRange(schema)
      : { firstKey: 0, lastKey: 0, keyStep: 0 }
  }

  const specs = declared.filter(
    spec => !spec.beginSearchKeyword && !spec.findKeysKeynum,
  )
  if (specs.length === 0) {
    return { firstKey: 0, lastKey: 0, keyStep: 0 }
  }

  if (specs.length === 1) {
    const [spec] = specs
    return {
      firstKey: spec.beginSearchIndex,
      lastKey: absoluteLastKey(spec),
      keyStep: spec.keyStep,
    }
  }

  let firstKey = 0
  let lastKey = 0
  for (const spec of specs) {
    if (spec.keyStep !== 1) {
      continue
    }

    if (firstKey !== 0 && lastKey !== spec.beginSearchIndex - 1) {
      continue
    }

    firstKey = firstKey || spec.beginSearchIndex
    lastKey = absoluteLastKey(spec)
  }

  return firstKey === 0
    ? { firstKey: 0, lastKey: 0, keyStep: 0 }
    : { firstKey, lastKey, keyStep: 1 }
}

// A non-negative spec `lastKey` is relative to the spec's first key; a
// negative one counts back from the end of the command and is kept as is.
function absoluteLastKey(spec: CommandKeySpec): number {
  return spec.lastKey < 0 ? spec.lastKey : spec.beginSearchIndex + spec.lastKey
}

/**
 * Redis's `getKeysUsingLegacyRangeSpec`: the keys a first/last/step range
 * picks out of `argv`. A range that runs past the end of the command gives no
 * keys at all (the command then answers its own arity or syntax error).
 */
function legacyRangeKeys(
  range: LegacyKeyRange,
  argv: readonly Buffer[],
): Buffer[] {
  const { firstKey, keyStep } = range
  if (firstKey <= 0 || keyStep < 1) {
    return []
  }

  const argc = argv.length
  const last = range.lastKey < 0 ? argc + range.lastKey : range.lastKey
  const keys: Buffer[] = []
  for (let i = firstKey; i <= last; i += keyStep) {
    if (i >= argc) {
      return []
    }
    keys.push(argv[i])
  }
  return keys
}

/**
 * C `atoi`: optional leading whitespace and sign, then as many digits as
 * there are; anything else stops the number, and no digits reads as 0. Redis's
 * getkeys procs read numkeys this way, so `2abc` counts as 2.
 */
export function atoi(value: Buffer): number {
  const match = /^[ \t\n\v\f\r]*([+-]?\d+)/.exec(value.toString('latin1'))
  if (!match) {
    return 0
  }
  const n = Number(match[1])
  // C atoi overflow is undefined; clamping keeps the sanity check below sound.
  return Math.max(-2147483648, Math.min(2147483647, n))
}

/**
 * Redis's `genericGetKeys`, the getkeys proc of the numkeys commands:
 * `numkeys` at `keyCountOfs` (read with {@link atoi}), keys from
 * `firstKeyOfs` every `keyStep`, plus the destination at `storeKeyOfs` when it
 * is not 0. A count below 1 or past the end of the command gives no keys.
 */
export function numkeysGetKeys(
  storeKeyOfs: number,
  keyCountOfs: number,
  firstKeyOfs: number,
  keyStep = 1,
): (argv: readonly Buffer[]) => Buffer[] {
  return argv => {
    const argc = argv.length
    if (keyCountOfs >= argc) {
      return []
    }
    const num = atoi(argv[keyCountOfs])
    if (num < 1 || num > Math.floor((argc - firstKeyOfs) / keyStep)) {
      return []
    }

    const keys: Buffer[] = []
    for (let i = 0; i < num; i++) {
      keys.push(argv[firstKeyOfs + i * keyStep])
    }
    if (storeKeyOfs) {
      keys.push(argv[storeKeyOfs])
    }
    return keys
  }
}

/**
 * Redis's `xreadGetKeys` (XREAD and XREADGROUP): the options are skipped
 * until `STREAMS`, and the keys are the first half of what follows. Anything
 * unexpected before `STREAMS`, or an odd or empty tail, gives no keys.
 */
export function xreadGetKeys(argv: readonly Buffer[]): Buffer[] {
  const argc = argv.length
  let streamsPos = -1
  for (let i = 1; i < argc; i++) {
    const arg = argv[i]
    if (equalsAscii(arg, 'block') || equalsAscii(arg, 'count')) {
      i++
    } else if (equalsAscii(arg, 'group')) {
      i += 2
    } else if (equalsAscii(arg, 'noack')) {
      continue
    } else if (equalsAscii(arg, 'streams')) {
      streamsPos = i
      break
    } else {
      break
    }
  }

  const num = argc - streamsPos - 1
  if (streamsPos === -1 || num === 0 || num % 2 !== 0) {
    return []
  }
  return argv.slice(streamsPos + 1, streamsPos + 1 + num / 2)
}

/**
 * Redis's `georadiusGetKeys` (GEORADIUS and GEORADIUSBYMEMBER): the key, plus
 * the destination of the last `STORE` / `STOREDIST` from argument 5 on.
 */
export function georadiusGetKeys(argv: readonly Buffer[]): Buffer[] {
  let storedKey = -1
  for (let i = 5; i < argv.length; i++) {
    const arg = argv[i]
    if (
      (equalsAscii(arg, 'store') || equalsAscii(arg, 'storedist')) &&
      i + 1 < argv.length
    ) {
      storedKey = i + 1
      i++
    }
  }
  return storedKey === -1 ? [argv[1]] : [argv[1], argv[storedKey]]
}

/**
 * Redis's `sortGetKeys`: the key, plus the destination of the last `STORE`,
 * skipping the arguments of `LIMIT`, `GET` and `BY`.
 */
export function sortGetKeys(argv: readonly Buffer[]): Buffer[] {
  const skips: Record<string, number> = { limit: 2, get: 1, by: 1 }
  let storedKey = -1
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i].toString('latin1').toLowerCase()
    if (Object.prototype.hasOwnProperty.call(skips, arg)) {
      i += skips[arg]
    } else if (arg === 'store' && i + 1 < argv.length) {
      storedKey = i + 1
    }
  }
  return storedKey === -1 ? [argv[1]] : [argv[1], argv[storedKey]]
}

function beginSearch(
  spec: CommandKeySpec,
  argv: readonly Buffer[],
): number | null {
  const keyword = spec.beginSearchKeyword
  if (!keyword) {
    return spec.beginSearchIndex
  }

  const argc = argv.length
  const forward = keyword.startFrom > 0
  const start = forward ? keyword.startFrom : argc + keyword.startFrom
  const end = forward ? argc - 1 : 1
  const target = keyword.keyword.toLowerCase()
  for (let i = start; i !== end; i += start <= end ? 1 : -1) {
    if (i >= argc || i < 1) {
      break
    }
    if (equalsAscii(argv[i], target)) {
      return i + 1
    }
  }
  return null
}

function findKeys(
  spec: CommandKeySpec,
  argv: readonly Buffer[],
  first: number,
): { first: number; last: number; step: number } | null {
  const argc = argv.length
  const keynum = spec.findKeysKeynum
  if (!keynum) {
    if (spec.lastKey >= 0) {
      return { first, last: first + spec.lastKey, step: spec.keyStep }
    }
    const last = spec.limit
      ? first + (Math.floor((argc - first) / spec.limit) + spec.lastKey)
      : argc + spec.lastKey
    return { first, last, step: spec.keyStep }
  }

  if (keynum.keyNumIdx >= argc - first) {
    return null
  }
  // Key specs read numkeys strictly (string2ll), unlike the getkeys procs.
  const raw = argv[first + keynum.keyNumIdx].toString('latin1')
  if (!isIntegerToken(raw)) {
    return null
  }
  const count = Number(raw)
  if (count < 0 || !Number.isSafeInteger(count)) {
    return null
  }
  const start = first + keynum.firstKey
  return { first: start, last: start + count - 1, step: keynum.keyStep }
}
