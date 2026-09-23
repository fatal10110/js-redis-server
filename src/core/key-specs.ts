import { equalsAscii } from './ascii-case'
import type { CommandDefinition, CommandKeySpec } from './command-definition'
import { schemaKeyRange } from './command-schema'

/**
 * The keys a command's key specs pick out of `argv` (the command name at
 * index 0), following Redis's `getKeysUsingKeySpecs` without partial results:
 * a spec whose keyword is absent adds nothing, and a spec that cannot be
 * applied (a numkeys that is not a non-negative integer, or keys running past
 * the end of the command) makes the whole command keyless, which is how
 * cluster routing treats it. Only consulted for a command whose own parser
 * failed, which has no parsed keys to route by.
 */
export function keysFromKeySpecs(
  specs: readonly CommandKeySpec[],
  argv: readonly Buffer[],
): Buffer[] {
  const argc = argv.length
  const keys: Buffer[] = []
  for (const spec of specs) {
    const first = beginSearch(spec, argv)
    if (first === null) {
      continue
    }

    const range = findKeys(spec, argv, first)
    if (!range || range.last >= argc || range.last < range.first) {
      return []
    }

    for (let i = range.first; i <= range.last; i += range.step) {
      keys.push(argv[i])
    }
  }
  return keys
}

/**
 * The keys a queued command whose parser failed is routed by: its key specs
 * when it declares any, as `COMMAND INFO` reports them; otherwise the legacy
 * first/last/step range its schema implies. `rawArgs` excludes the command
 * name.
 */
export function rawCommandKeys(
  definition: CommandDefinition<unknown>,
  rawCommand: Buffer | string,
  rawArgs: readonly Buffer[],
): Buffer[] {
  const argv = [Buffer.from(rawCommand), ...rawArgs]
  const specs = definition.introspection?.keySpecs
  if (specs && specs.length > 0) {
    return keysFromKeySpecs(specs, argv)
  }

  const { firstKey, lastKey, keyStep } = schemaKeyRange(definition.schema)
  if (firstKey <= 0 || keyStep <= 0) {
    return []
  }
  return keysFromKeySpecs(
    [
      {
        flags: [],
        beginSearchIndex: firstKey,
        lastKey: lastKey < 0 ? lastKey : lastKey - firstKey,
        keyStep,
      },
    ],
    argv,
  )
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
  for (let i = start; i !== end; i += start <= end ? 1 : -1) {
    if (i >= argc || i < 1) {
      break
    }
    if (equalsAscii(argv[i], keyword.keyword.toLowerCase())) {
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
  const raw = argv[first + keynum.keyNumIdx].toString('latin1')
  if (!/^(0|-?[1-9][0-9]*)$/.test(raw)) {
    return null
  }
  const count = Number(raw)
  if (count < 0 || !Number.isSafeInteger(count)) {
    return null
  }
  const start = first + keynum.firstKey
  return { first: start, last: start + count - 1, step: keynum.keyStep }
}
