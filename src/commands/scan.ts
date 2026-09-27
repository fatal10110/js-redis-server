import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import { RedisCommandError, errors } from '../core/redis-error'
import { redisGlobMatch } from '../core/glob'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { CompatibilityProfile } from '../core/compatibility'
import type { RedisDataTypeName } from '../state'
import { array, scoreBuffer } from './helpers'

type ScanOptions = {
  cursor: bigint
  match?: Buffer
  count?: number
  type?: string
  noValues?: boolean
}

/**
 * What the keyed scans (HSCAN / SSCAN / ZSCAN) parse up front: the key and the
 * cursor. The options stay raw until the key has been looked up, because real
 * Redis parses them only then (`scanGenericCommand` runs after
 * `lookupKeyReadOrReply` and `checkType`): a missing key answers the empty scan
 * reply whatever follows the cursor, and a key of the wrong type answers
 * WRONGTYPE before any option error.
 */
type KeyedScanArgs = {
  key: Buffer
  cursor: bigint
  options: readonly Buffer[]
}

type ScanResultOptions = {
  cursor: bigint
  match?: Buffer
  count?: number
  type?: string
}

type ScanItem = {
  matchValue: Buffer
  values: RedisValue[]
  type?: RedisDataTypeName
}

export const keysCommand = defineCommand({
  name: 'keys',
  schema: t.object({
    pattern: t.bulk(),
  }),
  flags: ['readonly'],
  keys: () => [],
  execute: (args, ctx) =>
    array(
      ctx.db
        .entriesSnapshot()
        .filter(entry => matchesPattern(entry.key, args.pattern))
        .map(entry => RedisValue.bulkString(entry.key)),
    ),
})

export const scanCommand = defineCommand({
  name: 'scan',
  schema: createScanOptionsSchema(),
  flags: ['readonly', 'random'],
  keys: () => [],
  execute: (args, ctx) => {
    const items = ctx.db.entriesSnapshot().map(entry => ({
      matchValue: entry.key,
      values: [RedisValue.bulkString(entry.key)],
      type: entry.value.type,
    }))

    return scanResult(items, args)
  },
})

export const hscanCommand = defineCommand({
  name: 'hscan',
  schema: createKeyedScanSchema(),
  flags: ['readonly', 'random'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    if (!hash) {
      return emptyScanResult()
    }

    const options = parseScanOptions(
      args.options,
      0,
      'hscan',
      ctx.server.profile,
    )
    const entries = ctx.db.updateHash(args.key, hash =>
      Array.from(hash.entries()),
    )
    const items: ScanItem[] = entries.map(({ field, value }) => ({
      matchValue: field,
      values: options.noValues
        ? [RedisValue.bulkString(field)]
        : [RedisValue.bulkString(field), RedisValue.bulkString(value)],
    }))

    return scanResult(items, { cursor: args.cursor, ...options })
  },
})

export const sscanCommand = defineCommand({
  name: 'sscan',
  schema: createKeyedScanSchema(),
  flags: ['readonly', 'random'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const set = ctx.db.getSet(args.key)
    if (!set) {
      return emptyScanResult()
    }

    const options = parseScanOptions(
      args.options,
      0,
      'sscan',
      ctx.server.profile,
    )
    const items: ScanItem[] = []
    for (const member of set.members.values()) {
      items.push({
        matchValue: member,
        values: [RedisValue.bulkString(member)],
      })
    }

    return scanResult(items, { cursor: args.cursor, ...options })
  },
})

export const zscanCommand = defineCommand({
  name: 'zscan',
  schema: createKeyedScanSchema(),
  flags: ['readonly', 'random'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) {
      return emptyScanResult()
    }

    const options = parseScanOptions(
      args.options,
      0,
      'zscan',
      ctx.server.profile,
    )
    const items: ScanItem[] = []
    for (const entry of zset.members.values()) {
      items.push({
        matchValue: entry.member,
        values: [
          RedisValue.bulkString(entry.member),
          RedisValue.bulkString(scoreBuffer(entry.score, ctx.server.profile)),
        ],
      })
    }

    return scanResult(items, { cursor: args.cursor, ...options })
  },
})

export const scanCommands = [
  keysCommand,
  scanCommand,
  hscanCommand,
  sscanCommand,
  zscanCommand,
]

function createScanOptionsSchema() {
  return t.custom<ScanOptions>({ min: 1 }, (input, index, ctx) => {
    const cursor = parseCursor(readRequired(input, index, ctx.commandName))
    const options = parseScanOptions(input, index + 1, 'scan', ctx.profile)

    return {
      value: { cursor, ...options },
      nextIndex: input.length,
    }
  })
}

function createKeyedScanSchema() {
  const layout = { min: 2, keys: [0] }
  return t.custom<KeyedScanArgs>(layout, (input, index, ctx) => {
    const key = readRequired(input, index, ctx.commandName)
    // The cursor, unlike the options, is checked before the key is looked up
    // (`parseScanCursorOrReply` comes first in h/s/zscanCommand), so
    // `HSCAN missing abc` is `invalid cursor` rather than an empty scan.
    const cursor = parseCursor(readRequired(input, index + 1, ctx.commandName))

    return {
      value: { key, cursor, options: input.slice(index + 2) },
      nextIndex: input.length,
    }
  })
}

type ScanCommandName = 'scan' | 'hscan' | 'sscan' | 'zscan'

/**
 * The option loop of Redis's `scanGenericCommand`, left to right, stopping at
 * the first bad option. An option whose value is missing (`MATCH` as the last
 * argument) is not an arity error there: it falls through to `syntax error`
 * like any unknown word.
 */
function parseScanOptions(
  input: readonly Buffer[],
  index: number,
  command: ScanCommandName,
  profile: CompatibilityProfile,
): Omit<ScanOptions, 'cursor'> {
  const options: Omit<ScanOptions, 'cursor'> = {}
  let cursor = index

  while (cursor < input.length) {
    const option = input[cursor].toString().toLowerCase()
    const hasValue = cursor + 1 < input.length

    if (option === 'match' && hasValue) {
      options.match = input[cursor + 1]
      cursor += 2
      continue
    }

    if (option === 'count' && hasValue) {
      options.count = parseCount(input[cursor + 1])
      cursor += 2
      continue
    }

    if (option === 'type' && command === 'scan' && hasValue) {
      options.type = input[cursor + 1].toString().toLowerCase()
      cursor += 2
      continue
    }

    // Before Redis 7.4 / Valkey 8.0 NOVALUES is just an unknown option.
    if (option === 'novalues' && profile.has('hscan.novalues')) {
      if (command !== 'hscan') {
        throw new RedisCommandError('NOVALUES option can only be used in HSCAN')
      }
      options.noValues = true
      cursor++
      continue
    }

    throw errors.syntax()
  }

  return options
}

function readRequired(
  input: readonly Buffer[],
  index: number,
  commandName: string,
): Buffer {
  const value = input[index]
  if (!value) {
    throw new RedisCommandError(
      `wrong number of arguments for '${commandName}' command`,
    )
  }

  return value
}

function parseCursor(raw: Buffer): bigint {
  const value = raw.toString()
  // Redis parses the cursor as an unsigned 64-bit integer (strict_strtoull):
  // a leading sign or a value past UINT64_MAX is rejected as `invalid cursor`.
  if (!/^\d+$/.test(value)) {
    throw errors.invalidCursor()
  }

  const parsed = BigInt(value)
  if (parsed > 0xffffffffffffffffn) {
    throw errors.invalidCursor()
  }

  return parsed
}

function parseCount(raw: Buffer): number {
  const value = raw.toString()
  if (!/^-?\d+$/.test(value)) {
    throw errors.expectedInteger()
  }

  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed)) {
    throw errors.expectedInteger()
  }

  if (parsed <= 0) {
    throw errors.syntax()
  }

  return parsed
}

function matchesPattern(value: Buffer, pattern?: Buffer): boolean {
  if (pattern === undefined) {
    return true
  }

  return redisGlobMatch(pattern, value)
}

/** `shared.emptyscan`: the reply for a keyed scan whose key does not exist. */
function emptyScanResult(): RedisResult {
  return scanResult([], { cursor: 0n })
}

function scanResult(
  items: ScanItem[],
  options: ScanResultOptions,
): RedisResult {
  const itemCount = items.length
  const startItem = normalizeScanCursor(options.cursor, itemCount)
  const pageSize = options.count ?? DEFAULT_SCAN_COUNT
  const endItem = Math.min(itemCount, startItem + pageSize)
  const nextCursor = endItem >= itemCount ? 0 : endItem
  const page = items
    .slice(startItem, endItem)
    .filter(item => matchesScanItem(item, options))
    .flatMap(item => item.values)

  return RedisResult.create(
    RedisValue.array([
      RedisValue.bulkString(Buffer.from(nextCursor.toString())),
      RedisValue.array(page),
    ]),
  )
}

function matchesScanItem(item: ScanItem, options: ScanResultOptions): boolean {
  if (!matchesPattern(item.matchValue, options.match)) {
    return false
  }

  if (options.type !== undefined && item.type !== options.type) {
    return false
  }

  return true
}

function normalizeScanCursor(cursor: bigint, itemCount: number): number {
  if (cursor <= 0n) {
    return 0
  }

  if (cursor > BigInt(Number.MAX_SAFE_INTEGER)) {
    return itemCount
  }

  return Math.min(Number(cursor), itemCount)
}

const DEFAULT_SCAN_COUNT = 10
