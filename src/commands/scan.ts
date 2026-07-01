import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import {
  ExpectedIntegerError,
  RedisCommandError,
  RedisSyntaxError,
} from '../core/redis-error'
import { redisGlobMatch } from '../core/glob'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { RedisDataTypeName } from '../state'
import { array, scoreBuffer } from './helpers'

type ScanOptions = {
  cursor: bigint
  match?: Buffer
  count?: number
  type?: string
  noValues?: boolean
}

type KeyedScanOptions = {
  key: Buffer
  cursor: bigint
  match?: Buffer
  count?: number
  noValues?: boolean
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
  schema: createScanOptionsSchema(true),
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
  schema: createKeyedScanOptionsSchema(),
  flags: ['readonly', 'random'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (args.noValues && !ctx.server.profile.has('hscan.novalues')) {
      throw new RedisSyntaxError()
    }

    const hash = ctx.db.getHash(args.key)
    if (!hash) {
      return scanResult([], args)
    }

    const entries = ctx.db.updateHash(args.key, hash =>
      Array.from(hash.entries()),
    )
    const items: ScanItem[] = entries.map(({ field, value }) => ({
      matchValue: field,
      values: args.noValues
        ? [RedisValue.bulkString(field)]
        : [RedisValue.bulkString(field), RedisValue.bulkString(value)],
    }))

    return scanResult(items, args)
  },
})

export const sscanCommand = defineCommand({
  name: 'sscan',
  schema: createKeyedScanOptionsSchema(),
  flags: ['readonly', 'random'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const set = ctx.db.getSet(args.key)
    if (!set) {
      return scanResult([], args)
    }

    const items: ScanItem[] = []
    for (const member of set.members.values()) {
      items.push({
        matchValue: member,
        values: [RedisValue.bulkString(member)],
      })
    }

    return scanResult(items, args)
  },
})

export const zscanCommand = defineCommand({
  name: 'zscan',
  schema: createKeyedScanOptionsSchema(),
  flags: ['readonly', 'random'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    if (!zset) {
      return scanResult([], args)
    }

    const items: ScanItem[] = []
    for (const entry of zset.members.values()) {
      items.push({
        matchValue: entry.member,
        values: [
          RedisValue.bulkString(entry.member),
          RedisValue.bulkString(scoreBuffer(entry.score)),
        ],
      })
    }

    return scanResult(items, args)
  },
})

export const scanCommands = [
  keysCommand,
  scanCommand,
  hscanCommand,
  sscanCommand,
  zscanCommand,
]

function createScanOptionsSchema(allowType: boolean) {
  return t.custom<ScanOptions>((input, index, ctx) => {
    const cursor = parseCursor(readRequired(input, index, ctx.commandName))
    const options = parseScanOptions(
      input,
      index + 1,
      ctx.commandName,
      allowType,
    )

    return {
      value: { cursor, ...options },
      nextIndex: input.length,
    }
  })
}

function createKeyedScanOptionsSchema() {
  return t.custom<KeyedScanOptions>((input, index, ctx) => {
    const key = readRequired(input, index, ctx.commandName)
    const cursor = parseCursor(readRequired(input, index + 1, ctx.commandName))
    const options = parseScanOptions(
      input,
      index + 2,
      ctx.commandName,
      false,
      ctx.commandName === 'hscan',
    )

    return {
      value: { key, cursor, ...options },
      nextIndex: input.length,
    }
  })
}

function parseScanOptions(
  input: readonly Buffer[],
  index: number,
  commandName: string,
  allowType: boolean,
  allowNoValues = false,
): Omit<ScanOptions, 'cursor'> {
  const options: Omit<ScanOptions, 'cursor'> = {}
  let cursor = index

  while (cursor < input.length) {
    const option = input[cursor].toString().toLowerCase()

    if (option === 'match') {
      options.match = readOptionValue(input, cursor, commandName)
      cursor += 2
      continue
    }

    if (option === 'count') {
      options.count = parseCount(readOptionValue(input, cursor, commandName))
      cursor += 2
      continue
    }

    if (option === 'type' && allowType) {
      options.type = readOptionValue(input, cursor, commandName)
        .toString()
        .toLowerCase()
      cursor += 2
      continue
    }

    if (option === 'novalues') {
      if (!allowNoValues) {
        throw new RedisCommandError('NOVALUES option can only be used in HSCAN')
      }
      options.noValues = true
      cursor++
      continue
    }

    throw new RedisSyntaxError()
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

function readOptionValue(
  input: readonly Buffer[],
  optionIndex: number,
  commandName: string,
): Buffer {
  return readRequired(input, optionIndex + 1, commandName)
}

function parseCursor(raw: Buffer): bigint {
  const value = raw.toString()
  // Redis parses the cursor as an unsigned 64-bit integer (strict_strtoull):
  // a leading sign or a value past UINT64_MAX is rejected as `invalid cursor`.
  if (!/^\d+$/.test(value)) {
    throw new RedisCommandError('invalid cursor')
  }

  const parsed = BigInt(value)
  if (parsed > 0xffffffffffffffffn) {
    throw new RedisCommandError('invalid cursor')
  }

  return parsed
}

function parseCount(raw: Buffer): number {
  const value = raw.toString()
  if (!/^-?\d+$/.test(value)) {
    throw new ExpectedIntegerError()
  }

  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed)) {
    throw new ExpectedIntegerError()
  }

  if (parsed <= 0) {
    throw new RedisSyntaxError()
  }

  return parsed
}

function matchesPattern(value: Buffer, pattern?: Buffer): boolean {
  if (pattern === undefined) {
    return true
  }

  return redisGlobMatch(pattern, value)
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
