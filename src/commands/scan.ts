import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import {
  ExpectedIntegerError,
  RedisCommandError,
  RedisSyntaxError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { RedisDataTypeName } from '../state'
import { array } from './helpers'

type ScanOptions = {
  cursor: bigint
  match?: Buffer
  count?: number
  type?: string
}

type KeyedScanOptions = {
  key: Buffer
  cursor: bigint
  match?: Buffer
  count?: number
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
    const items = ctx.db
      .entriesSnapshot()
      .filter(entry => matchesPattern(entry.key, args.match))
      .filter(entry => matchesType(entry.value.type, args.type))
      .map(entry => RedisValue.bulkString(entry.key))

    return scanResult(items, args, 1)
  },
})

export const hscanCommand = defineCommand({
  name: 'hscan',
  schema: createKeyedScanOptionsSchema(),
  flags: ['readonly', 'random'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const hash = ctx.db.getHash(args.key)
    if (!hash) {
      return scanResult([], args, 2)
    }

    const items: RedisValue[] = []
    for (const { field, value } of hash.fields.values()) {
      if (!matchesPattern(field, args.match)) {
        continue
      }

      items.push(RedisValue.bulkString(field))
      items.push(RedisValue.bulkString(value))
    }

    return scanResult(items, args, 2)
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
      return scanResult([], args, 1)
    }

    const items: RedisValue[] = []
    for (const member of set.members.values()) {
      if (matchesPattern(member, args.match)) {
        items.push(RedisValue.bulkString(member))
      }
    }

    return scanResult(items, args, 1)
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
      return scanResult([], args, 2)
    }

    const items: RedisValue[] = []
    for (const entry of zset.members.values()) {
      if (!matchesPattern(entry.member, args.match)) {
        continue
      }

      items.push(RedisValue.bulkString(entry.member))
      items.push(RedisValue.bulkString(Buffer.from(entry.score.toString())))
    }

    return scanResult(items, args, 2)
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
    const options = parseScanOptions(input, index + 2, ctx.commandName, false)

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
  if (!/^-?\d+$/.test(value)) {
    throw new RedisCommandError('invalid cursor')
  }

  return BigInt(value)
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

function matchesType(type: RedisDataTypeName, filter?: string): boolean {
  if (filter === undefined) {
    return true
  }

  return type === filter
}

function matchesPattern(value: Buffer, pattern?: Buffer): boolean {
  if (pattern === undefined) {
    return true
  }

  return redisGlobMatch(pattern, 0, value, 0)
}

function scanResult(
  items: RedisValue[],
  options: Pick<ScanOptions, 'cursor' | 'count'>,
  itemWidth: 1 | 2,
): RedisResult {
  const itemCount = Math.ceil(items.length / itemWidth)
  const startItem = normalizeScanCursor(options.cursor, itemCount)
  const pageSize = options.count ?? DEFAULT_SCAN_COUNT
  const endItem = Math.min(itemCount, startItem + pageSize)
  const nextCursor = endItem >= itemCount ? 0 : endItem

  return RedisResult.create(
    RedisValue.array([
      RedisValue.bulkString(Buffer.from(nextCursor.toString())),
      RedisValue.array(items.slice(startItem * itemWidth, endItem * itemWidth)),
    ]),
  )
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

function redisGlobMatch(
  pattern: Buffer,
  patternIndex: number,
  value: Buffer,
  valueIndex: number,
): boolean {
  let patternCursor = patternIndex
  let valueCursor = valueIndex

  while (patternCursor < pattern.length && valueCursor < value.length) {
    const token = pattern[patternCursor]

    if (token === STAR) {
      while (pattern[patternCursor + 1] === STAR) {
        patternCursor++
      }

      if (patternCursor + 1 === pattern.length) {
        return true
      }

      for (
        let nextValueCursor = valueCursor;
        nextValueCursor < value.length;
        nextValueCursor++
      ) {
        if (
          redisGlobMatch(pattern, patternCursor + 1, value, nextValueCursor)
        ) {
          return true
        }
      }

      return false
    }

    if (token === QUESTION_MARK) {
      patternCursor++
      valueCursor++
      continue
    }

    if (token === OPEN_BRACKET) {
      const characterClass = matchCharacterClass(
        pattern,
        patternCursor,
        value[valueCursor],
      )

      if (!characterClass.matches) {
        return false
      }

      patternCursor = characterClass.nextPatternIndex
      valueCursor++
      continue
    }

    if (token === BACKSLASH && patternCursor + 1 < pattern.length) {
      patternCursor++
    }

    if (pattern[patternCursor] !== value[valueCursor]) {
      return false
    }

    patternCursor++
    valueCursor++
  }

  while (pattern[patternCursor] === STAR) {
    patternCursor++
  }

  return patternCursor === pattern.length && valueCursor === value.length
}

function matchCharacterClass(
  pattern: Buffer,
  openBracketIndex: number,
  value: number,
): { matches: boolean; nextPatternIndex: number } {
  let patternCursor = openBracketIndex + 1
  let negated = false

  if (pattern[patternCursor] === CARET) {
    negated = true
    patternCursor++
  }

  let matches = false

  while (true) {
    if (pattern[patternCursor] === CLOSE_BRACKET) {
      break
    }

    if (patternCursor >= pattern.length) {
      patternCursor--
      break
    }

    if (
      pattern[patternCursor] === BACKSLASH &&
      patternCursor + 1 < pattern.length
    ) {
      patternCursor++
      if (pattern[patternCursor] === value) {
        matches = true
      }
    } else if (
      patternCursor + 2 < pattern.length &&
      pattern[patternCursor + 1] === DASH
    ) {
      let start = pattern[patternCursor]
      let end = pattern[patternCursor + 2]

      if (start > end) {
        const previousStart = start
        start = end
        end = previousStart
      }

      if (value >= start && value <= end) {
        matches = true
      }

      patternCursor += 2
    } else if (pattern[patternCursor] === value) {
      matches = true
    }

    patternCursor++
  }

  return {
    matches: negated ? !matches : matches,
    nextPatternIndex: patternCursor + 1,
  }
}

const BACKSLASH = '\\'.charCodeAt(0)
const CARET = '^'.charCodeAt(0)
const CLOSE_BRACKET = ']'.charCodeAt(0)
const DEFAULT_SCAN_COUNT = 10
const DASH = '-'.charCodeAt(0)
const OPEN_BRACKET = '['.charCodeAt(0)
const QUESTION_MARK = '?'.charCodeAt(0)
const STAR = '*'.charCodeAt(0)
