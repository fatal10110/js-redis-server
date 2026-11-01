import { asciiUpperCase } from '../core/ascii-case'
import {
  defineCommand,
  type CommandDocumentation,
  type CommandDocumentationArgument,
  type KeyWithFlags,
} from '../core/command-definition'
import { t } from '../core/command-schema'
import { errors } from '../core/redis-error'
import type { RedisExecutionContext } from '../core/redis-context'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { RedisDatabase } from '../state'
import { parseInt64Token } from './helpers'
import { commandKeyArgument } from './introspection'
import { MAX_MATERIALISABLE_LENGTH } from './strings'

/**
 * LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN] (Redis 7.0+,
 * Valkey 7.2+) and its Redis 6.2 predecessor
 * STRALGO LCS [IDX] [LEN] [MINMATCHLEN len] [WITHMATCHLEN]
 * STRINGS a b | KEYS k1 k2, which 7.0 removed. Both follow t_string.c
 * (`lcsCommand` at redis 7.2.4, `stralgoLCS` at 6.2.14) step for step: the
 * same dynamic-programming table, the same traceback from the end of both
 * strings and the same tie rule, so IDX lists the very ranges real Redis
 * lists, in its order (last range first).
 */

type LcsOptions = {
  getLen: boolean
  getIdx: boolean
  withMatchLen: boolean
  minMatchLen: bigint
}

function newOptions(): LcsOptions {
  return { getLen: false, getIdx: false, withMatchLen: false, minMatchLen: 0n }
}

/**
 * The option at `argv[j]` shared by both commands, applied to `options`:
 * the index of the last token it consumed, or -1 when `argv[j]` is not one
 * of them. MINMATCHLEN with no value after it is not an option (the caller's
 * syntax error), and a negative length means no minimum.
 */
function applyCommonOption(
  argv: readonly Buffer[],
  j: number,
  options: LcsOptions,
): number {
  const option = asciiUpperCase(argv[j].toString('latin1'))
  const moreArgs = argv.length - 1 - j
  if (option === 'IDX') {
    options.getIdx = true
  } else if (option === 'LEN') {
    options.getLen = true
  } else if (option === 'WITHMATCHLEN') {
    options.withMatchLen = true
  } else if (option === 'MINMATCHLEN' && moreArgs > 0) {
    const value = parseInt64Token(argv[j + 1])
    options.minMatchLen = value < 0n ? 0n : value
    return j + 1
  } else {
    return -1
  }
  return j
}

/**
 * The strings an LCS key names: its value, or empty when the key is missing.
 * Either key holding another type fails the whole command, with LCS's own
 * message rather than WRONGTYPE.
 */
function lookupLcsStrings(
  db: RedisDatabase,
  key1: Buffer,
  key2: Buffer,
): [Buffer, Buffer] {
  const type1 = db.getType(key1)
  const type2 = db.getType(key2)
  if (
    (type1 !== null && type1 !== 'string') ||
    (type2 !== null && type2 !== 'string')
  ) {
    throw errors.lcsKeysNotStrings()
  }
  return [
    (type1 && db.getString(key1)) || Buffer.alloc(0),
    (type2 && db.getString(key2)) || Buffer.alloc(0),
  ]
}

/** How the table size is checked: LCS (7.0+) or STRALGO (6.2). */
type SizeRule = 'lcs' | 'stralgo'

/**
 * The (alen+1) x (blen+1) table, refused the way the server refuses it. LCS
 * answers its proto-max-bulk-len error past that limit; STRALGO (6.2) has no
 * such check and only fails when the allocation does. The mock never
 * allocates past {@link MAX_MATERIALISABLE_LENGTH} (512MB, the default
 * proto-max-bulk-len), so above it, reachable only with a raised limit or
 * on 6.2, it answers the allocation-failure error instead.
 */
function allocateTable(
  alen: number,
  blen: number,
  ctx: RedisExecutionContext,
  rule: SizeRule,
): Uint32Array {
  const bytes = BigInt(alen + 1) * BigInt(blen + 1) * 4n
  if (rule === 'lcs' && bytes > ctx.server.protoMaxBulkLen) {
    throw errors.lcsExceedsProtoMaxBulkLen()
  }
  const failed = () =>
    rule === 'lcs' ? errors.lcsAllocationFailed() : errors.lcsOutOfMemory()
  if (bytes > MAX_MATERIALISABLE_LENGTH) {
    throw failed()
  }
  try {
    return new Uint32Array((alen + 1) * (blen + 1))
  } catch {
    throw failed()
  }
}

function computeLcs(
  a: Buffer,
  b: Buffer,
  options: LcsOptions,
  ctx: RedisExecutionContext,
  rule: SizeRule,
): RedisResult {
  const alen = a.length
  const blen = b.length
  const width = blen + 1
  const lcs = allocateTable(alen, blen, ctx, rule)

  for (let i = 1; i <= alen; i++) {
    const row = i * width
    const above = row - width
    const ai = a[i - 1]
    for (let j = 1; j <= blen; j++) {
      if (ai === b[j - 1]) {
        lcs[row + j] = lcs[above + j - 1] + 1
      } else {
        const lcs1 = lcs[above + j]
        const lcs2 = lcs[row + j - 1]
        lcs[row + j] = lcs1 > lcs2 ? lcs1 : lcs2
      }
    }
  }

  const total = lcs[alen * width + blen]
  if (options.getLen && !options.getIdx) {
    return RedisResult.create(RedisValue.integer(total))
  }

  const result = Buffer.allocUnsafe(total)
  const matches: RedisValue[] = []
  let idx = total
  // `alen` in arangeStart means no range is open.
  let arangeStart = alen
  let arangeEnd = 0
  let brangeStart = 0
  let brangeEnd = 0
  let i = alen
  let j = blen
  while (i > 0 && j > 0) {
    let emitRange = false
    if (a[i - 1] === b[j - 1]) {
      result[idx - 1] = a[i - 1]
      if (arangeStart === alen) {
        arangeStart = i - 1
        arangeEnd = i - 1
        brangeStart = j - 1
        brangeEnd = j - 1
      } else if (arangeStart === i && brangeStart === j) {
        arangeStart--
        brangeStart--
      } else {
        emitRange = true
      }
      if (arangeStart === 0 || brangeStart === 0) emitRange = true
      idx--
      i--
      j--
    } else {
      const lcs1 = lcs[(i - 1) * width + j]
      const lcs2 = lcs[i * width + j - 1]
      if (lcs1 > lcs2) i--
      else j--
      if (arangeStart !== alen) emitRange = true
    }

    if (emitRange) {
      const matchLen = arangeEnd - arangeStart + 1
      if (
        options.getIdx &&
        (options.minMatchLen === 0n || BigInt(matchLen) >= options.minMatchLen)
      ) {
        const entry = [
          RedisValue.array([
            RedisValue.integer(arangeStart),
            RedisValue.integer(arangeEnd),
          ]),
          RedisValue.array([
            RedisValue.integer(brangeStart),
            RedisValue.integer(brangeEnd),
          ]),
        ]
        if (options.withMatchLen) entry.push(RedisValue.integer(matchLen))
        matches.push(RedisValue.array(entry))
      }
      arangeStart = alen
    }
  }

  if (options.getIdx) {
    return RedisResult.create(
      RedisValue.map([
        [
          RedisValue.bulkString(Buffer.from('matches')),
          RedisValue.array(matches),
        ],
        [RedisValue.bulkString(Buffer.from('len')), RedisValue.integer(total)],
      ]),
    )
  }
  return RedisResult.create(RedisValue.bulkString(result))
}

type LcsArgs = {
  key1: Buffer
  key2: Buffer
  options: readonly Buffer[]
}

const LCS_COMPLEXITY =
  'O(N*M) where N and M are the lengths of s1 and s2, respectively'

/**
 * COMMAND DOCS lcs: Redis 7.2 rewrote the summary, renamed the MINMATCHLEN
 * argument from `len` to `min-match-len` and added `display_text` to every
 * argument (Valkey has the 7.2 form).
 */
function lcsDocs(from72: boolean): CommandDocumentation {
  const optional = ['optional']
  const minMatchLenName = from72 ? 'min-match-len' : 'len'
  const args: CommandDocumentationArgument[] = [
    commandKeyArgument('key1', 0),
    commandKeyArgument('key2', 0),
    { name: 'len', type: 'pure-token', token: 'LEN', flags: optional },
    { name: 'idx', type: 'pure-token', token: 'IDX', flags: optional },
    {
      name: minMatchLenName,
      type: 'integer',
      token: 'MINMATCHLEN',
      flags: optional,
    },
    {
      name: 'withmatchlen',
      type: 'pure-token',
      token: 'WITHMATCHLEN',
      flags: optional,
    },
  ]
  return {
    summary: from72
      ? 'Finds the longest common substring.'
      : 'Find longest common substring',
    since: '7.0.0',
    group: 'string',
    complexity: LCS_COMPLEXITY,
    arguments: from72
      ? args.map(arg => ({ ...arg, displayText: arg.name }))
      : args,
  }
}

export const lcsCommand = defineCommand({
  name: 'lcs',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  // Only collects the tokens: the keys are looked up and type-checked
  // before any option is read, so a wrong-type key beats an option error.
  schema: t.custom<LcsArgs>({ min: 2, keys: [0, 1] }, (input, index) => ({
    value: {
      key1: input[index],
      key2: input[index + 1],
      options: input.slice(index + 2),
    },
    nextIndex: input.length,
  })),
  flags: ['readonly'],
  introspection: {
    docs: lcsDocs(true),
    forProfile: profile =>
      profile.has('docs.summary-7.2-wording')
        ? undefined
        : { docs: lcsDocs(false) },
  },
  keys: args => [args.key1, args.key2],
  execute: (args, ctx) => {
    const [a, b] = lookupLcsStrings(ctx.db, args.key1, args.key2)
    const options = newOptions()
    for (let j = 0; j < args.options.length; j++) {
      const consumed = applyCommonOption(args.options, j, options)
      if (consumed === -1) throw errors.syntax()
      j = consumed
    }
    if (options.getLen && options.getIdx) {
      throw errors.lcsLenWithIdx()
    }
    return computeLcs(a, b, options, ctx, 'lcs')
  },
})

/**
 * Redis 6.2's `lcsGetKeys`: the two keys after the first `KEYS` that has two
 * arguments after it, unless `STRINGS` comes first (then none).
 */
export function stralgoGetKeys(argv: readonly Buffer[]): KeyWithFlags[] {
  for (let i = 1; i < argv.length; i++) {
    const arg = asciiUpperCase(argv[i].toString('latin1'))
    if (arg === 'STRINGS') {
      break
    }
    if (arg === 'KEYS' && argv.length - 1 - i >= 2) {
      return [
        { key: argv[i + 1], flags: [] },
        { key: argv[i + 2], flags: [] },
      ]
    }
  }
  return []
}

type StralgoArgs = { argv: readonly Buffer[] }

export const stralgoCommand = defineCommand({
  name: 'stralgo',
  until: { redis: '7.0.0', valkey: '7.2.0' },
  rawKeys: stralgoGetKeys,
  // The whole command is read as it runs: KEYS looks its keys up the moment
  // it is parsed, between the options before and after it.
  schema: t.custom<StralgoArgs>({ min: 1 }, (input, index) => ({
    value: { argv: input.slice(index) },
    nextIndex: input.length,
  })),
  flags: ['readonly'],
  keys: args =>
    stralgoGetKeys([Buffer.from('stralgo'), ...args.argv]).map(
      ({ key }) => key,
    ),
  execute: (args, ctx) => {
    const { argv } = args
    if (asciiUpperCase(argv[0].toString('latin1')) !== 'LCS') {
      throw errors.syntax()
    }

    const options = newOptions()
    let strings: [Buffer, Buffer] | undefined
    for (let j = 1; j < argv.length; j++) {
      const consumed = applyCommonOption(argv, j, options)
      if (consumed !== -1) {
        j = consumed
        continue
      }
      const option = asciiUpperCase(argv[j].toString('latin1'))
      const moreArgs = argv.length - 1 - j
      if ((option === 'STRINGS' || option === 'KEYS') && moreArgs > 1) {
        if (strings) throw errors.stralgoStringsOrKeys()
        strings =
          option === 'STRINGS'
            ? [argv[j + 1], argv[j + 2]]
            : lookupLcsStrings(ctx.db, argv[j + 1], argv[j + 2])
        j += 2
      } else {
        throw errors.syntax()
      }
    }

    if (!strings) throw errors.stralgoNoStrings()
    if (options.getLen && options.getIdx) throw errors.lcsLenWithIdx()
    return computeLcs(strings[0], strings[1], options, ctx, 'stralgo')
  },
})

export const lcsCommands = [lcsCommand, stralgoCommand]
