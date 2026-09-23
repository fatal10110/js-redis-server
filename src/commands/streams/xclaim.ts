import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type { StreamId } from '../../state/data-types'
import { array } from '../helpers'
import { createConsumerIfMissing, requireStreamGroup } from './groups'
import { parseExactId, parseLongLong } from './ids'
import { entryToReply, streamIdValue } from './replies'

type XclaimArgs = {
  key: Buffer
  group: Buffer
  consumer: Buffer
  // min-idle-time, ids and options. Real Redis parses them only after the key
  // type and group check, so a bad argument never beats WRONGTYPE / NOGROUP —
  // see parseXclaimOptions, called from execute.
  rest: readonly Buffer[]
}

type XclaimOptions = {
  minIdleMs: number
  ids: StreamId[]
  deliveryTime: number | null
  retryCount: number | null
  force: boolean
  justId: boolean
  lastId: StreamId | null
}

function createXclaimSchema() {
  return t.custom<XclaimArgs>(
    { min: 5, keys: [0] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const group = input[index + 1]
      const consumer = input[index + 2]
      if (!key || !group || !consumer || input.length - index < 5) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      return {
        value: { key, group, consumer, rest: input.slice(index + 3) },
        nextIndex: input.length,
      }
    },
  )
}

function tryParseId(token: Buffer): StreamId | null {
  try {
    return parseExactId(token.toString())
  } catch {
    return null
  }
}

// Mirrors real Redis' xclaimCommand: min-idle-time, then ids up to the first
// token that is not one (possibly none), then options. An option missing its
// value, or any other token, is `Unrecognized XCLAIM option '<token>'`.
function parseXclaimOptions(
  rest: readonly Buffer[],
  now: number,
): XclaimOptions {
  const minIdle = parseLongLong(
    rest[0],
    'Invalid min-idle-time argument for XCLAIM',
  )

  let cursor = 1
  const ids: StreamId[] = []
  while (cursor < rest.length) {
    const id = tryParseId(rest[cursor])
    if (!id) break
    ids.push(id)
    cursor++
  }

  const options: XclaimOptions = {
    minIdleMs: minIdle < 0n ? 0 : Number(minIdle),
    ids,
    deliveryTime: null,
    retryCount: null,
    force: false,
    justId: false,
    lastId: null,
  }
  let deliveryTime: bigint | null = null
  for (; cursor < rest.length; cursor++) {
    const token = rest[cursor]
    const option = token.toString().toUpperCase()
    const value = cursor + 1 < rest.length ? rest[cursor + 1] : undefined
    if (option === 'FORCE') {
      options.force = true
    } else if (option === 'JUSTID') {
      options.justId = true
    } else if (option === 'IDLE' && value) {
      const idle = parseLongLong(
        value,
        'Invalid IDLE option argument for XCLAIM',
      )
      deliveryTime = BigInt(now) - idle
      cursor++
    } else if (option === 'TIME' && value) {
      deliveryTime = parseLongLong(
        value,
        'Invalid TIME option argument for XCLAIM',
      )
      cursor++
    } else if (option === 'RETRYCOUNT' && value) {
      const retryCount = parseLongLong(
        value,
        'Invalid RETRYCOUNT option argument for XCLAIM',
      )
      // A negative count means "not given", as in real Redis.
      options.retryCount = retryCount < 0n ? null : Number(retryCount)
      cursor++
    } else if (option === 'LASTID' && value) {
      options.lastId = parseExactId(value.toString())
      cursor++
    } else {
      throw new RedisCommandError(
        `Unrecognized XCLAIM option '${token.toString()}'`,
      )
    }
  }

  // A delivery time in the past or future is clamped to now, not an error.
  if (deliveryTime !== null) {
    options.deliveryTime =
      deliveryTime < 0n || deliveryTime > BigInt(now)
        ? now
        : Number(deliveryTime)
  }
  return options
}

export const xclaimCommand = defineCommand({
  name: 'xclaim',
  schema: t.object({ args: createXclaimSchema() }),
  flags: ['write'],
  keys: args => [args.args.key],
  execute: (args, ctx) => {
    const command = args.args
    const now = Date.now()
    requireStreamGroup(
      ctx.db.getStream(command.key),
      command.key,
      command.group,
    )
    const options = parseXclaimOptions(command.rest, now)
    createConsumerIfMissing(
      ctx.db,
      command.key,
      command.group,
      command.consumer,
      now,
    )
    const claimed = ctx.db.updateStream(command.key, stream => {
      const group = requireStreamGroup(stream.value, command.key, command.group)
      return stream.claim(
        group,
        command.consumer,
        options.ids,
        {
          minIdleMs: options.minIdleMs,
          idleMs: null,
          timeMs: options.deliveryTime,
          retryCount: options.retryCount,
          force: options.force,
          justId: options.justId,
          lastId: options.lastId,
        },
        now,
      )
    })

    const replies = claimed.map(entry =>
      options.justId
        ? streamIdValue(entry.id)
        : entryToReply(entry.id, entry.fields),
    )
    return array(replies)
  },
})
