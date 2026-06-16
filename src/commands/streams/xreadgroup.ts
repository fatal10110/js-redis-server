import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type { StreamId } from '../../state/data-types'
import { bulk } from '../helpers'
import {
  ensureConsumer,
  pendingEntriesSorted,
  requireStreamGroup,
  findEntry,
} from './groups'
import {
  bufferId,
  cloneStreamId,
  compareStreamId,
  parseExactId,
  parseNonNegativeInteger,
  streamIdKey,
} from './ids'
import { bulkString, deletedEntryToReply, entryToReply } from './replies'

type XreadGroupStream = { key: Buffer; id: StreamId | '>' }

function createXreadGroupSchema() {
  return t.custom<{
    group: Buffer
    consumer: Buffer
    count: number | null
    blockMs: number | null
    noack: boolean
    streams: XreadGroupStream[]
  }>((input: readonly Buffer[], index: number, ctx: ParseContext) => {
    if (input[index]?.toString().toUpperCase() !== 'GROUP') {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const group = input[index + 1]
    const consumer = input[index + 2]
    if (!group || !consumer) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    let cursor = index + 3
    let count: number | null = null
    let blockMs: number | null = null
    let noack = false

    while (cursor < input.length) {
      const token = input[cursor].toString().toUpperCase()
      if (token === 'COUNT') {
        const raw = input[cursor + 1]
        if (!raw) throw new WrongNumberOfArgumentsError(ctx.commandName)
        count = parseNonNegativeInteger(raw)
        cursor += 2
        continue
      }

      if (token === 'BLOCK') {
        const raw = input[cursor + 1]
        if (!raw) throw new WrongNumberOfArgumentsError(ctx.commandName)
        blockMs = parseNonNegativeInteger(raw)
        cursor += 2
        continue
      }

      if (token === 'NOACK') {
        noack = true
        cursor++
        continue
      }

      break
    }

    if (
      cursor >= input.length ||
      input[cursor].toString().toUpperCase() !== 'STREAMS'
    ) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }
    cursor++

    const remaining = input.length - cursor
    if (remaining === 0 || remaining % 2 !== 0) {
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    }

    const half = remaining / 2
    const streams: XreadGroupStream[] = []
    for (let i = 0; i < half; i++) {
      const key = input[cursor + i]
      const rawId = input[cursor + half + i].toString()
      streams.push({
        key,
        id: rawId === '>' ? '>' : parseExactId(rawId),
      })
    }

    return {
      value: { group, consumer, count, blockMs, noack, streams },
      nextIndex: input.length,
    }
  })
}

function readGroupEntries(
  groupName: Buffer,
  consumerName: Buffer,
  streams: XreadGroupStream[],
  count: number | null,
  noack: boolean,
  ctx: RedisExecutionContext,
): RedisResult | null {
  const now = Date.now()
  const results: [RedisValue, RedisValue][] = []

  for (const { key } of streams) {
    requireStreamGroup(ctx.db.getStream(key), key, groupName, 'XREADGROUP')
  }

  for (const { key, id } of streams) {
    const entries = ctx.db.updateStream(key, stream => {
      const group = requireStreamGroup(stream, key, groupName, 'XREADGROUP')
      const consumer = ensureConsumer(group, consumerName, now)
      consumer.activeAt = now

      const consumerId = bufferId(consumerName)
      const replies: RedisValue[] = []

      if (id === '>') {
        for (const entry of stream.entries) {
          if (compareStreamId(entry.id, group.lastDeliveredId) <= 0) continue

          replies.push(entryToReply(entry.id, entry.fields))
          group.lastDeliveredId = cloneStreamId(entry.id)
          group.entriesRead = (group.entriesRead ?? 0) + 1

          if (!noack) {
            group.pending.set(streamIdKey(entry.id), {
              id: cloneStreamId(entry.id),
              consumerId,
              deliveredAt: now,
              deliveryCount: 1,
            })
          }

          if (count !== null && count > 0 && replies.length >= count) break
        }
      } else {
        for (const pending of pendingEntriesSorted(group)) {
          if (pending.consumerId !== consumerId) continue
          if (compareStreamId(pending.id, id) <= 0) continue

          const entry = findEntry(stream, pending.id)
          replies.push(
            entry
              ? entryToReply(entry.id, entry.fields)
              : deletedEntryToReply(pending.id),
          )

          if (count !== null && count > 0 && replies.length >= count) break
        }
      }

      return { result: replies, changed: true }
    })

    if (entries.length > 0 || id !== '>') {
      results.push([bulkString(key), RedisValue.array(entries)])
    }
  }

  return results.length > 0
    ? RedisResult.create(RedisValue.mapPairs(results))
    : null
}

async function blockingXreadGroup(
  groupName: Buffer,
  consumerName: Buffer,
  streams: XreadGroupStream[],
  count: number | null,
  noack: boolean,
  blockMs: number,
  ctx: RedisExecutionContext,
): Promise<RedisResult> {
  const timeoutMs = blockMs === 0 ? undefined : blockMs
  const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : undefined
  const keys = streams.map(s => s.key)

  while (true) {
    const remaining =
      deadline !== undefined ? Math.max(0, deadline - Date.now()) : undefined
    if (remaining === 0) return bulk(null)

    let wake!: (v: true) => void
    const waitFor = new Promise<true>(resolve => {
      wake = () => resolve(true)
    })

    const unsubs = keys.map(key =>
      ctx.db.subscribeKey(key, event => {
        if (event.type === 'write') wake(true)
      }),
    )

    let woken: boolean | null
    try {
      woken = await ctx.park({
        waitFor,
        timeoutMs: remaining,
        signal: ctx.signal,
      })
    } finally {
      for (const unsub of unsubs) {
        try {
          unsub()
        } catch {
          // ignore errors from individual unsubscribers so all are attempted
        }
      }
    }

    if (woken === null) return bulk(null)

    const result = readGroupEntries(
      groupName,
      consumerName,
      streams,
      count,
      noack,
      ctx,
    )
    if (result) return result
  }
}

export const xreadgroupCommand = defineCommand({
  name: 'xreadgroup',
  schema: t.object({ args: createXreadGroupSchema() }),
  flags: ['write', 'blocking'],
  capabilities: { blocking: true },
  keys: args => args.args.streams.map(s => s.key),
  execute: (args, ctx) => {
    const { group, consumer, streams, count, blockMs, noack } = args.args
    const immediate = readGroupEntries(
      group,
      consumer,
      streams,
      count,
      noack,
      ctx,
    )
    if (
      immediate ||
      blockMs === null ||
      streams.some(stream => stream.id !== '>')
    ) {
      return immediate ?? bulk(null)
    }

    return blockingXreadGroup(
      group,
      consumer,
      streams,
      count,
      noack,
      blockMs,
      ctx,
    )
  },
})
