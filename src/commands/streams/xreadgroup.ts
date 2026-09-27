import { defineCommand } from '../../core/command-definition'
import { xreadGetKeys } from '../../core/key-specs'
import { t, type ParseContext } from '../../core/command-schema'
import type { RedisExecutionContext } from '../../core/redis-context'
import { RedisResult } from '../../core/redis-result'
import { RedisValue } from '../../core/redis-value'
import type { StreamId } from '../../state/data-types'
import { bulk } from '../helpers'
import { createConsumerIfMissing, requireStreamGroup } from './groups'
import { compareStreamId, parseExactId } from './ids'
import { bulkString, deletedEntryToReply, entryToReply } from './replies'
import { blockOnKeys } from '../blocking'
import { parseXreadOptions } from './xread-options'

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
    const options = parseXreadOptions(input, index, ctx, true)
    const { count, blockMs, noack } = options
    // parseXreadOptions guarantees GROUP for XREADGROUP.
    const group = options.group as Buffer
    const consumer = options.consumer as Buffer
    const cursor = options.streamsStart
    const half = options.streamCount
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

  // 7.2+ creates the consumer before it looks for anything to deliver.
  // Through 7.0 a stream is only served (and the consumer only created or
  // refreshed) for a history read or when it holds an entry past the group's
  // last-delivered id, so an empty `>` read leaves a missing consumer missing
  // (#498).
  const eagerConsumer = ctx.server.profile.has('stream.consumer-active-time')
  for (const { key, id } of streams) {
    if (!eagerConsumer && id === '>' && !hasUndelivered(ctx, key, groupName)) {
      continue
    }
    createConsumerIfMissing(ctx.db, key, groupName, consumerName, now)
    const delivered = ctx.db.updateStream(key, stream => {
      const group = requireStreamGroup(
        stream.value,
        key,
        groupName,
        'XREADGROUP',
      )
      return stream.readGroup(group, consumerName, id, { count, noack }, now)
    })

    const entries = delivered.map(item =>
      item.fields === null
        ? deletedEntryToReply(item.id)
        : entryToReply(item.id, item.fields),
    )

    if (entries.length > 0 || id !== '>') {
      results.push([bulkString(key), RedisValue.array(entries)])
    }
  }

  return results.length > 0
    ? RedisResult.create(RedisValue.mapPairs(results))
    : null
}

function hasUndelivered(
  ctx: RedisExecutionContext,
  key: Buffer,
  groupName: Buffer,
): boolean {
  const stream = ctx.db.getStream(key)
  const group = requireStreamGroup(stream, key, groupName, 'XREADGROUP')
  const last = stream?.entries[stream.entries.length - 1]
  return (
    last !== undefined && compareStreamId(last.id, group.lastDeliveredId) > 0
  )
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
  const result = await blockOnKeys(ctx, {
    keys: streams.map(s => s.key),
    // No `type`: every change to the keys wakes XREADGROUP, as in real Redis.
    // A stream overwritten with another type unblocks it with WRONGTYPE; one
    // deleted (DEL, expiry, RENAME, FLUSHDB) or whose group is destroyed
    // unblocks it with NOGROUP.
    timeoutMs: blockMs === 0 ? undefined : blockMs,
    attempt: () =>
      readGroupEntries(groupName, consumerName, streams, count, noack, ctx),
  })
  return result ?? bulk(null)
}

export const xreadgroupCommand = defineCommand({
  name: 'xreadgroup',
  rawKeys: xreadGetKeys,
  schema: t.object({
    args: t.withLayout(createXreadGroupSchema(), { min: 6 }),
  }),
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
