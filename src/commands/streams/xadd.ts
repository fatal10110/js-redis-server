import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  InvalidStreamIdError,
  StreamIdEqualOrSmallerError,
  StreamIdExhaustedError,
  StreamIdNotGreaterThanZeroError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type { StreamId } from '../../state/data-types'
import { bulk } from '../helpers'
import {
  compareStreamId,
  formatStreamId,
  MAX_UINT64,
  MIN_ID,
  parseUint64,
} from './ids'
import { applyTrim, createTrimSpecSchema } from './trim'

// XADD id: "*", "<ms>-*", "<ms>-<seq>", or "<ms>" (seq defaults to 0).
function resolveXaddId(spec: string, lastId: StreamId): StreamId {
  if (spec === '*') {
    return nextAutoId(lastId)
  }

  const dash = spec.indexOf('-')
  if (dash === -1) {
    const ms = parseUint64(spec)
    if (ms === null) throw new InvalidStreamIdError()
    return { ms, seq: 0n }
  }

  const ms = parseUint64(spec.slice(0, dash))
  if (ms === null) throw new InvalidStreamIdError()

  const seqPart = spec.slice(dash + 1)
  if (seqPart === '*') {
    return nextSeqForMs(ms, lastId)
  }

  const seq = parseUint64(seqPart)
  if (seq === null) throw new InvalidStreamIdError()
  return { ms, seq }
}

function nextAutoId(lastId: StreamId): StreamId {
  const now = BigInt(Date.now())
  if (now > lastId.ms) return { ms: now, seq: 0n }
  if (lastId.seq < MAX_UINT64) return { ms: lastId.ms, seq: lastId.seq + 1n }
  if (lastId.ms < MAX_UINT64) return { ms: lastId.ms + 1n, seq: 0n }

  throw new StreamIdExhaustedError()
}

function nextSeqForMs(ms: bigint, lastId: StreamId): StreamId {
  if (ms > lastId.ms) return { ms, seq: 0n }
  if (ms < lastId.ms) {
    return { ms, seq: lastId.seq === MAX_UINT64 ? MAX_UINT64 : lastId.seq + 1n }
  }
  if (lastId.seq === MAX_UINT64) return { ms, seq: MAX_UINT64 }

  return { ms, seq: lastId.seq + 1n }
}

type FieldList = Buffer[]

// Parses the trailing `field value [field value ...]` of XADD into a flat
// [field1, value1, ...] array, requiring at least one complete pair.
function createStreamFieldsSchema() {
  return t.custom<FieldList>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const fields: FieldList = []
      let cursor = index
      while (cursor < input.length) {
        const field = input[cursor]
        const value = input[cursor + 1]
        if (value === undefined) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        fields.push(field, value)
        cursor += 2
      }
      if (fields.length === 0) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      return { value: fields, nextIndex: cursor }
    },
  )
}

export const xaddCommand = defineCommand({
  name: 'xadd',
  schema: t.object({
    key: t.key(),
    nomkstream: t.optional(t.keyword('NOMKSTREAM')),
    trim: t.optional(createTrimSpecSchema()),
    id: t.string(),
    fields: createStreamFieldsSchema(),
  }),
  flags: ['write', 'denyoom', 'fast'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    if (args.nomkstream !== undefined && ctx.db.getType(args.key) === null) {
      return bulk(null)
    }

    const id = ctx.db.updateStream(args.key, stream => {
      const next = resolveXaddId(args.id, stream.lastId)

      // An id must be > 0-0 and strictly greater than the stream's last id.
      // lastId is 0-0 for a brand-new stream and is retained even after XDEL
      // empties the stream, so this single comparison covers every case.
      if (compareStreamId(next, MIN_ID) <= 0) {
        throw new StreamIdNotGreaterThanZeroError()
      }
      if (compareStreamId(next, stream.lastId) <= 0) {
        throw new StreamIdEqualOrSmallerError()
      }

      stream.appendEntry(next, args.fields)

      if (args.trim !== undefined) {
        const trim = args.trim
        stream.trim(value => applyTrim(value, trim))
      }

      return next
    })

    return bulk(Buffer.from(formatStreamId(id)))
  },
})
