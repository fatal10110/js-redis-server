import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import {
  buildSearchReply,
  collectMatches,
  orderAndLimit,
  parseGeoBy,
  parseGeoFrom,
  resolveCenterFromMember,
  tryParseOrderOrCount,
  type GeoBy,
  type GeoFrom,
  type GeoOrderCount,
} from './search-core'

type GeoSearchArgs = GeoOrderCount & {
  key: Buffer
  from: GeoFrom
  by: GeoBy
  withCoord: boolean
  withDist: boolean
  withHash: boolean
}

function createGeoSearchSchema() {
  return t.custom<GeoSearchArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) throw new WrongNumberOfArgumentsError(ctx.commandName)

      let cursor = index + 1
      if (cursor >= input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const [from, afterFrom] = parseGeoFrom(input, cursor, ctx.commandName)
      cursor = afterFrom

      if (cursor >= input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const [by, afterBy] = parseGeoBy(input, cursor)
      cursor = afterBy

      const options: GeoOrderCount = {}
      let withCoord = false
      let withDist = false
      let withHash = false
      while (cursor < input.length) {
        const advanced = tryParseOrderOrCount(input, cursor, options)
        if (advanced !== null) {
          cursor = advanced
          continue
        }
        const token = input[cursor]!.toString().toUpperCase()
        if (token === 'WITHCOORD') {
          withCoord = true
          cursor++
          continue
        }
        if (token === 'WITHDIST') {
          withDist = true
          cursor++
          continue
        }
        if (token === 'WITHHASH') {
          withHash = true
          cursor++
          continue
        }
        throw new RedisSyntaxError()
      }

      return {
        value: { key, from, by, withCoord, withDist, withHash, ...options },
        nextIndex: input.length,
      }
    },
  )
}

export const geosearchCommand = defineCommand({
  name: 'geosearch',
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: createGeoSearchSchema(),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    const center =
      args.from.type === 'member'
        ? resolveCenterFromMember(zset, args.from.member)
        : { lon: args.from.lon, lat: args.from.lat }

    const matches = collectMatches(zset, center, args.by)
    const ordered = orderAndLimit(matches, {
      order: args.order,
      count: args.count,
    })
    return buildSearchReply(
      ordered,
      {
        withCoord: args.withCoord,
        withDist: args.withDist,
        withHash: args.withHash,
      },
      args.by.unit,
    )
  },
})
