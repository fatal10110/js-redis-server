import { asciiUpperCase } from '../../core/ascii-case'
import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
  errors,
} from '../../core/redis-error'
import { array } from '../helpers'
import {
  buildSearchReply,
  collectMatches,
  deferGeoParseError,
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

type GeoSearchParsed =
  GeoSearchArgs | { key: Buffer; failed: RedisCommandError; member?: Buffer }

function createGeoSearchSchema() {
  return t.custom<GeoSearchParsed>(
    { min: 6, keys: [0] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) throw new WrongNumberOfArgumentsError(ctx.commandName)

      let cursor = index + 1
      if (cursor >= input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      let member: Buffer | undefined
      try {
        const [from, afterFrom] = parseGeoFrom(
          input,
          cursor,
          ctx.commandName,
          ctx.profile,
        )
        cursor = afterFrom
        if (from.type === 'member') member = from.member

        if (cursor >= input.length) {
          throw new WrongNumberOfArgumentsError(ctx.commandName)
        }
        const [by, afterBy] = parseGeoBy(input, cursor, ctx.profile)
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
          const token = asciiUpperCase(input[cursor]!.toString())
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
          throw errors.syntax()
        }

        return {
          value: { key, from, by, withCoord, withDist, withHash, ...options },
          nextIndex: input.length,
        }
      } catch (err) {
        return {
          value: { key, failed: deferGeoParseError(err), member },
          nextIndex: input.length,
        }
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
    // A FROMMEMBER parsed before the bad argument is looked up first.
    if ('failed' in args) {
      if (zset && args.member) {
        resolveCenterFromMember(zset, args.member, ctx.server.profile)
      }
      throw args.failed
    }
    // A missing key replies `*0` without looking up the FROMMEMBER member.
    if (!zset) return array([])
    const center =
      args.from.type === 'member'
        ? resolveCenterFromMember(zset, args.from.member, ctx.server.profile)
        : { lon: args.from.lon, lat: args.from.lat }

    const matches = collectMatches(zset, center, args.by, ctx.server.profile)
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
      ctx.server.profile,
    )
  },
})
