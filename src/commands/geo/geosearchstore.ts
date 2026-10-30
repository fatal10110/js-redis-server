import { asciiUpperCase } from '../../core/ascii-case'
import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError, errors } from '../../core/redis-error'
import { integer } from '../helpers'
import {
  buildStoreMembers,
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

type GeoSearchStoreArgs = GeoOrderCount & {
  destination: Buffer
  source: Buffer
  from: GeoFrom
  by: GeoBy
  storeDist: boolean
}

function createGeoSearchStoreSchema() {
  return t.custom<GeoSearchStoreArgs>(
    { min: 7, keys: [0, 1] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const destination = input[index]
      const source = input[index + 1]
      if (!destination || !source) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      let cursor = index + 2
      if (cursor >= input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const [from, afterFrom] = parseGeoFrom(
        input,
        cursor,
        ctx.commandName,
        ctx.profile,
      )
      cursor = afterFrom

      if (cursor >= input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const [by, afterBy] = parseGeoBy(input, cursor, ctx.profile)
      cursor = afterBy

      const options: GeoOrderCount = {}
      let storeDist = false
      while (cursor < input.length) {
        const advanced = tryParseOrderOrCount(input, cursor, options)
        if (advanced !== null) {
          cursor = advanced
          continue
        }
        const token = asciiUpperCase(input[cursor]!.toString())
        if (token === 'STOREDIST') {
          storeDist = true
          cursor++
          continue
        }
        if (
          token === 'WITHCOORD' ||
          token === 'WITHDIST' ||
          token === 'WITHHASH'
        ) {
          throw errors.geoSearchStoreWithOptions(ctx.profile)
        }
        throw errors.syntax()
      }

      return {
        value: { destination, source, from, by, storeDist, ...options },
        nextIndex: input.length,
      }
    },
  )
}

export const geosearchstoreCommand = defineCommand({
  name: 'geosearchstore',
  since: { redis: '6.2.0', valkey: '7.2.0' },
  schema: createGeoSearchStoreSchema(),
  flags: ['write', 'denyoom'],
  keys: args => [args.destination, args.source],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.source)
    // A missing source deletes the destination and replies `:0` without
    // looking up the FROMMEMBER member.
    if (!zset) {
      ctx.db.delete(args.destination)
      return integer(0)
    }
    const center =
      args.from.type === 'member'
        ? resolveCenterFromMember(zset, args.from.member, ctx.server.profile)
        : { lon: args.from.lon, lat: args.from.lat }

    const matches = collectMatches(zset, center, args.by, ctx.server.profile)
    const ordered = orderAndLimit(matches, {
      order: args.order,
      count: args.count,
    })
    const members = buildStoreMembers(ordered, args.storeDist, args.by.unit)

    if (members.size === 0) {
      ctx.db.delete(args.destination)
      return integer(0)
    }

    // One write replacing whatever the destination held: a single
    // `geosearchstore`, not `del` followed by it.
    ctx.db.set(args.destination, { type: 'zset', members })
    return integer(members.size)
  },
})
