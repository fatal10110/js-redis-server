import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  GeoSearchStoreWithOptionsError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
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
      const [from, afterFrom] = parseGeoFrom(input, cursor, ctx.commandName)
      cursor = afterFrom

      if (cursor >= input.length) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }
      const [by, afterBy] = parseGeoBy(input, cursor)
      cursor = afterBy

      const options: GeoOrderCount = {}
      let storeDist = false
      while (cursor < input.length) {
        const advanced = tryParseOrderOrCount(input, cursor, options)
        if (advanced !== null) {
          cursor = advanced
          continue
        }
        const token = input[cursor]!.toString().toUpperCase()
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
          throw new GeoSearchStoreWithOptionsError()
        }
        throw new RedisSyntaxError()
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
    const center =
      args.from.type === 'member'
        ? resolveCenterFromMember(zset, args.from.member)
        : { lon: args.from.lon, lat: args.from.lat }

    const matches = collectMatches(zset, center, args.by)
    const ordered = orderAndLimit(matches, {
      order: args.order,
      count: args.count,
    })
    const members = buildStoreMembers(ordered, args.storeDist, args.by.unit)

    if (members.size === 0) {
      ctx.db.delete(args.destination)
      return integer(0)
    }

    ctx.db.delete(args.destination)
    ctx.db.updateSortedSet(args.destination, destZset => {
      destZset.replaceMembers(members, { forceDirty: true })
    })
    return integer(members.size)
  },
})
