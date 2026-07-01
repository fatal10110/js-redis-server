import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import type { RedisExecutionContext } from '../../core/redis-context'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import { integer } from '../helpers'
import { assertValidCoordinates } from './helpers'
import {
  buildSearchReply,
  buildStoreMembers,
  collectMatches,
  orderAndLimit,
  parseByRadius,
  parseGeoFloatToken,
  parseGeoRadiusFlags,
  type GeoBy,
  type GeoRadiusFlags,
} from './search-core'

type GeoRadiusArgs = GeoRadiusFlags & {
  key: Buffer
  lon: number
  lat: number
  by: GeoBy
}

function createGeoRadiusSchema(allowStore: boolean) {
  return t.custom<GeoRadiusArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const lonTok = input[index + 1]
      const latTok = input[index + 2]
      const radiusTok = input[index + 3]
      const unitTok = input[index + 4]
      if (!key || !lonTok || !latTok || !radiusTok || !unitTok) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const lon = parseGeoFloatToken(lonTok)
      const lat = parseGeoFloatToken(latTok)
      assertValidCoordinates(lon, lat)
      const by = parseByRadius(radiusTok, unitTok)
      const [flags, nextIndex] = parseGeoRadiusFlags(
        input,
        index + 5,
        allowStore,
      )

      return { value: { key, lon, lat, by, ...flags }, nextIndex }
    },
  )
}

function executeGeoRadius(args: GeoRadiusArgs, ctx: RedisExecutionContext) {
  const zset = ctx.db.getSortedSet(args.key)
  const matches = collectMatches(
    zset,
    { lon: args.lon, lat: args.lat },
    args.by,
  )
  const ordered = orderAndLimit(matches, {
    order: args.order,
    count: args.count,
  })

  const dest = args.store ?? args.storeDist
  if (dest) {
    const members = buildStoreMembers(
      ordered,
      args.storeDist !== undefined,
      args.by.unit,
    )
    if (members.size === 0) {
      ctx.db.delete(dest)
      return integer(0)
    }
    ctx.db.delete(dest)
    ctx.db.updateSortedSet(dest, destZset => {
      destZset.replaceMembers(members, { forceDirty: true })
    })
    return integer(members.size)
  }

  return buildSearchReply(
    ordered,
    {
      withCoord: args.withCoord,
      withDist: args.withDist,
      withHash: args.withHash,
    },
    args.by.unit,
  )
}

export const georadiusCommand = defineCommand({
  name: 'georadius',
  schema: createGeoRadiusSchema(true),
  flags: ['write', 'denyoom'],
  keys: args =>
    (args.store ?? args.storeDist)
      ? [args.key, (args.store ?? args.storeDist)!]
      : [args.key],
  execute: executeGeoRadius,
})

export const georadiusRoCommand = defineCommand({
  name: 'georadius_ro',
  schema: createGeoRadiusSchema(false),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: executeGeoRadius,
})
