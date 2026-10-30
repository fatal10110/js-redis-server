import { defineCommand } from '../../core/command-definition'
import { georadiusGetKeys } from '../../core/key-specs'
import { t, type ParseContext } from '../../core/command-schema'
import type { RedisExecutionContext } from '../../core/redis-context'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { integer } from '../helpers'
import { assertValidCoordinates } from './helpers'
import {
  buildSearchReply,
  buildStoreMembers,
  collectMatches,
  deferGeoParseError,
  georadiusRawKeys,
  orderAndLimit,
  parseByRadius,
  parseGeoFloatToken,
  parseGeoRadiusFlags,
  type GeoBy,
  type GeoRadiusFlags,
} from './search-core'
import { storedZsetConvertedIfFits } from '../../state/zset-encoding'
import { zsetEncodingRules } from '../zsets/helpers'

type GeoRadiusArgs = GeoRadiusFlags & {
  key: Buffer
  lon: number
  lat: number
  by: GeoBy
}

type GeoRadiusParsed =
  GeoRadiusArgs | { key: Buffer; failed: RedisCommandError; rawKeys: Buffer[] }

function createGeoRadiusSchema(allowStore: boolean) {
  return t.custom<GeoRadiusParsed>(
    { min: 5, keys: [0] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      const lonTok = input[index + 1]
      const latTok = input[index + 2]
      const radiusTok = input[index + 3]
      const unitTok = input[index + 4]
      if (!key || !lonTok || !latTok || !radiusTok || !unitTok) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      try {
        const lon = parseGeoFloatToken(lonTok, ctx.profile)
        const lat = parseGeoFloatToken(latTok, ctx.profile)
        assertValidCoordinates(lon, lat)
        const by = parseByRadius(radiusTok, unitTok, ctx.profile)
        const [flags, nextIndex] = parseGeoRadiusFlags(
          input,
          index + 5,
          allowStore,
          ctx.profile,
        )

        return { value: { key, lon, lat, by, ...flags }, nextIndex }
      } catch (err) {
        return {
          value: {
            key,
            failed: deferGeoParseError(err),
            rawKeys: georadiusRawKeys(ctx.commandName, input),
          },
          nextIndex: input.length,
        }
      }
    },
  )
}

function executeGeoRadius(args: GeoRadiusParsed, ctx: RedisExecutionContext) {
  const zset = ctx.db.getSortedSet(args.key)
  if ('failed' in args) throw args.failed
  const matches = collectMatches(
    zset,
    { lon: args.lon, lat: args.lat },
    args.by,
    ctx.server.profile,
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
    // One write replacing whatever the destination held, published as
    // `georadiusstore` like real Redis — not `del` followed by the command.
    ctx.db
      .withOrigin('georadiusstore')
      .set(
        dest,
        storedZsetConvertedIfFits(members, zsetEncodingRules(ctx.server)),
      )
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
    ctx.server.profile,
  )
}

export const georadiusCommand = defineCommand({
  name: 'georadius',
  rawKeys: georadiusGetKeys,
  schema: createGeoRadiusSchema(true),
  flags: ['write', 'denyoom'],
  keys: args => {
    if ('failed' in args) return args.rawKeys
    const dest = args.store ?? args.storeDist
    return dest ? [args.key, dest] : [args.key]
  },
  execute: executeGeoRadius,
})

export const georadiusRoCommand = defineCommand({
  name: 'georadius_ro',
  schema: createGeoRadiusSchema(false),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: executeGeoRadius,
})
