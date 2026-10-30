import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import { array } from '../helpers'
import {
  buildSearchReply,
  collectMatches,
  orderAndLimit,
  parseGeoSearchArgs,
  resolveGeoSearch,
  type GeoSearchParse,
} from './search-core'

type GeoSearchArgs = { key: Buffer; search: GeoSearchParse }

function createGeoSearchSchema() {
  return t.custom<GeoSearchArgs>(
    { min: 6, keys: [0] },
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) throw new WrongNumberOfArgumentsError(ctx.commandName)
      return {
        value: {
          key,
          search: parseGeoSearchArgs(input, index + 1, ctx, false),
        },
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
    // Each FROMMEMBER is looked up where it stood, then any argument error
    // is raised; a missing key replies `*0` without a lookup.
    const resolved = resolveGeoSearch(zset, args.search, ctx.server.profile)
    if (!resolved || !zset) return array([])
    const { query, center } = resolved

    const matches = collectMatches(zset, center, query.by, ctx.server.profile)
    const ordered = orderAndLimit(matches, {
      order: query.order,
      count: query.count,
    })
    return buildSearchReply(
      ordered,
      {
        withCoord: query.withCoord,
        withDist: query.withDist,
        withHash: query.withHash,
      },
      query.by.unit,
      ctx.server.profile,
    )
  },
})
