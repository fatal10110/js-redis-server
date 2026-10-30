import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import { integer } from '../helpers'
import {
  buildStoreMembers,
  collectMatches,
  orderAndLimit,
  parseGeoSearchArgs,
  resolveGeoSearch,
  type GeoSearchParse,
} from './search-core'
import { storedZsetConvertedIfFits } from '../../state/zset-encoding'
import { zsetEncodingRules } from '../zsets/helpers'

type GeoSearchStoreArgs = {
  destination: Buffer
  source: Buffer
  search: GeoSearchParse
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
      return {
        value: {
          destination,
          source,
          search: parseGeoSearchArgs(input, index + 2, ctx, true),
        },
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
    // Each FROMMEMBER is looked up where it stood, then any argument error
    // is raised; a missing source deletes the destination and replies `:0`
    // without a lookup.
    const resolved = resolveGeoSearch(zset, args.search, ctx.server.profile)
    if (!resolved || !zset) {
      ctx.db.delete(args.destination)
      return integer(0)
    }
    const { query, center } = resolved

    const matches = collectMatches(zset, center, query.by, ctx.server.profile)
    const ordered = orderAndLimit(matches, {
      order: query.order,
      count: query.count,
    })
    const members = buildStoreMembers(ordered, query.storeDist, query.by.unit)

    if (members.size === 0) {
      ctx.db.delete(args.destination)
      return integer(0)
    }

    // One write replacing whatever the destination held: a single
    // `geosearchstore`, not `del` followed by it.
    ctx.db.set(
      args.destination,
      storedZsetConvertedIfFits(members, zsetEncodingRules(ctx.server)),
    )
    return integer(members.size)
  },
})
