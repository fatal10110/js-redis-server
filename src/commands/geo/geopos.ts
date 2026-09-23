import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { formatGeoCoordinate } from '../../core/double-format'
import { RedisValue } from '../../core/redis-value'
import { array } from '../helpers'
import { decodeGeoScore } from './helpers'

export const geoposCommand = defineCommand({
  name: 'geopos',
  schema: t.object({ key: t.key(), members: t.variadic(t.bulk()) }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    const items = args.members.map(member => {
      const entry = zset?.members.get(member.toString('hex'))
      if (!entry) return RedisValue.null()
      const { lon, lat } = decodeGeoScore(entry.score)
      return RedisValue.array([
        RedisValue.double(lon, formatGeoCoordinate(lon, ctx.server.profile)),
        RedisValue.double(lat, formatGeoCoordinate(lat, ctx.server.profile)),
      ])
    })
    return array(items)
  },
})
