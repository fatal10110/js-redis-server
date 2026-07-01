import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { RedisValue } from '../../core/redis-value'
import { array } from '../helpers'
import { decodeGeoScore } from './helpers'

export const geoposCommand = defineCommand({
  name: 'geopos',
  schema: t.object({ key: t.key(), members: t.variadic(t.key(), { min: 1 }) }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const zset = ctx.db.getSortedSet(args.key)
    const items = args.members.map(member => {
      const entry = zset?.members.get(member.toString('hex'))
      if (!entry) return RedisValue.null()
      const { lon, lat } = decodeGeoScore(entry.score)
      return RedisValue.array([
        RedisValue.bulkString(Buffer.from(lon.toString())),
        RedisValue.bulkString(Buffer.from(lat.toString())),
      ])
    })
    return array(items)
  },
})
