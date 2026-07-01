import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { GeoUnsupportedUnitError } from '../../core/redis-error'
import { bulk } from '../helpers'
import {
  decodeGeoScore,
  haversineMeters,
  isSupportedGeoUnit,
  metersToUnit,
} from './helpers'

export const geodistCommand = defineCommand({
  name: 'geodist',
  schema: t.object({
    key: t.key(),
    member1: t.key(),
    member2: t.key(),
    unit: t.optional(t.string()),
  }),
  flags: ['readonly'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const unit = args.unit ?? 'm'
    if (!isSupportedGeoUnit(unit)) throw new GeoUnsupportedUnitError()

    const zset = ctx.db.getSortedSet(args.key)
    const entry1 = zset?.members.get(args.member1.toString('hex'))
    const entry2 = zset?.members.get(args.member2.toString('hex'))
    if (!entry1 || !entry2) return bulk(null)

    const pos1 = decodeGeoScore(entry1.score)
    const pos2 = decodeGeoScore(entry2.score)
    const meters = haversineMeters(pos1.lon, pos1.lat, pos2.lon, pos2.lat)
    return bulk(Buffer.from(metersToUnit(meters, unit).toFixed(4)))
  },
})
