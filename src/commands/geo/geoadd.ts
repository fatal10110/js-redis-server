import { defineCommand } from '../../core/command-definition'
import { t, type ParseContext } from '../../core/command-schema'
import {
  ExpectedFloatError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import { integer } from '../helpers'
import { assertValidCoordinates, encodeGeoScore } from './helpers'

type GeoPoint = { lon: number; lat: number; member: Buffer }
type GeoAddOptions = { nx: boolean; xx: boolean; ch: boolean }
type GeoAddArgs = { key: Buffer; options: GeoAddOptions; points: GeoPoint[] }

function parseFloatToken(token: Buffer): number {
  const n = Number(token.toString())
  if (!Number.isFinite(n)) throw new ExpectedFloatError()
  return n
}

function createGeoAddSchema() {
  return t.custom<GeoAddArgs>(
    (input: readonly Buffer[], index: number, ctx: ParseContext) => {
      const key = input[index]
      if (!key) throw new WrongNumberOfArgumentsError(ctx.commandName)

      let cursor = index + 1
      if (input.length - cursor < 3) {
        throw new WrongNumberOfArgumentsError(ctx.commandName)
      }

      const options: GeoAddOptions = { nx: false, xx: false, ch: false }
      while (cursor < input.length) {
        const token = input[cursor]!.toString().toUpperCase()
        if (token === 'NX') {
          options.nx = true
          cursor++
          continue
        }
        if (token === 'XX') {
          options.xx = true
          cursor++
          continue
        }
        if (token === 'CH') {
          options.ch = true
          cursor++
          continue
        }
        break
      }

      const remaining = input.length - cursor
      if (options.nx && options.xx) throw new RedisSyntaxError()
      if (remaining === 0 || remaining % 3 !== 0) {
        throw new RedisSyntaxError()
      }

      const points: GeoPoint[] = []
      while (cursor < input.length) {
        const lon = parseFloatToken(input[cursor]!)
        const lat = parseFloatToken(input[cursor + 1]!)
        assertValidCoordinates(lon, lat)
        points.push({ lon, lat, member: input[cursor + 2]! })
        cursor += 3
      }

      return { value: { key, options, points }, nextIndex: input.length }
    },
  )
}

export const geoaddCommand = defineCommand({
  name: 'geoadd',
  schema: createGeoAddSchema(),
  flags: ['write', 'denyoom'],
  keys: args => [args.key],
  execute: (args, ctx) => {
    const replyCount = ctx.db.updateSortedSet(args.key, zset => {
      let count = 0
      for (const { lon, lat, member } of args.points) {
        const score = encodeGeoScore(lon, lat)
        const existing = zset.getMember(member)

        if (!existing && args.options.xx) continue
        if (existing && args.options.nx) continue

        const { added, scoreChanged } = zset.setScore(member, score)
        if (added || (args.options.ch && scoreChanged)) count++
      }
      return count
    })
    return integer(replyCount)
  },
})
