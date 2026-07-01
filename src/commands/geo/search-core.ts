import { RedisValue } from '../../core/redis-value'
import { RedisResult } from '../../core/redis-result'
import {
  ExpectedFloatError,
  GeoAnyRequiresCountError,
  GeoBoxNegativeError,
  GeoCountNotPositiveError,
  GeoHeightNotNumericError,
  GeoMissingMemberError,
  GeoRadiusNegativeError,
  GeoRadiusNotNumericError,
  GeoRadiusStoreWithOptionsError,
  GeoUnsupportedUnitError,
  GeoWidthNotNumericError,
  RedisSyntaxError,
  WrongNumberOfArgumentsError,
} from '../../core/redis-error'
import type {
  RedisSortedSetData,
  RedisSortedSetMember,
} from '../../state/data-types'
import { array, parseIntegerToken } from '../helpers'
import { getSortedMembers } from '../zsets/helpers'
import {
  assertValidCoordinates,
  decodeGeoScore,
  haversineMeters,
  isSupportedGeoUnit,
  metersToUnit,
  unitToMeters,
} from './helpers'

export type GeoCenter = { lon: number; lat: number }

export type GeoBy =
  | { kind: 'radius'; radiusMeters: number; unit: string }
  | { kind: 'box'; widthMeters: number; heightMeters: number; unit: string }

export type GeoOrderCount = {
  order?: 'ASC' | 'DESC'
  count?: { count: number; any: boolean }
}

export type GeoWithFlags = {
  withCoord: boolean
  withDist: boolean
  withHash: boolean
}

export type GeoMatch = {
  member: Buffer
  score: number
  distanceMeters: number
  lon: number
  lat: number
}

export function parseGeoFloatToken(token: Buffer): number {
  const n = Number(token.toString())
  if (!Number.isFinite(n)) throw new ExpectedFloatError()
  return n
}

function parseGeoUnit(token: Buffer | undefined): string {
  if (!token) throw new RedisSyntaxError()
  const unit = token.toString()
  if (!isSupportedGeoUnit(unit)) throw new GeoUnsupportedUnitError()
  return unit
}

export function parseByRadius(
  radiusTok: Buffer | undefined,
  unitTok: Buffer | undefined,
): GeoBy {
  if (!radiusTok) throw new RedisSyntaxError()
  const radius = Number(radiusTok.toString())
  if (!Number.isFinite(radius)) throw new GeoRadiusNotNumericError()
  if (radius < 0) throw new GeoRadiusNegativeError()
  const unit = parseGeoUnit(unitTok)
  return { kind: 'radius', radiusMeters: unitToMeters(radius, unit), unit }
}

export function parseByBox(
  widthTok: Buffer | undefined,
  heightTok: Buffer | undefined,
  unitTok: Buffer | undefined,
): GeoBy {
  if (!widthTok) throw new RedisSyntaxError()
  const width = Number(widthTok.toString())
  if (!Number.isFinite(width)) throw new GeoWidthNotNumericError()
  if (!heightTok) throw new RedisSyntaxError()
  const height = Number(heightTok.toString())
  if (!Number.isFinite(height)) throw new GeoHeightNotNumericError()
  if (width < 0 || height < 0) throw new GeoBoxNegativeError()
  const unit = parseGeoUnit(unitTok)
  return {
    kind: 'box',
    widthMeters: unitToMeters(width, unit),
    heightMeters: unitToMeters(height, unit),
    unit,
  }
}

export type GeoFrom =
  | { type: 'member'; member: Buffer }
  | { type: 'lonlat'; lon: number; lat: number }

// FROMMEMBER member | FROMLONLAT lon lat, shared by GEOSEARCH and
// GEOSEARCHSTORE. A present-but-incomplete FROM clause is a wrong-number-of-
// arguments error in real Redis (ground-truthed), not a syntax error.
export function parseGeoFrom(
  input: readonly Buffer[],
  cursor: number,
  commandName: string,
): [GeoFrom, number] {
  const token = input[cursor]?.toString().toUpperCase()
  if (token === 'FROMMEMBER') {
    const member = input[cursor + 1]
    if (!member) throw new WrongNumberOfArgumentsError(commandName)
    return [{ type: 'member', member }, cursor + 2]
  }
  if (token === 'FROMLONLAT') {
    const lonTok = input[cursor + 1]
    const latTok = input[cursor + 2]
    if (!lonTok || !latTok) throw new WrongNumberOfArgumentsError(commandName)
    const lon = parseGeoFloatToken(lonTok)
    const lat = parseGeoFloatToken(latTok)
    assertValidCoordinates(lon, lat)
    return [{ type: 'lonlat', lon, lat }, cursor + 3]
  }
  throw new RedisSyntaxError()
}

// BYRADIUS radius unit | BYBOX width height unit, shared the same way.
export function parseGeoBy(
  input: readonly Buffer[],
  cursor: number,
): [GeoBy, number] {
  const token = input[cursor]?.toString().toUpperCase()
  if (token === 'BYRADIUS') {
    return [parseByRadius(input[cursor + 1], input[cursor + 2]), cursor + 3]
  }
  if (token === 'BYBOX') {
    return [
      parseByBox(input[cursor + 1], input[cursor + 2], input[cursor + 3]),
      cursor + 4,
    ]
  }
  throw new RedisSyntaxError()
}

// Parses a single ASC | DESC | COUNT n [ANY] token starting at cursor into
// `acc`. Returns the advanced cursor, or null if the token at cursor isn't
// one of these — the caller decides what to do with an unmatched token.
export function tryParseOrderOrCount(
  input: readonly Buffer[],
  cursor: number,
  acc: GeoOrderCount,
): number | null {
  const token = input[cursor]?.toString().toUpperCase()
  if (token === 'ASC') {
    acc.order = 'ASC'
    return cursor + 1
  }
  if (token === 'DESC') {
    acc.order = 'DESC'
    return cursor + 1
  }
  if (token === 'COUNT') {
    const countTok = input[cursor + 1]
    if (!countTok) throw new RedisSyntaxError()
    const count = parseIntegerToken(countTok)
    if (count <= 0) throw new GeoCountNotPositiveError()
    let next = cursor + 2
    let any = false
    if (input[next]?.toString().toUpperCase() === 'ANY') {
      any = true
      next++
    }
    acc.count = { count, any }
    return next
  }
  if (token === 'ANY') throw new GeoAnyRequiresCountError()
  return null
}

export type GeoRadiusFlags = GeoOrderCount & {
  withCoord: boolean
  withDist: boolean
  withHash: boolean
  store?: Buffer
  storeDist?: Buffer
}

// Shared WITH*/ASC|DESC/COUNT[/STORE/STOREDIST] tail for GEORADIUS[BYMEMBER]
// [_RO]. `allowStore` is false for the _RO variants, where STORE/STOREDIST
// aren't recognized at all (falls through to a generic syntax error, matching
// real Redis) rather than being accepted and then rejected.
export function parseGeoRadiusFlags(
  input: readonly Buffer[],
  cursor: number,
  allowStore: boolean,
): [GeoRadiusFlags, number] {
  const options: GeoOrderCount = {}
  let withCoord = false
  let withDist = false
  let withHash = false
  let store: Buffer | undefined
  let storeDist: Buffer | undefined

  while (cursor < input.length) {
    const advanced = tryParseOrderOrCount(input, cursor, options)
    if (advanced !== null) {
      cursor = advanced
      continue
    }
    const token = input[cursor]!.toString().toUpperCase()
    if (token === 'WITHCOORD') {
      withCoord = true
      cursor++
      continue
    }
    if (token === 'WITHDIST') {
      withDist = true
      cursor++
      continue
    }
    if (token === 'WITHHASH') {
      withHash = true
      cursor++
      continue
    }
    if (allowStore && token === 'STORE') {
      const dest = input[cursor + 1]
      if (!dest) throw new RedisSyntaxError()
      store = dest
      cursor += 2
      continue
    }
    if (allowStore && token === 'STOREDIST') {
      const dest = input[cursor + 1]
      if (!dest) throw new RedisSyntaxError()
      storeDist = dest
      cursor += 2
      continue
    }
    throw new RedisSyntaxError()
  }

  if ((store || storeDist) && (withCoord || withDist || withHash)) {
    throw new GeoRadiusStoreWithOptionsError()
  }

  return [
    { ...options, withCoord, withDist, withHash, store, storeDist },
    cursor,
  ]
}

// The stored zset score is a 52-bit geohash; real Redis' box test compares
// component-wise great-circle distances (same-lon for latitude, same-lat for
// longitude) against half the width/height, not a single radial distance —
// see geohashGetDistanceIfInRectangle in Redis' geohash_helper.c.
function withinBy(
  center: GeoCenter,
  point: GeoCenter,
  by: GeoBy,
): number | null {
  if (by.kind === 'radius') {
    const dist = haversineMeters(center.lon, center.lat, point.lon, point.lat)
    return dist <= by.radiusMeters ? dist : null
  }

  const latDist = haversineMeters(center.lon, center.lat, center.lon, point.lat)
  if (latDist > by.heightMeters / 2) return null
  const lonDist = haversineMeters(center.lon, center.lat, point.lon, center.lat)
  if (lonDist > by.widthMeters / 2) return null
  return haversineMeters(center.lon, center.lat, point.lon, point.lat)
}

export function resolveCenterFromMember(
  zset: RedisSortedSetData | null,
  member: Buffer,
): GeoCenter {
  const entry = zset?.members.get(member.toString('hex'))
  if (!entry) throw new GeoMissingMemberError()
  return decodeGeoScore(entry.score)
}

// Brute-force scan: decode every member and test it against the search
// shape. O(n) rather than Redis' neighbor-cell box scan, but the same result
// set — see issue #326 implementation notes (ponytail: box-scan optimizer
// only pays off once a test proves brute force is too slow, which it hasn't).
export function collectMatches(
  zset: RedisSortedSetData | null,
  center: GeoCenter,
  by: GeoBy,
): GeoMatch[] {
  if (!zset) return []
  const matches: GeoMatch[] = []
  for (const entry of getSortedMembers(zset)) {
    const point = decodeGeoScore(entry.score)
    const distanceMeters = withinBy(center, point, by)
    if (distanceMeters === null) continue
    matches.push({
      member: entry.member,
      score: entry.score,
      distanceMeters,
      lon: point.lon,
      lat: point.lat,
    })
  }
  return matches
}

function sortByDistance(
  matches: GeoMatch[],
  order: 'ASC' | 'DESC',
): GeoMatch[] {
  const sorted = matches
    .slice()
    .sort((a, b) => a.distanceMeters - b.distanceMeters)
  return order === 'DESC' ? sorted.reverse() : sorted
}

// Ordering matches real Redis' observed behavior: plain COUNT (no ASC/DESC)
// defaults to nearest-first; COUNT ANY skips the distance sort entirely and
// takes the first N in zset score order (only sorting that subset if
// ASC/DESC was also given) — real Redis' ANY doesn't guarantee "closest N"
// either, so this is not a fixed neighbor-cell traversal order to match.
export function orderAndLimit(
  matches: GeoMatch[],
  options: GeoOrderCount,
): GeoMatch[] {
  if (options.count?.any) {
    const limited = matches.slice(0, options.count.count)
    return options.order ? sortByDistance(limited, options.order) : limited
  }

  let working = matches
  if (options.order) {
    working = sortByDistance(matches, options.order)
  } else if (options.count) {
    working = sortByDistance(matches, 'ASC')
  }
  return options.count ? working.slice(0, options.count.count) : working
}

export function buildSearchReply(
  matches: GeoMatch[],
  flags: GeoWithFlags,
  unit: string,
): RedisResult {
  if (!flags.withCoord && !flags.withDist && !flags.withHash) {
    return array(matches.map(m => RedisValue.bulkString(m.member)))
  }

  return array(
    matches.map(m => {
      const parts: RedisValue[] = [RedisValue.bulkString(m.member)]
      if (flags.withDist) {
        parts.push(
          RedisValue.bulkString(
            Buffer.from(metersToUnit(m.distanceMeters, unit).toFixed(4)),
          ),
        )
      }
      if (flags.withHash) parts.push(RedisValue.integer(m.score))
      if (flags.withCoord) {
        parts.push(
          RedisValue.array([
            RedisValue.bulkString(Buffer.from(m.lon.toString())),
            RedisValue.bulkString(Buffer.from(m.lat.toString())),
          ]),
        )
      }
      return RedisValue.array(parts)
    }),
  )
}

// STOREDIST stores the query-unit distance as the score instead of the
// geohash; without it the original geohash score is reused as-is (not
// re-derived from the decoded lon/lat, which would lose precision).
export function buildStoreMembers(
  matches: GeoMatch[],
  storeDist: boolean,
  unit: string,
): Map<string, RedisSortedSetMember> {
  return new Map(
    matches.map(m => [
      m.member.toString('hex'),
      {
        member: Buffer.from(m.member),
        score: storeDist ? metersToUnit(m.distanceMeters, unit) : m.score,
      },
    ]),
  )
}
