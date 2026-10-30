import { asciiUpperCase, asciiUpperToken } from '../../core/ascii-case'
import {
  formatGeoCoordinate,
  type DoubleFormatProfile,
} from '../../core/double-format'
import type { CompatibilityProfile } from '../../core/compatibility/profile'
import { parseString2dToken } from '../../core/double-token'
import type { ParseContext } from '../../core/command-schema'
import { RedisValue } from '../../core/redis-value'
import { RedisResult } from '../../core/redis-result'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
  errors,
} from '../../core/redis-error'
import type {
  RedisSortedSetData,
  RedisSortedSetMember,
} from '../../state/data-types'
import { georadiusGetKeys } from '../../core/key-specs'
import { array, parseIntegerToken } from '../helpers'
import { getSortedMembers } from '../zsets/helpers'
import {
  assertValidCoordinates,
  decodeGeoScore,
  geoDistanceMeters,
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

/** A GEO* longitude or latitude, read like Redis' `string2d()`. */
export function parseGeoFloatToken(
  token: Buffer,
  profile: CompatibilityProfile,
): number {
  const value = parseString2dToken(token, profile)
  if (value === undefined) throw errors.expectedFloat()
  return value
}

function parseGeoUnit(
  token: Buffer | undefined,
  profile: CompatibilityProfile,
): string {
  if (!token) throw errors.syntax()
  const unit = token.toString()
  if (!isSupportedGeoUnit(unit, profile)) {
    throw errors.geoUnsupportedUnit(profile)
  }
  return unit
}

export function parseByRadius(
  radiusTok: Buffer | undefined,
  unitTok: Buffer | undefined,
  profile: CompatibilityProfile,
): GeoBy {
  if (!radiusTok) throw errors.syntax()
  const radius = parseString2dToken(radiusTok, profile)
  if (radius === undefined) throw errors.geoRadiusNotNumeric()
  if (radius < 0) throw errors.geoRadiusNegative()
  const unit = parseGeoUnit(unitTok, profile)
  return { kind: 'radius', radiusMeters: unitToMeters(radius, unit), unit }
}

export function parseByBox(
  widthTok: Buffer | undefined,
  heightTok: Buffer | undefined,
  unitTok: Buffer | undefined,
  profile: CompatibilityProfile,
): GeoBy {
  if (!widthTok) throw errors.syntax()
  const width = parseString2dToken(widthTok, profile)
  if (width === undefined) throw errors.geoWidthNotNumeric()
  if (!heightTok) throw errors.syntax()
  const height = parseString2dToken(heightTok, profile)
  if (height === undefined) throw errors.geoHeightNotNumeric()
  if (width < 0 || height < 0) throw errors.geoBoxNegative()
  const unit = parseGeoUnit(unitTok, profile)
  return {
    kind: 'box',
    widthMeters: unitToMeters(width, unit),
    heightMeters: unitToMeters(height, unit),
    unit,
  }
}

/**
 * A GEO argument error held back until execute. Real georadiusGeneric()
 * parses its arguments only after the WRONGTYPE check (and, for FROMMEMBER /
 * BYMEMBER, after looking the member up), so a wrong-typed key answers
 * WRONGTYPE whatever the arguments. Arity errors are not deferred.
 */
export function deferGeoParseError(err: unknown): RedisCommandError {
  if (
    !(err instanceof RedisCommandError) ||
    err instanceof WrongNumberOfArgumentsError
  ) {
    throw err
  }
  return err
}

/**
 * The keys a GEORADIUS / GEORADIUSBYMEMBER whose arguments failed to parse is
 * routed and locked by: Redis' georadiusGetKeys() over the raw arguments (the
 * source plus the last STORE / STOREDIST destination), as for a command whose
 * parser throws.
 */
export function georadiusRawKeys(
  commandName: string,
  input: readonly Buffer[],
): Buffer[] {
  return georadiusGetKeys([Buffer.from(commandName), ...input]).map(
    ({ key }) => key,
  )
}

export type GeoFrom =
  | { type: 'member'; member: Buffer }
  | { type: 'lonlat'; lon: number; lat: number }

/** ASC / DESC / COUNT n / ANY as the argument loop reads them. */
export type GeoOrderCountFlags = GeoOrderCount & { any?: boolean }

// Parses a single ASC | DESC | COUNT n | ANY token starting at cursor into
// `acc`. Returns the advanced cursor, or null if the token at cursor isn't
// one of these: the caller decides what to do with an unmatched token. ANY
// is a plain flag wherever it stands; finishOrderCount() checks it has a
// COUNT once the loop is over, as georadiusGeneric() does.
export function tryParseOrderOrCount(
  input: readonly Buffer[],
  cursor: number,
  acc: GeoOrderCountFlags,
): number | null {
  const token = asciiUpperToken(input[cursor])
  if (token === 'ASC') {
    acc.order = 'ASC'
    return cursor + 1
  }
  if (token === 'DESC') {
    acc.order = 'DESC'
    return cursor + 1
  }
  if (token === 'ANY') {
    acc.any = true
    return cursor + 1
  }
  if (token === 'COUNT') {
    const countTok = input[cursor + 1]
    if (!countTok) throw errors.syntax()
    const count = parseIntegerToken(countTok)
    if (count <= 0) throw errors.geoCountNotPositive()
    // The last COUNT wins.
    acc.count = { count, any: false }
    return cursor + 2
  }
  return null
}

/** The post-loop `any && !count` check, folding ANY into the COUNT. */
export function finishOrderCount(acc: GeoOrderCountFlags): GeoOrderCount {
  if (!acc.any) return { order: acc.order, count: acc.count }
  if (!acc.count) throw errors.geoAnyRequiresCount()
  return { order: acc.order, count: { count: acc.count.count, any: true } }
}

export type GeoSearchQuery = GeoOrderCount &
  GeoWithFlags & {
    from: GeoFrom
    by: GeoBy
    storeDist: boolean
  }

/**
 * GEOSEARCH / GEOSEARCHSTORE arguments. `lookups` holds every FROMMEMBER
 * member the loop reached, in order, before any argument error: execute
 * looks each one up (when the key exists) before raising `failed`.
 */
export type GeoSearchParse =
  | { lookups: Buffer[]; failed: RedisCommandError }
  | { lookups: Buffer[]; query: GeoSearchQuery }

/**
 * The georadiusGeneric() argument loop for GEOSEARCH (`store` false) and
 * GEOSEARCHSTORE: FROM*, BY* and the options in any order, each clause
 * parsed where it stands and repeatable (the last one wins), then the
 * post-loop checks: STORE with WITH*, no FROM, no BY, ANY without COUNT
 * (redis@8.0.0 geo.c:572-695).
 */
export function parseGeoSearchArgs(
  input: readonly Buffer[],
  start: number,
  ctx: ParseContext,
  store: boolean,
): GeoSearchParse {
  const lookups: Buffer[] = []
  try {
    const acc: GeoOrderCountFlags = {}
    let withCoord = false
    let withDist = false
    let withHash = false
    let storeDist = false
    let lonLat: GeoCenter | undefined
    let by: GeoBy | undefined
    let cursor = start
    while (cursor < input.length) {
      const advanced = tryParseOrderOrCount(input, cursor, acc)
      if (advanced !== null) {
        cursor = advanced
        continue
      }
      const token = asciiUpperToken(input[cursor])
      // Tokens after this one.
      const left = input.length - cursor - 1
      if (token === 'WITHDIST') {
        withDist = true
      } else if (token === 'WITHHASH') {
        withHash = true
      } else if (token === 'WITHCOORD') {
        withCoord = true
      } else if (token === 'STOREDIST' && store) {
        storeDist = true
      } else if (token === 'FROMMEMBER' && left >= 1 && !lonLat) {
        lookups.push(input[cursor + 1]!)
        cursor += 1
      } else if (token === 'FROMLONLAT' && left >= 2 && lookups.length === 0) {
        const lon = parseGeoFloatToken(input[cursor + 1]!, ctx.profile)
        const lat = parseGeoFloatToken(input[cursor + 2]!, ctx.profile)
        assertValidCoordinates(lon, lat)
        lonLat = { lon, lat }
        cursor += 2
      } else if (token === 'BYRADIUS' && left >= 2 && by?.kind !== 'box') {
        by = parseByRadius(input[cursor + 1], input[cursor + 2], ctx.profile)
        cursor += 2
      } else if (token === 'BYBOX' && left >= 3 && by?.kind !== 'radius') {
        by = parseByBox(
          input[cursor + 1],
          input[cursor + 2],
          input[cursor + 3],
          ctx.profile,
        )
        cursor += 3
      } else {
        throw errors.syntax()
      }
      cursor++
    }

    if (store && (withCoord || withDist || withHash)) {
      throw errors.geoSearchStoreWithOptions(ctx.profile)
    }
    const command = ctx.rawCommand ?? ctx.commandName
    const lastMember = lookups[lookups.length - 1]
    const from: GeoFrom | undefined = lastMember
      ? { type: 'member', member: lastMember }
      : lonLat && { type: 'lonlat', ...lonLat }
    if (!from) throw errors.geoSearchNoFrom(command)
    if (!by) throw errors.geoSearchNoBy(command)
    const orderCount = finishOrderCount(acc)
    return {
      lookups,
      query: {
        ...orderCount,
        from,
        by,
        withCoord,
        withDist,
        withHash,
        storeDist,
      },
    }
  } catch (err) {
    return { lookups, failed: deferGeoParseError(err) }
  }
}

/**
 * Replays a parsed GEOSEARCH against its key: every FROMMEMBER lookup in
 * argument order (only when the key exists), then the held argument error.
 * Returns null for a missing key, else the query and its center.
 */
export function resolveGeoSearch(
  zset: RedisSortedSetData | null | undefined,
  parsed: GeoSearchParse,
  profile: CompatibilityProfile,
): { query: GeoSearchQuery; center: GeoCenter } | null {
  let center: GeoCenter | undefined
  if (zset) {
    for (const member of parsed.lookups) {
      center = resolveCenterFromMember(zset, member, profile)
    }
  }
  if ('failed' in parsed) throw parsed.failed
  if (!zset) return null
  const { query } = parsed
  if (query.from.type === 'lonlat') {
    center = { lon: query.from.lon, lat: query.from.lat }
  }
  if (!center) throw errors.syntax()
  return { query, center }
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
  profile: CompatibilityProfile,
): [GeoRadiusFlags, number] {
  const options: GeoOrderCountFlags = {}
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
    const token = asciiUpperCase(input[cursor]!.toString())
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
      if (!dest) throw errors.syntax()
      // The last STORE / STOREDIST wins, key and kind, as in Redis.
      store = dest
      storeDist = undefined
      cursor += 2
      continue
    }
    if (allowStore && token === 'STOREDIST') {
      const dest = input[cursor + 1]
      if (!dest) throw errors.syntax()
      storeDist = dest
      store = undefined
      cursor += 2
      continue
    }
    throw errors.syntax()
  }

  if ((store || storeDist) && (withCoord || withDist || withHash)) {
    throw errors.geoRadiusStoreWithOptions(profile)
  }
  const orderCount = finishOrderCount(options)

  return [
    { ...orderCount, withCoord, withDist, withHash, store, storeDist },
    cursor,
  ]
}

// geoWithinShape(): the radius test is geohashGetDistanceIfInRadiusWGS84()
// (center first) and the box test geohashGetDistanceIfInRectangle(), which
// compares the latitude distance and the longitude distance at the point's
// latitude against half the height / width. Redis 7.0.6+ computes the former
// with geohashGetLatDistance(); geoDistanceMeters() on a same-longitude pair
// is that same expression there, and the full haversine on older servers.
function withinBy(
  center: GeoCenter,
  point: GeoCenter,
  by: GeoBy,
  profile: CompatibilityProfile,
): number | null {
  if (by.kind === 'radius') {
    const dist = geoDistanceMeters(
      center.lon,
      center.lat,
      point.lon,
      point.lat,
      profile,
    )
    return dist <= by.radiusMeters ? dist : null
  }

  const latDist = geoDistanceMeters(
    point.lon,
    point.lat,
    point.lon,
    center.lat,
    profile,
  )
  if (latDist > by.heightMeters / 2) return null
  const lonDist = geoDistanceMeters(
    point.lon,
    point.lat,
    center.lon,
    point.lat,
    profile,
  )
  if (lonDist > by.widthMeters / 2) return null
  return geoDistanceMeters(
    center.lon,
    center.lat,
    point.lon,
    point.lat,
    profile,
  )
}

export function resolveCenterFromMember(
  zset: RedisSortedSetData,
  member: Buffer,
  profile: CompatibilityProfile,
): GeoCenter {
  const entry = zset.members.get(member.toString('hex'))
  if (!entry) throw errors.geoMissingMember(member, profile)
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
  profile: CompatibilityProfile,
): GeoMatch[] {
  if (!zset) return []
  const matches: GeoMatch[] = []
  for (const entry of getSortedMembers(zset)) {
    const point = decodeGeoScore(entry.score)
    const distanceMeters = withinBy(center, point, by, profile)
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
  profile: DoubleFormatProfile,
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
            RedisValue.double(m.lon, formatGeoCoordinate(m.lon, profile)),
            RedisValue.double(m.lat, formatGeoCoordinate(m.lat, profile)),
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
