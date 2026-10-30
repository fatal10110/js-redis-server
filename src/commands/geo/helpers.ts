import { asciiLowerCase } from '../../core/ascii-case'
import type { CompatibilityProfile } from '../../core/compatibility/profile'
import { errors } from '../../core/redis-error'

const STEP = 26n
const CELLS = Number(1n << STEP)

export const GEO_LON_MIN = -180
export const GEO_LON_MAX = 180
export const GEO_LAT_MIN = -85.05112878
export const GEO_LAT_MAX = 85.05112878

const GEO_ALPHABET = '0123456789bcdefghjkmnpqrstuvwxyz'

const EARTH_RADIUS_M = 6372797.560856

// Meters-per-unit divisors matching Redis' exact GEODIST constants (not the
// rounded 3.28084 ft/m reciprocal, which drifts in the 4th decimal place).
const UNIT_TO_METERS: Record<string, number> = {
  m: 1,
  km: 1000,
  ft: 0.3048,
  mi: 1609.34,
}

// Own keys only: `in` would also find `constructor` and friends on the
// prototype, and the unit comes straight off the wire. Redis 6.2 compares
// with `strcmp`, 7.0+ with `strcasecmp` (`geo.unit-case-folding`).
export function isSupportedGeoUnit(
  unit: string,
  profile: CompatibilityProfile,
): boolean {
  return Object.hasOwn(
    UNIT_TO_METERS,
    profile.has('geo.unit-case-folding') ? asciiLowerCase(unit) : unit,
  )
}

// `unit` has passed isSupportedGeoUnit(): folding is a no-op on 6.2.
export function metersToUnit(meters: number, unit: string): number {
  return meters / UNIT_TO_METERS[asciiLowerCase(unit)]!
}

export function unitToMeters(value: number, unit: string): number {
  return value * UNIT_TO_METERS[asciiLowerCase(unit)]!
}

export function assertValidCoordinates(lon: number, lat: number): void {
  if (
    lon < GEO_LON_MIN ||
    lon > GEO_LON_MAX ||
    lat < GEO_LAT_MIN ||
    lat > GEO_LAT_MAX
  ) {
    throw errors.invalidLongitudeLatitude(lon, lat)
  }
}

// Bit-spreads a 26-bit value into the even bit positions of a 52-bit result
// (Redis' geohash.c interleave64, truncated to the 26-bit step this repo uses).
function interleave64(latBits: bigint, lonBits: bigint): bigint {
  const B = [
    0x5555555555555555n,
    0x3333333333333333n,
    0x0f0f0f0f0f0f0f0fn,
    0x00ff00ff00ff00ffn,
    0x0000ffff0000ffffn,
  ]
  const S = [1n, 2n, 4n, 8n, 16n]
  let x = latBits
  let y = lonBits
  for (let i = 4; i >= 0; i--) {
    x = (x | (x << S[i]!)) & B[i]!
    y = (y | (y << S[i]!)) & B[i]!
  }
  return x | (y << 1n)
}

function deinterleave64(bits: bigint): bigint {
  const B = [
    0x5555555555555555n,
    0x3333333333333333n,
    0x0f0f0f0f0f0f0f0fn,
    0x00ff00ff00ff00ffn,
    0x0000ffff0000ffffn,
    0x00000000ffffffffn,
  ]
  const S = [0n, 1n, 2n, 4n, 8n, 16n]
  let x = bits & B[0]!
  for (let i = 1; i <= 5; i++) {
    x = (x | (x >> S[i]!)) & B[i]!
  }
  return x
}

function encodeBits(
  lon: number,
  lat: number,
  lonMin: number,
  lonMax: number,
  latMin: number,
  latMax: number,
): bigint {
  const lonOffset = (lon - lonMin) / (lonMax - lonMin)
  const latOffset = (lat - latMin) / (latMax - latMin)
  const lonBits = BigInt(Math.floor(lonOffset * CELLS))
  const latBits = BigInt(Math.floor(latOffset * CELLS))
  return interleave64(latBits, lonBits)
}

function decodeBits(
  bits: bigint,
  lonMin: number,
  lonMax: number,
  latMin: number,
  latMax: number,
): { lon: number; lat: number } {
  const latBits = deinterleave64(bits)
  const lonBits = deinterleave64(bits >> 1n)

  const latLo = latMin + (Number(latBits) / CELLS) * (latMax - latMin)
  const latHi = latMin + (Number(latBits + 1n) / CELLS) * (latMax - latMin)
  const lonLo = lonMin + (Number(lonBits) / CELLS) * (lonMax - lonMin)
  const lonHi = lonMin + (Number(lonBits + 1n) / CELLS) * (lonMax - lonMin)

  return { lon: (lonLo + lonHi) / 2, lat: (latLo + latHi) / 2 }
}

// The stored zset score: a 52-bit geohash interleaving lon/lat over Redis'
// internal ranges (lat clamped to ±85.05112878 so the projection stays square).
export function encodeGeoScore(lon: number, lat: number): number {
  return Number(
    encodeBits(lon, lat, GEO_LON_MIN, GEO_LON_MAX, GEO_LAT_MIN, GEO_LAT_MAX),
  )
}

export function decodeGeoScore(score: number): { lon: number; lat: number } {
  return decodeBits(
    BigInt(score),
    GEO_LON_MIN,
    GEO_LON_MAX,
    GEO_LAT_MIN,
    GEO_LAT_MAX,
  )
}

// GEOHASH renders the standard geohash.org string, which uses a ±90 latitude
// range rather than the ±85.05112878 range the zset score is stored with. Real
// Redis re-derives it by decoding the stored score with the real range, then
// re-encoding that position with the standard range (see geohashCommand in
// geo.c) — encoding straight from the original lon/lat produces a different
// string because decode-then-reencode loses the sub-cell fraction.
export function geohashString(score: number): string {
  const pos = decodeGeoScore(score)
  const bits = encodeBits(pos.lon, pos.lat, GEO_LON_MIN, GEO_LON_MAX, -90, 90)

  let out = ''
  for (let i = 0; i < 10; i++) {
    const idx = Number((bits >> BigInt(52 - i * 5 - 5)) & 0x1fn)
    out += GEO_ALPHABET[idx]
  }
  // The 52-bit hash only covers 10 full 5-bit groups (50 bits); Redis pads
  // the unused 11th character with a literal '0' rather than the leftover 2 bits.
  return out + '0'
}

// geohash_helper.c `deg_rad(a) = a * D_R` with `D_R = M_PI / 180.0`. Not
// `a * M_PI / 180`, which rounds differently and changes STOREDIST digits.
const D_R = Math.PI / 180.0

function degRad(degrees: number): number {
  return degrees * D_R
}

/** geohashGetLatDistance(): the distance between two latitudes on a meridian. */
export function geoLatDistanceMeters(lat1: number, lat2: number): number {
  return EARTH_RADIUS_M * Math.abs(degRad(lat2) - degRad(lat1))
}

/**
 * Haversine great-circle distance in meters, ported from Redis'
 * geohashGetDistance() operation for operation so the doubles match: a
 * same-longitude pair takes the latitude-only shortcut where the profile's
 * server has it (`geo.distance-same-lon-shortcut`, `geo.distance-lon-epsilon`).
 */
export function geoDistanceMeters(
  lon1: number,
  lat1: number,
  lon2: number,
  lat2: number,
  profile: CompatibilityProfile,
): number {
  const lon1r = degRad(lon1)
  const lon2r = degRad(lon2)
  const v = Math.sin((lon2r - lon1r) / 2)
  const sameLon = profile.has('geo.distance-lon-epsilon')
    ? Math.abs(v) <= 1e-15
    : profile.has('geo.distance-same-lon-shortcut') && v === 0
  if (sameLon) return geoLatDistanceMeters(lat1, lat2)
  const lat1r = degRad(lat1)
  const lat2r = degRad(lat2)
  const u = Math.sin((lat2r - lat1r) / 2)
  const a = u * u + Math.cos(lat1r) * Math.cos(lat2r) * v * v
  return 2.0 * EARTH_RADIUS_M * Math.asin(Math.sqrt(a))
}
