import { InvalidLongitudeLatitudeError } from '../../core/redis-error'

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

export function isSupportedGeoUnit(unit: string): boolean {
  return unit.toLowerCase() in UNIT_TO_METERS
}

export function metersToUnit(meters: number, unit: string): number {
  return meters / UNIT_TO_METERS[unit.toLowerCase()]!
}

export function unitToMeters(value: number, unit: string): number {
  return value * UNIT_TO_METERS[unit.toLowerCase()]!
}

export function assertValidCoordinates(lon: number, lat: number): void {
  if (
    lon < GEO_LON_MIN ||
    lon > GEO_LON_MAX ||
    lat < GEO_LAT_MIN ||
    lat > GEO_LAT_MAX
  ) {
    throw new InvalidLongitudeLatitudeError(lon, lat)
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

function toRadians(degrees: number): number {
  return (degrees * Math.PI) / 180
}

// Haversine great-circle distance in meters, matching Redis' geohashGetDistance.
export function haversineMeters(
  lon1: number,
  lat1: number,
  lon2: number,
  lat2: number,
): number {
  const lat1r = toRadians(lat1)
  const lat2r = toRadians(lat2)
  const u = Math.sin((toRadians(lat2) - toRadians(lat1)) / 2)
  const v = Math.sin((toRadians(lon2) - toRadians(lon1)) / 2)
  return (
    2.0 *
    EARTH_RADIUS_M *
    Math.asin(Math.sqrt(u * u + Math.cos(lat1r) * Math.cos(lat2r) * v * v))
  )
}
