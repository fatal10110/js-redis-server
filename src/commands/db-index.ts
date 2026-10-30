import { t } from '../core/command-schema'
import { errors } from '../core/redis-error'
import { parseInt64Token } from './helpers'

const INT32_MIN = -2147483648n
const INT32_MAX = 2147483647n

/**
 * A database index read as real Redis' `getIntFromObjectOrReply` does for
 * SELECT, MOVE and COPY ... DB: an int64 token (else the integer error), then
 * a C int (else the int32 range error). The `0 .. databases - 1` check is the
 * command's own, so a negative index answers `DB index is out of range`
 * (#571).
 */
export function parseDbIndexToken(token: Buffer): number {
  const value = parseInt64Token(token)
  if (value < INT32_MIN || value > INT32_MAX) {
    throw errors.int32OutOfRange()
  }
  return Number(value)
}

/** One database-index argument, parsed by `parseDbIndexToken`. */
export const dbIndexSchema = t.custom<number>(
  { min: 1, max: 1 },
  (input, index, ctx) => {
    const { value, nextIndex } = t.bulk().parse(input, index, ctx)
    return { value: parseDbIndexToken(value), nextIndex }
  },
)
