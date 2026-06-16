import { MinMaxNotFloatError } from '../../core/redis-error'

export type ScoreBound = { value: number; exclusive: boolean }

export function parseScoreBoundArg(s: string): ScoreBound {
  const exclusive = s.startsWith('(')
  const raw = exclusive ? s.slice(1) : s

  if (raw.length === 0) throw new MinMaxNotFloatError()

  const normalized = raw.toLowerCase()
  if (normalized === '+inf') return { value: Infinity, exclusive }
  if (normalized === '-inf') return { value: -Infinity, exclusive }

  const n = Number(raw)
  if (!Number.isFinite(n)) throw new MinMaxNotFloatError()
  return { value: n, exclusive }
}

export function scoreWithinBounds(
  score: number,
  min: ScoreBound,
  max: ScoreBound,
) {
  if (min.exclusive ? score <= min.value : score < min.value) return false
  if (max.exclusive ? score >= max.value : score > max.value) return false
  return true
}
