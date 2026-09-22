/**
 * `SORT` BY/GET pattern predicates shared by the command implementation
 * (`src/commands/keys.ts`) and the cluster guard
 * (`src/core/execution-policies/cluster-policy.ts`).
 *
 * They have to agree exactly: the guard decides which patterns are safe in
 * cluster mode on the assumption that the command dereferences precisely the
 * patterns the guard classified as globs.
 */

/**
 * The part of `SORT`'s parsed arguments the cluster guard reasons about.
 * It lives here rather than in `src/commands/keys.ts` so `src/core` never
 * names `src/commands` — not even in an erased `import type`, which would
 * leave the layering only half applied. `SortArgs` extends it, so the two
 * stay linked and a rename there is a compile error here.
 */
export type ClusterSortArgs = {
  key: Buffer
  by?: Buffer
  get: readonly Buffer[]
}

const ASTERISK = 0x2a
const NUL = 0x00
const HASH = 0x23

/**
 * Real Redis finds the wildcard with `strchr(spat, '*')` in
 * `lookupKeyByPattern()` and `sortCommand()`, so the search stops at the first
 * NUL even though keys and patterns are otherwise binary-safe.
 */
export function sortPatternWildcardIndex(pattern: Buffer): number {
  for (let i = 0; i < pattern.length; i++) {
    if (pattern[i] === NUL) {
      return -1
    }
    if (pattern[i] === ASTERISK) {
      return i
    }
  }
  return -1
}

/**
 * A pattern with no `*` is constant: it expands to the same key for every
 * element. Real Redis sets `dontsort` for such a `BY` and returns NULL from
 * `lookupKeyByPattern()` for such a `GET`, so neither one reads a key —
 * which is why the documented `BY nosort` is accepted in cluster mode.
 */
export function isConstantSortPattern(pattern: Buffer): boolean {
  return sortPatternWildcardIndex(pattern) === -1
}

/** `GET #` returns the sorted element itself and dereferences no key. */
export function isSelfSortPattern(pattern: Buffer): boolean {
  return pattern.length === 1 && pattern[0] === HASH
}
