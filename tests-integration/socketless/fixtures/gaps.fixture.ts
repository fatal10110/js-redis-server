import type { KnownGap } from '../known-gaps'

/** Known-gaps list for `listed.fixture.ts` (see `tests/socketless-register.test.ts`). */
export const SOCKETLESS_KNOWN_GAPS: readonly KnownGap[] = [
  {
    file: 'socketless/fixtures/listed.fixture.ts',
    reason: 'fixture: the facade has no `mGet()`',
    error: /\bmGet is not a function/,
    test: ['listed'],
  },
]
