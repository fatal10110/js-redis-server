/**
 * Fixture for `tests/socketless-register.test.ts` — not a suite (no
 * `.test.ts` suffix, so no glob picks it up). `gaps.fixture.ts` lists the
 * `listed` test as a todo expecting `mGet is not a function`; FIXTURE_MODE
 * decides how it actually ends.
 */
import { test } from 'node:test'

const mode = process.env.FIXTURE_MODE

// 'timeout' gives the test its own deadline, which node:test enforces in-process
// (as `--test-timeout` itself is on Node 24): the test is marked timed out and
// the file carries on to its root after() hook.
test('listed', { timeout: mode === 'timeout' ? 100 : undefined }, async () => {
  switch (mode) {
    case 'expected':
      throw new TypeError('redisClient.mGet is not a function')
    case 'unexpected':
      throw new Error('ERR something else entirely')
    case 'timeout':
      // Outlives the test's own 100ms timeout.
      await new Promise(resolve => setTimeout(resolve, 1000))
      return
    case 'cancel':
      // Never settles and holds nothing open: node:test cancels it.
      await new Promise(() => {})
      return
    case 'pass':
      return
    default:
      throw new Error(`unknown FIXTURE_MODE ${mode}`)
  }
})

test('unlisted', () => {})
