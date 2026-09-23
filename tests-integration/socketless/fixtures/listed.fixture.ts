/**
 * Fixture for `tests/socketless-register.test.ts` — not a suite (no
 * `.test.ts` suffix, so no glob picks it up). `gaps.fixture.ts` lists the
 * `listed` test as a todo expecting `mGet is not a function`; FIXTURE_MODE
 * decides how it actually ends.
 */
import { test } from 'node:test'

const mode = process.env.FIXTURE_MODE

// The 'timeout' and 'late-*' modes give the test its own deadline, which
// node:test enforces in-process (as `--test-timeout` itself is on Node 24): the
// test is marked timed out and the file carries on to its root after() hook.
const timesOut = mode === 'timeout' || mode?.startsWith('late-')

test('listed', { timeout: timesOut ? 100 : undefined }, async () => {
  switch (mode) {
    case 'expected':
      throw new TypeError('redisClient.mGet is not a function')
    case 'unexpected':
      throw new Error('ERR something else entirely')
    case 'timeout':
      // Outlives the test's own 100ms timeout.
      await new Promise(resolve => setTimeout(resolve, 1000))
      return
    case 'late-expected':
      // Settles with the expected error, but only after the timeout ended it.
      await new Promise(resolve => setTimeout(resolve, 500))
      throw new TypeError('redisClient.mGet is not a function')
    case 'late-pass':
      // Returns normally, but only after the timeout ended it.
      await new Promise(resolve => setTimeout(resolve, 500))
      return
    case 'pass':
      return
    default:
      throw new Error(`unknown FIXTURE_MODE ${mode}`)
  }
})

// Keeps the file running past a late body's settle, so the late settle is
// seen before the root after() hook.
test('unlisted', async () => {
  if (mode?.startsWith('late-')) {
    await new Promise(resolve => setTimeout(resolve, 1500))
  }
})
