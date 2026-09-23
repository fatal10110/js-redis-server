import { test, describe } from 'node:test'
import assert from 'node:assert'
import shim, { process as namedShim } from './process-shim'

// process-shim.ts imports the plugin's *browser* `process` object by relative
// path — the same module the demo bundle gets — which has no hrtime of its
// own. So under node:test this exercises the patch itself, not Node's process.
// If this suite breaks, the Pages demo renders an empty terminal: clock.ts
// calls process.hrtime.bigint() at module load.
describe('process shim', () => {
  test("wraps the plugin's browser process object, not Node's", () => {
    assert.notStrictEqual(shim, globalThis.process)
    assert.strictEqual(namedShim, shim)
    assert.strictEqual(typeof shim.nextTick, 'function')
  })

  test('hrtime.bigint() exists and returns a bigint', () => {
    assert.strictEqual(typeof shim.hrtime.bigint, 'function')
    assert.strictEqual(typeof shim.hrtime.bigint(), 'bigint')
  })

  test('hrtime.bigint() is non-decreasing and counts nanoseconds', async () => {
    let previous = shim.hrtime.bigint()
    for (let i = 0; i < 1000; i++) {
      const next = shim.hrtime.bigint()
      assert.ok(next >= previous, `sample ${i} went backwards`)
      previous = next
    }
    const before = shim.hrtime.bigint()
    await new Promise(resolve => setTimeout(resolve, 5))
    const elapsed = shim.hrtime.bigint() - before
    // ~5ms; loose bounds, it only has to be the right unit.
    assert.ok(elapsed >= 1_000_000n && elapsed < 5_000_000_000n, `${elapsed}`)
  })

  test('bare hrtime() is a [seconds, nanoseconds] tuple', () => {
    const [seconds, nanos] = shim.hrtime()
    assert.ok(Number.isInteger(seconds) && seconds >= 0)
    assert.ok(Number.isInteger(nanos) && nanos >= 0 && nanos < 1e9)
  })
})
