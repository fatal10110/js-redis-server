import { describe, test } from 'node:test'
import assert from 'node:assert'
import { constants as bufferConstants } from 'node:buffer'

import { MAX_MATERIALISABLE_LENGTH } from '../src/commands/strings'

/**
 * Bounds on the string-allocation ceiling.
 *
 * These are asserted on the constant rather than over the wire on purpose. The
 * only wire-level probe that would distinguish a materialisability ceiling from
 * a representability one is an offset around 1e12: `Buffer.alloc` *accepts* it
 * (lazy mapping, ~11ms) and the process is then SIGKILLed during zero-fill. A
 * test that sends it does not fail on a regression, it takes the runner down —
 * and no ordering fixes that, since there is no assertion outcome to abort on.
 * So the wire test in tests-integration/raw-tcp/proto-max-bulk-len.test.ts
 * covers the refusal behaviour, and the invariant that makes the ceiling
 * meaningful is pinned here, where nothing is allocated at all.
 */
describe('string allocation ceiling', () => {
  test('is small enough for the process to materialise', () => {
    // The guard rail against someone "just raising the limit": anything up to
    // 1GiB a Node process can genuinely back, and the lazily-mapped danger zone
    // starts orders of magnitude above that.
    assert.ok(
      MAX_MATERIALISABLE_LENGTH <= 1073741824n,
      `ceiling ${MAX_MATERIALISABLE_LENGTH} is large enough that Buffer.alloc may map it lazily and the process be killed during zero-fill`,
    )
  })

  test('is at least Redis’ default proto-max-bulk-len, so the default configuration is untouched', () => {
    assert.ok(
      MAX_MATERIALISABLE_LENGTH >= 536870912n,
      `ceiling ${MAX_MATERIALISABLE_LENGTH} is below Redis' 512MB default, which would make APPEND/SETRANGE diverge from Redis out of the box`,
    )
  })

  test('is not buffer.constants.MAX_LENGTH, which Buffer.alloc will not serve', () => {
    // MAX_LENGTH is what Buffer.alloc accepts as an argument, not what it can
    // return: it throws at that value and across a wide band below it.
    assert.notStrictEqual(
      MAX_MATERIALISABLE_LENGTH,
      BigInt(bufferConstants.MAX_LENGTH),
    )
    assert.throws(
      () => Buffer.alloc(bufferConstants.MAX_LENGTH),
      RangeError,
      'buffer.constants.MAX_LENGTH is expected to be unservable; if this ever starts succeeding, revisit the ceiling rationale',
    )
  })
})
