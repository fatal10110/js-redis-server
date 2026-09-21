// Unit-tests the comparison that `export-surface.test.ts` depends on.
//
// Every assertion in that file is `deepStrictEqual(diff, [])`, which passes
// just as happily when the diff is broken as when the surface is intact. That
// is the vacuous-guard failure the whole change exists to prevent: invert one
// `includes` and the suite goes permanently green while the next `/core`
// deletion ships exactly like #374/#376/#377 did.
//
// So the diff is pinned here against synthetic surfaces. No `dist/`, no build —
// pure `(before, after) -> string[]`.

import { test, describe } from 'node:test'
import assert from 'node:assert'
import {
  findAdditions,
  findRemovals,
  type ExportSurface,
} from './export-surface.js'

const BASELINE: ExportSurface = {
  CommandFlag: { kind: 'type', variants: ['admin', 'noscript', 'readonly'] },
  CommandRegistry: {
    kind: 'value',
    members: ['get', 'getAll', 'has', 'register'],
  },
  RedisResult: {
    kind: 'value',
    members: ['options', 'static:error', 'static:ok', 'value'],
  },
  RedisTurnQueue: { kind: 'type' },
  SerialTurnQueue: { kind: 'value', members: ['waitTurn'] },
}

/** `BASELINE` with one targeted edit applied. */
function mutate(edit: (draft: ExportSurface) => void): ExportSurface {
  const draft: ExportSurface = JSON.parse(
    JSON.stringify(BASELINE),
  ) as ExportSurface
  edit(draft)
  return draft
}

describe('findRemovals', () => {
  test('reports nothing when the surface is unchanged', () => {
    assert.deepStrictEqual(
      findRemovals(
        BASELINE,
        mutate(() => {}),
      ),
      [],
    )
  })

  test('names a deleted symbol and its kind', () => {
    const gone = findRemovals(
      BASELINE,
      mutate(draft => {
        delete draft.RedisTurnQueue
        delete draft.SerialTurnQueue
      }),
    )

    assert.deepStrictEqual(gone, [
      'RedisTurnQueue (type) — gone',
      'SerialTurnQueue (value) — gone',
    ])
  })

  test('names a deleted member of a surviving symbol', () => {
    // The #377 shape: `CommandRegistry` stays, three of its methods do not.
    const gone = findRemovals(
      BASELINE,
      mutate(draft => {
        draft.CommandRegistry = { kind: 'value', members: ['get', 'register'] }
      }),
    )

    assert.deepStrictEqual(gone, [
      'CommandRegistry.getAll — member gone',
      'CommandRegistry.has — member gone',
    ])
  })

  test('distinguishes a deleted static from the instance member', () => {
    const gone = findRemovals(
      BASELINE,
      mutate(draft => {
        draft.RedisResult = {
          kind: 'value',
          members: ['options', 'static:ok', 'value'],
        }
      }),
    )

    assert.deepStrictEqual(gone, ['RedisResult.static:error — member gone'])
  })

  test('names a deleted union variant', () => {
    const gone = findRemovals(
      BASELINE,
      mutate(draft => {
        draft.CommandFlag = { kind: 'type', variants: ['admin', 'readonly'] }
      }),
    )

    assert.deepStrictEqual(gone, [
      "CommandFlag — union variant 'noscript' gone",
    ])
  })

  test('reports a value export downgraded to type-only', () => {
    const gone = findRemovals(
      BASELINE,
      mutate(draft => {
        draft.SerialTurnQueue = { kind: 'type', members: ['waitTurn'] }
      }),
    )

    assert.deepStrictEqual(gone, [
      'SerialTurnQueue — was a value export, is now type-only (no runtime binding)',
    ])
  })

  test('treats a member losing its whole list as removal, not as unchanged', () => {
    // Guards the `members ?? []` fallbacks: an entry that drops its `members`
    // key entirely must not read as "nothing removed".
    const gone = findRemovals(
      BASELINE,
      mutate(draft => {
        draft.SerialTurnQueue = { kind: 'value' }
      }),
    )

    assert.deepStrictEqual(gone, ['SerialTurnQueue.waitTurn — member gone'])
  })

  test('a rename is reported as both a removal and an addition', () => {
    const renamed = mutate(draft => {
      delete draft.SerialTurnQueue
      draft.RedisSerialTurnQueue = { kind: 'value', members: ['waitTurn'] }
    })

    assert.deepStrictEqual(findRemovals(BASELINE, renamed), [
      'SerialTurnQueue (value) — gone',
    ])
    assert.deepStrictEqual(findAdditions(BASELINE, renamed), [
      'RedisSerialTurnQueue (value)',
    ])
  })

  test('a pure addition is not a removal', () => {
    const gone = findRemovals(
      BASELINE,
      mutate(draft => {
        draft.PubSubKind = { kind: 'type', variants: ['channel', 'pattern'] }
        draft.CommandRegistry = {
          kind: 'value',
          members: ['get', 'getAll', 'has', 'register', 'registerAll'],
        }
      }),
    )

    assert.deepStrictEqual(gone, [])
  })
})

describe('findAdditions', () => {
  test('reports nothing when the surface is unchanged', () => {
    assert.deepStrictEqual(
      findAdditions(
        BASELINE,
        mutate(() => {}),
      ),
      [],
    )
  })

  test('names a new symbol, member and variant', () => {
    const added = findAdditions(
      BASELINE,
      mutate(draft => {
        draft.PubSubKind = { kind: 'type', variants: ['channel'] }
        draft.CommandRegistry = {
          kind: 'value',
          members: ['get', 'getAll', 'has', 'register', 'registerAll'],
        }
        draft.CommandFlag = {
          kind: 'type',
          variants: ['admin', 'noscript', 'readonly', 'write'],
        }
      }),
    )

    assert.deepStrictEqual(added, [
      "CommandFlag — union variant 'write'",
      'CommandRegistry.registerAll',
      'PubSubKind (type)',
    ])
  })

  test('a removal is not an addition', () => {
    const added = findAdditions(
      BASELINE,
      mutate(draft => {
        delete draft.SerialTurnQueue
        draft.CommandRegistry = { kind: 'value', members: ['get'] }
      }),
    )

    assert.deepStrictEqual(added, [])
  })

  test('reports a type-only export that gained a runtime binding', () => {
    // Not breaking in itself, but leaving it unreported disarms the downgrade
    // check permanently: nobody refreshes, the baseline keeps `kind: "type"`,
    // and when the binding is later removed, baseline `type` vs current `type`
    // reports nothing while the consumer's `import { … }` breaks at runtime.
    const upgraded = mutate(draft => {
      draft.RedisTurnQueue = { kind: 'value' }
    })

    assert.deepStrictEqual(findAdditions(BASELINE, upgraded), [
      'RedisTurnQueue — is now a value export (was type-only)',
    ])
    assert.deepStrictEqual(findRemovals(BASELINE, upgraded), [])
  })

  test('the downgrade check survives a refreshed upgrade', () => {
    // End to end: upgrade is reported, baseline is refreshed, the later removal
    // of the binding is then caught as BREAKING.
    const upgraded = mutate(draft => {
      draft.RedisTurnQueue = { kind: 'value' }
    })
    const removedAgain = mutate(draft => {
      draft.RedisTurnQueue = { kind: 'type' }
    })

    assert.deepStrictEqual(findRemovals(upgraded, removedAgain), [
      'RedisTurnQueue — was a value export, is now type-only (no runtime binding)',
    ])
  })

  test('an empty baseline reports the whole surface as added', () => {
    // The "no baseline for this entry" path: it must not read as "all clear".
    const added = findAdditions({}, BASELINE)

    assert.strictEqual(added.length, Object.keys(BASELINE).length)
  })
})
