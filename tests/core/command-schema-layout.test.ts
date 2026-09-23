import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  schemaArity,
  schemaKeyRange,
  schemaLayout,
  t,
} from '../../src/core/command-schema'
import { keySpecsKeyRange } from '../../src/commands/command'
import { createRedisCommandExecutor } from '../../src/internal'

describe('schema layout (#370)', () => {
  test('arity counts the command name and negates open-ended schemas', () => {
    assert.strictEqual(schemaArity(t.object({})), 1)
    assert.strictEqual(schemaArity(t.object({ key: t.key() })), 2)
    assert.strictEqual(
      schemaArity(
        t.object({ key: t.key(), values: t.variadic(t.bulk(), { min: 1 }) }),
      ),
      -3,
    )
    assert.strictEqual(
      schemaArity(t.object({ key: t.key(), count: t.optional(t.integer()) })),
      -2,
    )
    assert.strictEqual(
      schemaArity(
        t.variadic(t.object({ field: t.bulk(), value: t.bulk() }), { min: 1 }),
      ),
      -3,
    )
    assert.strictEqual(
      schemaArity(t.custom(() => ({ value: 0, nextIndex: 0 }))),
      -1,
    )
    assert.strictEqual(
      schemaArity(
        t.custom({ min: 2, max: 2 }, () => ({ value: 0, nextIndex: 0 })),
      ),
      3,
    )
  })

  test('key range follows Redis legacy first/last/step rules', () => {
    const range = (schema: Parameters<typeof schemaKeyRange>[0]) => {
      const { firstKey, lastKey, keyStep } = schemaKeyRange(schema)
      return [firstKey, lastKey, keyStep]
    }

    assert.deepStrictEqual(range(t.object({ message: t.bulk() })), [0, 0, 0])
    assert.deepStrictEqual(range(t.object({ key: t.key() })), [1, 1, 1])
    assert.deepStrictEqual(
      range(t.object({ source: t.key(), destination: t.key() })),
      [1, 2, 1],
    )
    assert.deepStrictEqual(
      range(t.object({ keys: t.variadic(t.key(), { min: 1 }) })),
      [1, -1, 1],
    )
    assert.deepStrictEqual(
      range(t.variadic(t.object({ key: t.key(), value: t.bulk() }))),
      [1, -1, 2],
    )
    // A fixed key followed by a range that continues it.
    assert.deepStrictEqual(
      range(
        t.object({
          operation: t.bulk(),
          destination: t.key(),
          sources: t.variadic(t.key(), { min: 1 }),
        }),
      ),
      [2, -1, 1],
    )
    // A gap ends the range, and nothing after a variable-width argument counts.
    assert.deepStrictEqual(
      range(t.object({ a: t.key(), gap: t.bulk(), b: t.key() })),
      [1, 1, 1],
    )
    assert.deepStrictEqual(
      range(t.object({ flag: t.optional(t.keyword('NX')), key: t.key() })),
      [0, 0, 0],
    )
  })

  test('key specs fold into the legacy range like Redis', () => {
    const spec = (begin: number, lastKey: number, keyStep: number) => ({
      flags: [],
      beginSearchIndex: begin,
      lastKey,
      keyStep,
    })

    assert.deepStrictEqual(keySpecsKeyRange([spec(1, 0, 1)]), {
      firstKey: 1,
      lastKey: 1,
      keyStep: 1,
    })
    assert.deepStrictEqual(keySpecsKeyRange([spec(1, -1, 2)]), {
      firstKey: 1,
      lastKey: -1,
      keyStep: 2,
    })
    // RENAME-style: two consecutive single-key specs merge.
    assert.deepStrictEqual(keySpecsKeyRange([spec(1, 0, 1), spec(2, 0, 1)]), {
      firstKey: 1,
      lastKey: 2,
      keyStep: 1,
    })
    // A non-consecutive spec is left out.
    assert.deepStrictEqual(keySpecsKeyRange([spec(1, 0, 1), spec(3, 0, 1)]), {
      firstKey: 1,
      lastKey: 1,
      keyStep: 1,
    })
  })

  test('declared key specs agree with the schema key positions', () => {
    const executor = createRedisCommandExecutor()
    let checked = 0
    for (const definition of executor.getCommandDefinitions()) {
      const keySpecs = definition.introspection?.keySpecs ?? []
      if (keySpecs.length === 0) {
        continue
      }

      checked++
      assert.deepStrictEqual(
        keySpecsKeyRange(keySpecs),
        schemaKeyRange(definition.schema),
        definition.name,
      )
    }

    assert.ok(checked > 0)
  })
})

describe('schema layout composition (#370 review)', () => {
  const parse = () => ({ value: null, nextIndex: 0 })
  const range = (schema: Parameters<typeof schemaKeyRange>[0]) => {
    const { firstKey, lastKey, keyStep } = schemaKeyRange(schema)
    return [firstKey, lastKey, keyStep]
  }
  const trailingKeys = () =>
    t.custom({ min: 1, keyRange: { start: 0, step: 1, last: -1 } }, parse)

  test('a hand-built { parse } schema is opaque, not a crash', () => {
    const handBuilt = { parse }
    assert.strictEqual(schemaArity(handBuilt), -1)
    assert.deepStrictEqual(range(handBuilt), [0, 0, 0])
    assert.deepStrictEqual(schemaLayout(handBuilt), {
      min: 0,
      max: Infinity,
      keys: [],
    })

    // The combinators accept it and treat it as any number of tokens.
    const object = t.object({ key: t.key(), rest: handBuilt })
    assert.strictEqual(schemaArity(object), -2)
    assert.deepStrictEqual(range(object), [1, 1, 1])
    assert.strictEqual(schemaArity(t.optional(handBuilt)), -1)
    assert.strictEqual(schemaArity(t.variadic(handBuilt)), -1)
    assert.strictEqual(schemaArity(t.union([handBuilt, t.key()])), -1)
    assert.strictEqual(schemaArity(t.withLayout(handBuilt, { min: 2 })), -3)
  })

  test('a key range holds only while nothing can follow it', () => {
    assert.deepStrictEqual(
      range(t.object({ key: t.key(), rest: trailingKeys() })),
      [1, -1, 1],
    )
    // An empty object after the range takes no tokens, so the range survives.
    assert.deepStrictEqual(
      range(t.object({ keys: trailingKeys(), none: t.object({}) })),
      [1, -1, 1],
    )
    // Any field that may take a token after it shifts `last`: the range goes.
    assert.deepStrictEqual(
      range(t.object({ keys: trailingKeys(), timeout: t.integer() })),
      [0, 0, 0],
    )
    assert.deepStrictEqual(
      range(
        t.object({ keys: trailingKeys(), flag: t.optional(t.keyword('X')) }),
      ),
      [0, 0, 0],
    )
    // A repeated item that carries its own range cannot repeat as one.
    assert.deepStrictEqual(range(t.variadic(trailingKeys())), [0, 0, 0])
  })

  test('union keeps what every branch shares', () => {
    const union = t.union([
      t.object({ key: t.key(), a: t.bulk() }),
      t.object({ key: t.key(), a: t.bulk(), b: t.key(), c: t.bulk() }),
    ])
    assert.deepStrictEqual(schemaLayout(union), {
      min: 2,
      max: 4,
      keys: [0],
      keyRange: undefined,
    })

    const ranges = t.union([
      t.variadic(t.key(), { min: 1 }),
      t.variadic(t.key(), { min: 2 }),
    ])
    assert.deepStrictEqual(range(ranges), [1, -1, 1])
    assert.strictEqual(schemaArity(ranges), -2)

    const differentRanges = t.union([
      t.variadic(t.key()),
      t.variadic(t.object({ key: t.key(), value: t.bulk() })),
    ])
    assert.deepStrictEqual(range(differentRanges), [0, 0, 0])
    assert.deepStrictEqual(schemaLayout(t.union([])), {
      min: 0,
      max: 0,
      keys: [],
    })
  })

  test('withLayout overrides only what it declares', () => {
    const base = t.custom({ min: 1, keys: [0] }, parse)
    const layout = schemaLayout(t.withLayout(base, { min: 3, max: 3 }))
    assert.deepStrictEqual(
      [layout.min, layout.max, layout.keys, layout.keyRange],
      [3, 3, [0], undefined],
    )
    // The original schema is untouched.
    assert.strictEqual(schemaArity(base), -2)
  })

  test('a declared layout is validated', () => {
    const invalid = (layout: Parameters<typeof t.custom>[0]) =>
      assert.throws(
        () => t.custom(layout as never, parse),
        /Invalid schema layout/,
        JSON.stringify(layout),
      )

    invalid({ min: 2, keys: [1, 0] })
    invalid({ min: 2, keys: [0, 0] })
    invalid({ min: 1, keys: [1] })
    invalid({ min: 3, max: 2 })
    invalid({ min: -1 })
    invalid({ keyRange: { start: 0, step: 0, last: -1 } })
    invalid({ keyRange: { start: 0, step: 1, last: 0 } })
    assert.throws(
      () => t.withLayout(t.key(), { min: 0 }),
      /Invalid schema layout/,
    )
    assert.doesNotThrow(() => t.custom({ min: 2, keys: [0, 1] }, parse))
  })
})
