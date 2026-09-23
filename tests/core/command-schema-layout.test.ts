import { describe, test } from 'node:test'
import assert from 'node:assert'
import { schemaArity, schemaKeyRange, t } from '../../src/core/command-schema'
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
