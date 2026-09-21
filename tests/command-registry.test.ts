import { test, describe } from 'node:test'
import assert from 'node:assert'
import {
  CommandRegistry,
  RedisResult,
  RedisValue,
  defineCommand,
  t,
} from '../src/internal'
import type { CommandDefinition, CommandFlag } from '../src/internal'

function makeCommand(
  name: string,
  flags: readonly CommandFlag[] = ['readonly'],
): CommandDefinition<Record<string, never>> {
  return defineCommand({
    name,
    schema: t.object({}),
    flags,
    keys: () => [],
    execute: () => RedisResult.create(RedisValue.simpleString(name)),
  })
}

describe('CommandRegistry', () => {
  test('registers and retrieves commands case-insensitively', () => {
    const registry = new CommandRegistry()
    const command = makeCommand('GET')

    registry.register(command)

    assert.strictEqual(registry.get('get'), command)
    assert.strictEqual(registry.get('GET'), command)
    assert.strictEqual(registry.get('Get'), command)
    assert.strictEqual(registry.get('nope'), undefined)
  })

  test('defineCommand lowercases the declared name', () => {
    assert.strictEqual(makeCommand('GeT').name, 'get')
    assert.strictEqual(makeCommand('get').name, 'get')
  })

  test('register stores the definition by reference, never a copy', () => {
    // Regression guard: `CommandDefinition` is an interface, so a class
    // instance is a legal definition. A registry that spread-copied its input
    // would strip the prototype and lose `keys`/`execute` entirely, and would
    // break identity for anything keying metadata off the definition object.
    // The name is deliberately mixed-case: a registry that normalized by
    // copying would take its copy branch here and nowhere else, since every
    // in-repo definition already declares a lowercase name.
    class ClassCommand implements CommandDefinition<Record<string, never>> {
      readonly name = 'ClassCmd'
      readonly schema = t.object({})
      readonly flags: readonly CommandFlag[] = ['readonly']
      keys(): readonly Buffer[] {
        return []
      }
      execute() {
        return RedisResult.create(RedisValue.simpleString('CLASS'))
      }
    }

    const registry = new CommandRegistry()
    const definition = new ClassCommand()
    const metadata = new WeakMap<CommandDefinition<never>, string>()
    metadata.set(definition, 'policy-config')

    registry.register(definition)

    const stored = registry.get('classcmd')
    assert.strictEqual(stored, definition)
    // register files it under the lowercased key but leaves the object — and
    // therefore its `name` — exactly as handed in.
    assert.strictEqual(stored?.name, 'ClassCmd')
    assert.strictEqual(typeof stored?.keys, 'function')
    assert.deepStrictEqual(stored?.keys({}), [])
    assert.strictEqual(
      metadata.get(stored as unknown as CommandDefinition<never>),
      'policy-config',
    )
    assert.strictEqual(registry.getAll()[0], definition)
  })

  test('rejects duplicate registration unless override is explicit', () => {
    const registry = new CommandRegistry()
    const first = makeCommand('get')
    const replacement = makeCommand('GET', ['write'])

    registry.register(first)

    assert.throws(() => registry.register(replacement), /already registered/)

    registry.register(replacement, { override: true })
    assert.strictEqual(registry.get('get'), replacement)
  })

  test('registerAll preserves registered commands and names', () => {
    const registry = new CommandRegistry()
    const get = makeCommand('get')
    const set = makeCommand('set', ['write'])

    registry.registerAll([get, set])

    assert.deepStrictEqual(registry.getAll(), [get, set])
    assert.deepStrictEqual(
      registry.getAll().map(definition => definition.name),
      ['get', 'set'],
    )
  })
})
