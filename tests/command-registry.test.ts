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
    const command = makeCommand('get')

    registry.register(command)

    assert.strictEqual(registry.get('get'), command)
    assert.strictEqual(registry.get('GET'), command)
    assert.strictEqual(registry.get('Get'), command)
    assert.strictEqual(registry.get('nope'), undefined)
  })

  test('register is the one place a command name is lowercased', () => {
    const registry = new CommandRegistry()

    registry.register(makeCommand('GeT'))

    // The definition kept its declared casing, but everything the registry
    // hands back — and therefore every policy matching on `definition.name` —
    // sees the normalized form.
    assert.strictEqual(registry.get('get')?.name, 'get')
    assert.strictEqual(registry.get('GET')?.name, 'get')
    assert.deepStrictEqual(
      registry.getAll().map(definition => definition.name),
      ['get'],
    )
  })

  test('rejects duplicate registration unless override is explicit', () => {
    const registry = new CommandRegistry()
    const first = makeCommand('get')
    const replacement = makeCommand('GET', ['write'])

    registry.register(first)

    assert.throws(() => registry.register(replacement), /already registered/)

    registry.register(replacement, { override: true })
    assert.deepStrictEqual(registry.get('get')?.flags, ['write'])
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
