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
