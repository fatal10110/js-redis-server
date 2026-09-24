import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  defineCommand,
  type CommandDefinition,
} from '../src/core/command-definition'
import { t } from '../src/core/command-schema'
import { RedisResult } from '../src/core/redis-result'
import { encodeRedisValue } from '../src/core/resp-encoder'
import { createRedisSessionHarness } from './core-session-test-helpers'

// A user-added command whose keys come only from `keys(args)`: no key specs,
// getkeys procedure or key positions in its schema. Real Redis cannot load
// one, so this is mock-only; the keyless built-ins that take the same path
// are pinned against real Redis in the raw-tcp GETKEYS matrix (#518).
const mykeys = defineCommand({
  name: 'mykeys',
  schema: t.object({ count: t.integer(), args: t.variadic(t.bulk()) }),
  flags: ['readonly'],
  keys: args => args.args.slice(0, args.count),
  execute: () => RedisResult.ok(),
}) as CommandDefinition<unknown>

// A user-added container whose subcommand entry declares no key specs.
const mybox = defineCommand({
  name: 'mybox',
  schema: t.object({ subcommand: t.bulk(), args: t.variadic(t.bulk()) }),
  flags: ['write'],
  introspection: {
    arity: -2,
    subcommands: [{ name: 'mybox|get', arity: -3 }],
  },
  keys: args => args.args.slice(0, 1),
  execute: () => RedisResult.ok(),
}) as CommandDefinition<unknown>

async function wire(args: string[]): Promise<string> {
  const { session } = createRedisSessionHarness({
    extraCommands: [mykeys, mybox],
  })
  const result = await session.execute(
    'command',
    args.map(arg => Buffer.from(arg)),
  )
  return encodeRedisValue(result.value, 2).toString()
}

describe('COMMAND GETKEYS on a user-added command', () => {
  test('answers the keys its keys(args) returns', async () => {
    assert.strictEqual(
      await wire(['GETKEYS', 'MYKEYS', '2', 'a', 'b', 'c']),
      '*2\r\n$1\r\na\r\n$1\r\nb\r\n',
    )
    assert.strictEqual(
      await wire(['GETKEYSANDFLAGS', 'MYKEYS', '1', 'a']),
      '*1\r\n*2\r\n$1\r\na\r\n*2\r\n+RO\r\n+access\r\n',
    )
    assert.strictEqual(
      await wire(['GETKEYS', 'MYBOX', 'GET', 'k']),
      '*1\r\n$1\r\nk\r\n',
    )
  })

  test('has no key arguments when there are none or the call does not parse', async () => {
    const noKeys = '-ERR The command has no key arguments\r\n'
    assert.strictEqual(await wire(['GETKEYS', 'MYKEYS', '0', 'a']), noKeys)
    assert.strictEqual(await wire(['GETKEYS', 'MYKEYS', 'x']), noKeys)
  })
})
