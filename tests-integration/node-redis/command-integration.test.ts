import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import { errorWithMessage } from '../utils'

const testRunner = new TestRunner()

type CommandInfoReply = [
  string,
  number,
  string[],
  number,
  number,
  number,
  string[],
  string[],
  unknown[],
  unknown[],
]

describe(`COMMAND integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  const command = (args: string[]): Promise<unknown> =>
    redisClient.sendCommand(undefined, true, args)

  test('COMMAND INFO returns Redis command metadata and nulls for unknown commands', async () => {
    const reply = (await command([
      'COMMAND',
      'INFO',
      'GET',
      'SET',
      'NOSUCHCOMMAND',
    ])) as [CommandInfoReply, CommandInfoReply, null]

    assert.strictEqual(reply.length, 3)
    assertCommandInfo(reply[0], {
      name: 'get',
      arity: 2,
      flags: ['readonly', 'fast'],
      firstKey: 1,
      lastKey: 1,
      keyStep: 1,
      categories: ['@read', '@string', '@fast'],
    })
    assertCommandInfo(reply[1], {
      name: 'set',
      arity: -3,
      flags: ['write', 'denyoom'],
      firstKey: 1,
      lastKey: 1,
      keyStep: 1,
      categories: ['@write', '@string'],
    })
    assert.strictEqual(reply[2], null)
  })

  test('COMMAND LIST returns names and supports Redis FILTERBY variants', async () => {
    const names = (await command(['COMMAND', 'LIST'])) as string[]
    assert.ok(names.includes('get'))
    assert.ok(names.includes('set'))
    assert.ok(names.includes('command'))
    assert.ok(names.includes('command|info'))

    const patternNames = (await command([
      'COMMAND',
      'LIST',
      'FILTERBY',
      'PATTERN',
      'get*',
    ])) as string[]
    assert.ok(patternNames.includes('get'))
    assert.ok(patternNames.every(name => name.startsWith('get')))

    const moduleNames = (await command([
      'COMMAND',
      'LIST',
      'FILTERBY',
      'MODULE',
      'nosuchmodule',
    ])) as string[]
    assert.deepStrictEqual(moduleNames, [])
  })

  test('COMMAND COUNT and HELP expose the command surface', async () => {
    const names = (await command(['COMMAND', 'LIST'])) as string[]
    const count = (await command(['COMMAND', 'COUNT'])) as number
    const help = (await command(['COMMAND', 'HELP'])) as string[]

    assert.ok(count > 0)
    assert.ok(names.length > 0)
    assert.ok(help.some(line => line.includes('GETKEYS')))
    assert.ok(help.some(line => line.includes('GETKEYSANDFLAGS')))
  })

  test('COMMAND DOCS returns documentation entries and skips unknown commands', async () => {
    // node-redis decodes COMMAND DOCS (a RESP3 map) into an object keyed by
    // command name, each entry an object of doc fields.
    const docs = (await command(['COMMAND', 'DOCS', 'GET'])) as Record<
      string,
      Record<string, unknown>
    >
    assert.strictEqual(docs.get.summary, 'Returns the string value of a key.')
    assert.strictEqual(docs.get.group, 'string')

    const args = docs.get.arguments as Array<Record<string, unknown>>
    assert.strictEqual(args[0].name, 'key')

    const unknownDocs = await command(['COMMAND', 'DOCS', 'NOSUCHCOMMAND'])
    assert.deepStrictEqual(unknownDocs, {})
  })

  test('COMMAND DOCS argument flags use RESP status strings', async () => {
    const docs = (await command(['COMMAND', 'DOCS', 'MGET'])) as Record<
      string,
      Record<string, unknown>
    >
    const args = docs.mget.arguments as Array<Record<string, unknown>>
    assert.deepStrictEqual(args[0].flags, ['multiple'])
  })

  test('COMMAND GETKEYS and GETKEYSANDFLAGS use command key extraction', async () => {
    const tag = '{command-getkeys}'
    const keys = [`${tag}:a`, `${tag}:b`, `${tag}:c`]

    const getKeys = (await command([
      'COMMAND',
      'GETKEYS',
      'MGET',
      ...keys,
    ])) as string[]
    assert.deepStrictEqual(getKeys, keys)

    const keysAndFlags = (await command([
      'COMMAND',
      'GETKEYSANDFLAGS',
      'MGET',
      ...keys,
    ])) as Array<[string, string[]]>
    assert.deepStrictEqual(
      keysAndFlags,
      keys.map(key => [key, ['RO', 'access']]),
    )
  })

  test('COMMAND errors match Redis', async () => {
    await assert.rejects(
      command(['COMMAND', 'BAD']),
      errorWithMessage("ERR unknown subcommand 'BAD'. Try COMMAND HELP."),
    )
    await assert.rejects(
      command(['COMMAND', 'LIST', 'BAD']),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      command(['COMMAND', 'GETKEYS']),
      errorWithMessage(
        "ERR wrong number of arguments for 'command|getkeys' command",
      ),
    )
    await assert.rejects(
      command(['COMMAND', 'GETKEYS', 'NOSUCH', 'key']),
      errorWithMessage('ERR Invalid command specified'),
    )
    await assert.rejects(
      command(['COMMAND', 'GETKEYS', 'SCAN', '0']),
      errorWithMessage('ERR The command has no key arguments'),
    )
  })
})

function assertCommandInfo(
  actual: CommandInfoReply,
  expected: {
    name: string
    arity: number
    flags: string[]
    firstKey: number
    lastKey: number
    keyStep: number
    categories: string[]
  },
): void {
  assert.strictEqual(actual[0], expected.name)
  assert.strictEqual(actual[1], expected.arity)
  for (const flag of expected.flags) {
    assert.ok(actual[2].includes(flag), `${expected.name} missing ${flag}`)
  }
  assert.strictEqual(actual[3], expected.firstKey)
  assert.strictEqual(actual[4], expected.lastKey)
  assert.strictEqual(actual[5], expected.keyStep)
  for (const category of expected.categories) {
    assert.ok(
      actual[6].includes(category),
      `${expected.name} missing ${category}`,
    )
  }
}
