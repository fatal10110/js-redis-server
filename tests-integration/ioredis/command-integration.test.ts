import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { commandFrame, errorWithMessage } from '../utils'
import {
  RawRedisConnection,
  respMapGet,
  respText,
} from '../raw-tcp/raw-connection'

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

describe(`COMMAND integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('command-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('COMMAND INFO returns Redis command metadata and nulls for unknown commands', async () => {
    const reply = (await redisClient?.call(
      'COMMAND',
      'INFO',
      'GET',
      'SET',
      'NOSUCHCOMMAND',
    )) as [CommandInfoReply, CommandInfoReply, null]

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
    const names = (await redisClient?.call('COMMAND', 'LIST')) as string[]
    assert.ok(names.includes('get'))
    assert.ok(names.includes('set'))
    assert.ok(names.includes('command'))
    assert.ok(names.includes('command|info'))

    const patternNames = (await redisClient?.call(
      'COMMAND',
      'LIST',
      'FILTERBY',
      'PATTERN',
      'get*',
    )) as string[]
    assert.ok(patternNames.includes('get'))
    assert.ok(patternNames.every(name => name.startsWith('get')))

    const moduleNames = (await redisClient?.call(
      'COMMAND',
      'LIST',
      'FILTERBY',
      'MODULE',
      'nosuchmodule',
    )) as string[]
    assert.deepStrictEqual(moduleNames, [])
  })

  test('COMMAND COUNT and HELP expose the command surface', async () => {
    const names = (await redisClient?.call('COMMAND', 'LIST')) as string[]
    const count = (await redisClient?.call('COMMAND', 'COUNT')) as number
    const help = (await redisClient?.call('COMMAND', 'HELP')) as string[]

    assert.ok(count > 0)
    assert.ok(names.length > 0)
    assert.ok(help.some(line => line.includes('GETKEYS')))
    assert.ok(help.some(line => line.includes('GETKEYSANDFLAGS')))
  })

  test('COMMAND DOCS returns documentation entries and skips unknown commands', async () => {
    const docs = (await redisClient?.call(
      'COMMAND',
      'DOCS',
      'GET',
    )) as unknown[]
    assert.strictEqual(docs[0], 'get')

    const getDocs = docs[1] as unknown[]
    assertMapEntry(getDocs, 'summary', 'Get the value of a key')
    assertMapEntry(getDocs, 'group', 'string')

    const unknownDocs = (await redisClient?.call(
      'COMMAND',
      'DOCS',
      'NOSUCHCOMMAND',
    )) as unknown[]
    assert.deepStrictEqual(unknownDocs, [])
  })

  test('COMMAND DOCS returns RESP3 maps after HELLO 3', async () => {
    const port = testRunner.getClusterPorts()[0]
    assert.notStrictEqual(port, undefined)
    const connection = await RawRedisConnection.connect('127.0.0.1', port!)

    try {
      connection.write(commandFrame('HELLO', '3'))
      assert.ok((await connection.readFrame()) instanceof Map)

      connection.write(commandFrame('COMMAND', 'DOCS', 'GET'))
      const reply = await connection.readFrame()
      assert.ok(reply instanceof Map)

      const getDocs = respMapGet(reply, 'get')
      assert.ok(getDocs instanceof Map)
      assert.strictEqual(
        respText(respMapGet(getDocs, 'summary')),
        'Get the value of a key',
      )
      assert.strictEqual(respText(respMapGet(getDocs, 'group')), 'string')

      const args = respMapGet(getDocs, 'arguments')
      assert.ok(Array.isArray(args))
      assert.ok(args[0] instanceof Map)
      assert.strictEqual(respText(respMapGet(args[0], 'name')), 'key')
    } finally {
      connection.close()
    }
  })

  test('COMMAND DOCS argument flags use RESP status strings for redis-cli', async () => {
    const port = testRunner.getClusterPorts()[0]
    assert.notStrictEqual(port, undefined)
    const connection = await RawRedisConnection.connect('127.0.0.1', port!)

    try {
      connection.write(commandFrame('COMMAND', 'DOCS', 'MGET'))
      const reply = await connection.readFrame()
      assert.ok(Array.isArray(reply))

      const docs = resp2MapGet(reply, 'mget')
      assert.ok(Array.isArray(docs))
      const args = resp2MapGet(docs, 'arguments')
      assert.ok(Array.isArray(args))
      assert.ok(Array.isArray(args[0]))

      const flags = resp2MapGet(args[0], 'flags')
      assert.deepStrictEqual(flags, ['multiple'])
    } finally {
      connection.close()
    }
  })

  test('COMMAND GETKEYS and GETKEYSANDFLAGS use command key extraction', async () => {
    const tag = '{command-getkeys}'
    const keys = [`${tag}:a`, `${tag}:b`, `${tag}:c`]

    const getKeys = (await redisClient?.call(
      'COMMAND',
      'GETKEYS',
      'MGET',
      ...keys,
    )) as string[]
    assert.deepStrictEqual(getKeys, keys)

    const keysAndFlags = (await redisClient?.call(
      'COMMAND',
      'GETKEYSANDFLAGS',
      'MGET',
      ...keys,
    )) as Array<[string, string[]]>
    assert.deepStrictEqual(
      keysAndFlags,
      keys.map(key => [key, ['RO', 'access']]),
    )
  })

  test('COMMAND errors match Redis', async () => {
    await assert.rejects(
      redisClient!.call('COMMAND', 'BAD'),
      errorWithMessage("ERR unknown subcommand 'BAD'. Try COMMAND HELP."),
    )
    await assert.rejects(
      redisClient!.call('COMMAND', 'LIST', 'BAD'),
      errorWithMessage('ERR syntax error'),
    )
    await assert.rejects(
      redisClient!.call('COMMAND', 'GETKEYS'),
      errorWithMessage(
        "ERR wrong number of arguments for 'command|getkeys' command",
      ),
    )
    await assert.rejects(
      redisClient!.call('COMMAND', 'GETKEYS', 'NOSUCH', 'key'),
      errorWithMessage('ERR Invalid command specified'),
    )
    await assert.rejects(
      redisClient!.call('COMMAND', 'GETKEYS', 'SCAN', '0'),
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

function assertMapEntry(
  values: unknown[],
  key: string,
  expected: unknown,
): void {
  const index = values.indexOf(key)
  assert.notStrictEqual(index, -1)
  assert.strictEqual(values[index + 1], expected)
}

function resp2MapGet(values: unknown[], key: string): unknown {
  for (let i = 0; i < values.length; i += 2) {
    if (respText(values[i]) === key) {
      return values[i + 1]
    }
  }

  assert.fail(`Missing RESP2 map key ${key}`)
}
