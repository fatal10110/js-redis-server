import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import type { Cluster } from 'ioredis'
import clusterKeySlot from 'cluster-key-slot'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import {
  RawRedisConnection,
  respMapGet,
  respNumber,
  respText,
} from './raw-connection'

const testRunner = new TestRunner()

describe(`Raw TCP MONITOR protocol (${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(targetPort = port): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', targetPort)
    connections.push(connection)
    return connection
  }

  test('activates a stream and delivers commands from other connections', async () => {
    const monitor = await connect()
    const actor = await connect()
    const key = `monitor:${randomKey()}`

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('ping'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+PONG\r\n'))
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['ping'],
    })

    actor.write(commandFrame('select', '2'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+OK\r\n'))
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 2,
      argv: ['select', '2'],
    })

    actor.write(commandFrame('set', key, 'hello world'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+OK\r\n'))
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 2,
      argv: ['set', key, 'hello world'],
    })
  })

  test('uses simple string monitor frames after HELLO 3', async () => {
    const monitor = await connect()
    const actor = await connect()

    monitor.write(commandFrame('HELLO', '3'))
    const hello = await monitor.readFrame()
    assert.ok(hello instanceof Map)
    assert.strictEqual(respNumber(respMapGet(hello, 'proto')), 3)

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('ping'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+PONG\r\n'))

    const rawLine = await monitor.readRawFrame()
    assert.strictEqual(rawLine[0], '+'.charCodeAt(0))
    assertMonitorLine(rawLine.toString().slice(1, -2), {
      database: 0,
      argv: ['ping'],
    })
  })

  test('skips unknown and unparsed commands but monitors execution errors', async () => {
    const monitor = await connect()
    const actor = await connect()
    const key = `monitor:${randomKey()}`

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('NOEXISTS', 'arg'))
    assert.match(respText(await actor.readFrame()), /^ERR unknown command/)

    actor.write(commandFrame('GET'))
    assert.strictEqual(
      respText(await actor.readFrame()),
      "ERR wrong number of arguments for 'get' command",
    )

    actor.write(commandFrame('SET', key, 'hello'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+OK\r\n'))
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['SET', key, 'hello'],
    })

    actor.write(commandFrame('LPOP', key))
    assert.match(respText(await actor.readFrame()), /^WRONGTYPE/)
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['LPOP', key],
    })
  })

  test('skips cluster commands redirected with MOVED', async () => {
    const cluster = await testRunner.setupIoredisCluster('monitor-moved')
    const directPort = testRunner.getClusterPorts()[0]
    assert.notStrictEqual(directPort, undefined)
    const remoteKey = await findKeyNotOwnedByPort(cluster, directPort!)
    const monitor = await connect(directPort)
    const actor = await connect(directPort)

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('GET', remoteKey))
    assert.match(respText(await actor.readFrame()), /^MOVED\b/)

    actor.write(commandFrame('PING'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+PONG\r\n'))
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['PING'],
    })
  })

  test('rejects invalid MONITOR arity and Lua usage', async () => {
    const conn = await connect()

    conn.write(commandFrame('MONITOR', 'extra'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR wrong number of arguments for 'monitor' command",
    )

    conn.write(commandFrame('EVAL', 'return redis.call("monitor")', '0'))
    assert.match(
      respText(await conn.readFrame()),
      /^ERR This Redis command is not allowed from script/,
    )
  })
})

function assertMonitorLine(
  line: string,
  expected: { database: number; argv: string[] },
): void {
  const match = /^(\d+\.\d{6}) \[(\d+) ([^\]]+)\] (.+)$/.exec(line)
  assert.ok(match, `unexpected monitor line: ${line}`)

  assert.ok(Number.isFinite(Number(match[1])))
  assert.strictEqual(Number(match[2]), expected.database)
  assert.match(match[3], /^(?:\d{1,3}\.){3}\d{1,3}:\d+$/)
  assert.deepStrictEqual(parseMonitorArgv(match[4]), expected.argv)
}

function parseMonitorArgv(text: string): string[] {
  const values: string[] = []
  let index = 0

  while (index < text.length) {
    assert.strictEqual(text[index], '"')
    index++

    let value = ''
    while (index < text.length) {
      const char = text[index]
      index++

      if (char === '"') {
        break
      }

      if (char !== '\\') {
        value += char
        continue
      }

      const escaped = text[index]
      index++
      if (escaped === 'n') value += '\n'
      else if (escaped === 'r') value += '\r'
      else if (escaped === 't') value += '\t'
      else value += escaped
    }

    values.push(value)
    if (index < text.length) {
      assert.strictEqual(text[index], ' ')
      index++
    }
  }

  return values
}

type ClusterSlotsReply = Array<
  [min: number, max: number, master: [host: string, port: number]]
>

async function findKeyNotOwnedByPort(
  cluster: Cluster,
  port: number,
): Promise<string> {
  const slots = (await cluster.cluster('SLOTS')) as ClusterSlotsReply

  for (let i = 0; i < 10_000; i++) {
    const key = `{monitor-moved-${i}}`
    const owner = findSlotOwner(slots, clusterKeySlot(key))
    if (owner?.[1] !== port) {
      return key
    }
  }

  assert.fail(`Could not find a key outside port ${port}`)
}

function findSlotOwner(
  slots: ClusterSlotsReply,
  slot: number,
): [host: string, port: number] | undefined {
  for (const [min, max, master] of slots) {
    if (slot >= min && slot <= max) {
      return master
    }
  }

  return undefined
}
