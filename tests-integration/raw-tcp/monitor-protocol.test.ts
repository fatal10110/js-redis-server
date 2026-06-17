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

  test('escapes binary command arguments like Redis', async () => {
    const monitor = await connect()
    const actor = await connect()
    const key = Buffer.from(`monitor-binary:${randomKey()}`)
    const value = Buffer.from([
      0x00, 0x07, 0x08, 0x09, 0x0a, 0x0d, 0x1f, 0x20, 0x22, 0x5c, 0x7e, 0x7f,
      0x80, 0xff,
    ])

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(binaryCommandFrame('SET', key, value))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+OK\r\n'))

    const rawLine = await monitor.readRawFrame()
    assert.strictEqual(rawLine[0], '+'.charCodeAt(0))
    const line = rawLine.toString().slice(1, -2)
    const expectedValue = String.raw`\x00\a\b\t\n\r\x1f \"\\~\x7f\x80\xff`
    assert.ok(
      line.endsWith(` "SET" "${key.toString()}" "${expectedValue}"`),
      `unexpected monitor line: ${line}`,
    )
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

  test('skips cluster commands rejected before local execution', async () => {
    const cluster = await testRunner.setupIoredisCluster('monitor-moved')
    const directPort = testRunner.getClusterPorts()[0]
    assert.notStrictEqual(directPort, undefined)
    const remoteKey = await findKeyNotOwnedByPort(cluster, directPort!)
    const crossSlotKeyA = '{monitor-crossslot-a}'
    const crossSlotKeyB = '{monitor-crossslot-b}'
    assert.notStrictEqual(
      clusterKeySlot(crossSlotKeyA),
      clusterKeySlot(crossSlotKeyB),
    )
    const monitor = await connect(directPort)
    const actor = await connect(directPort)

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('GET', remoteKey))
    assert.match(respText(await actor.readFrame()), /^MOVED\b/)

    actor.write(commandFrame('MGET', crossSlotKeyA, crossSlotKeyB))
    assert.match(respText(await actor.readFrame()), /^CROSSSLOT\b/)

    actor.write(commandFrame('PING'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+PONG\r\n'))
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['PING'],
    })
  })

  test('redacts authentication credentials', async () => {
    const monitor = await connect()
    const actor = await connect()

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('AUTH', 'secret'))
    assert.match(
      respText(await actor.readFrame()),
      /^ERR AUTH <password> called without any password configured/,
    )
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['AUTH', '(redacted)'],
    })

    actor.write(commandFrame('AUTH', 'default', 'secret'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+OK\r\n'))
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['AUTH', '(redacted)', '(redacted)'],
    })

    actor.write(commandFrame('HELLO', '2', 'AUTH', 'default', 'secret'))
    assert.ok(Array.isArray(await actor.readFrame()))
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['HELLO', '2', 'AUTH', '(redacted)', '(redacted)'],
    })
  })

  test('skips monitor-hidden commands without hiding INFO or CLIENT', async () => {
    const monitor = await connect()
    const actor = await connect()

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('CONFIG', 'GET', 'maxmemory'))
    await actor.readFrame()

    actor.write(commandFrame('INFO', 'server'))
    await actor.readFrame()
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['INFO', 'server'],
    })

    actor.write(commandFrame('CLIENT', 'GETNAME'))
    await actor.readFrame()
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['CLIENT', 'GETNAME'],
    })
  })

  test('emits Lua redis.call commands with a lua monitor source', async () => {
    const monitor = await connect()
    const actor = await connect()
    const key = `monitor-lua:${randomKey()}`
    const script = `redis.call("set", "${key}", "v"); return redis.call("get", "${key}")`

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('EVAL', script, '0'))
    assert.deepStrictEqual(await actor.readFrame(), Buffer.from('v'))

    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['EVAL', script, '0'],
    })
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      source: 'lua',
      argv: ['set', key, 'v'],
    })
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      source: 'lua',
      argv: ['get', key],
    })
  })

  test('monitors transaction commands once in client order', async () => {
    const monitor = await connect()
    const actor = await connect()
    const key = `monitor-tx:${randomKey()}`

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('MULTI'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+OK\r\n'))
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['MULTI'],
    })

    actor.write(commandFrame('SET', key, 'v'))
    assert.deepStrictEqual(
      await actor.readRawFrame(),
      Buffer.from('+QUEUED\r\n'),
    )

    actor.write(commandFrame('EXEC'))
    assert.deepStrictEqual(await actor.readFrame(), ['OK'])
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['SET', key, 'v'],
    })
    assertMonitorLine(respText(await monitor.readFrame()), {
      database: 0,
      argv: ['EXEC'],
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

  test('keeps the monitoring connection itself usable for further commands (#126)', async () => {
    const monitor = await connect()

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    // Draining the long-lived MONITOR stream must not block the connection's
    // frame loop: the same connection can still issue commands and read their
    // replies (real Redis lets a monitoring client keep sending commands).
    monitor.write(commandFrame('PING'))
    assert.deepStrictEqual(
      await readMonitorReply(monitor),
      Buffer.from('+PONG\r\n'),
    )

    // A second round proves the loop keeps flowing, not just a one-shot.
    monitor.write(commandFrame('PING', 'hello'))
    assert.deepStrictEqual(
      await readMonitorReply(monitor),
      Buffer.from('$5\r\nhello\r\n'),
    )
  })

  test('RESET on a monitoring connection exits monitor mode (#126)', async () => {
    const monitor = await connect()

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    monitor.write(commandFrame('RESET'))
    assert.deepStrictEqual(
      await readMonitorReply(monitor),
      Buffer.from('+RESET\r\n'),
    )

    // Back in normal mode: a command on this connection still works.
    monitor.write(commandFrame('PING'))
    assert.deepStrictEqual(
      await readMonitorReply(monitor),
      Buffer.from('+PONG\r\n'),
    )
  })
})

/**
 * Read the next command reply on a monitoring connection, skipping any monitor
 * feed lines that interleave with it. Feed lines are simple strings beginning
 * with a timestamp (`+<digits>.<digits> [...]`), so a reply is any frame whose
 * second byte is not a digit. Times out instead of hanging forever so a blocked
 * frame loop surfaces as a test failure rather than a stuck process.
 */
async function readMonitorReply(
  connection: RawRedisConnection,
): Promise<Buffer> {
  for (let i = 0; i < 32; i++) {
    const raw = await withTimeout(
      connection.readRawFrame(),
      2000,
      'monitor connection reply',
    )
    if (raw[0] === PLUS && raw[1] >= ZERO && raw[1] <= NINE) {
      continue
    }
    return raw
  }
  assert.fail('no non-monitor reply arrived on the monitoring connection')
}

function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  label: string,
): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => {
      setTimeout(
        () => reject(new Error(`timed out waiting for ${label}`)),
        ms,
      ).unref()
    }),
  ])
}

const PLUS = '+'.charCodeAt(0)
const ZERO = '0'.charCodeAt(0)
const NINE = '9'.charCodeAt(0)

function assertMonitorLine(
  line: string,
  expected: { database: number; argv: string[]; source?: string | RegExp },
): void {
  const match =
    /^(\d+\.\d{6}) \[(\d+) ((?:\[[^\]]+\]:\d+)|[^\]]+)\] (.+)$/.exec(line)
  assert.ok(match, `unexpected monitor line: ${line}`)

  assert.ok(Number.isFinite(Number(match[1])))
  assert.strictEqual(Number(match[2]), expected.database)
  if (typeof expected.source === 'string') {
    assert.strictEqual(match[3], expected.source)
  } else if (expected.source) {
    assert.match(match[3], expected.source)
  } else {
    assert.match(match[3], /^(?:(?:\d{1,3}\.){3}\d{1,3}|\[[^\]]+\]):\d+$/)
  }
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

function binaryCommandFrame(...items: Array<string | Buffer>): Buffer {
  const parts = [Buffer.from(`*${items.length}\r\n`)]

  for (const item of items) {
    const value = Buffer.isBuffer(item) ? item : Buffer.from(item)
    parts.push(Buffer.from(`$${value.length}\r\n`), value, Buffer.from('\r\n'))
  }

  return Buffer.concat(parts)
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
