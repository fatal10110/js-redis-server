import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection, respText } from './raw-connection'

/**
 * MONITOR shows a command that fails its own argument check: Redis runs the
 * check inside call(), which feeds the monitors (#519). An unknown command
 * or a wrong argument count is refused in processCommand and never shown.
 * Checked against redis-server 6.2.14, 8.0.0 and valkey-server 9.0.0.
 */
const testRunner = new TestRunner()
const legacy = activeProfile === 'redis-6.2'

describe(`Raw TCP MONITOR of argument errors (${testRunner.getBackendName()})`, () => {
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

  async function connect(): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    return connection
  }

  test('logs commands whose own argument check fails', async () => {
    const monitor = await connect()
    const actor = await connect()
    const key = `monitor:${randomKey()}`

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    const failing = [
      ['EVAL', 'return 1', '-1', key],
      ['EVAL', 'return 1', '1x', key],
      ['XREAD', 'COUNT', 'x', 'STREAMS', key, '0'],
      ['LPOP', key, 'x'],
      ['SET', key, 'v', 'EX', 'abc'],
      ['BLPOP', key, 'x'],
      ['ZADD', key, '1', 'a', '2'],
      ['INCRBY', key, 'x'],
    ]
    // Refused before call(): not shown.
    const hidden = [['GET'], ['NOSUCHCOMMAND', key]]
    // An argument error from 7.0; an unknown command on 6.2.
    const functionList = ['FUNCTION', 'LIST', 'LIBRARYNAME']

    const expected: string[][] = []
    for (const argv of failing) {
      actor.write(commandFrame(...argv))
      assert.match(respText(await actor.readFrame()), /^ERR /)
      expected.push(argv)
    }
    for (const argv of hidden) {
      actor.write(commandFrame(...argv))
      assert.match(respText(await actor.readFrame()), /^ERR /)
    }
    actor.write(commandFrame(...functionList))
    assert.match(respText(await actor.readFrame()), /^ERR /)
    if (!legacy) {
      expected.push(functionList)
    }
    actor.write(commandFrame('PING'))
    assert.deepStrictEqual(await actor.readRawFrame(), Buffer.from('+PONG\r\n'))
    expected.push(['PING'])

    for (const argv of expected) {
      const line = respText(await monitor.readFrame())
      assert.match(line, /^\d+\.\d{6} \[0 [^\]]+\] "/)
      assert.ok(
        line.endsWith(` ${argv.map(arg => `"${arg}"`).join(' ')}`),
        `expected ${JSON.stringify(argv)}, got monitor line: ${line}`,
      )
    }
  })
})
