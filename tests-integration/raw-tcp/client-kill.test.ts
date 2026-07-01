import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as delay } from 'node:timers/promises'
import { TestRunner } from '../test-config'
import { commandFrame } from '../utils'
import { RawRedisConnection } from './raw-connection'

const testRunner = new TestRunner()

describe(
  `Raw TCP CLIENT KILL (${testRunner.getBackendName()})`,
  { skip: testRunner.backend === 'real' && 'real Redis backend is 7.2.1' },
  () => {
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

    test('CLIENT KILL ID with MAXAGE closes an old matching client', async () => {
      const killer = await connect()
      const victim = await connect()
      const victimId = await clientId(victim)

      await delay(1100)

      killer.write(
        commandFrame('CLIENT', 'KILL', 'ID', victimId, 'MAXAGE', '0'),
      )
      assert.deepStrictEqual(await killer.readRawFrame(), Buffer.from(':1\r\n'))
      await assertCloses(victim)

      killer.write(commandFrame('PING'))
      assert.deepStrictEqual(
        await killer.readRawFrame(),
        Buffer.from('+PONG\r\n'),
      )
    })

    test('CLIENT KILL MAXAGE leaves newer matching clients connected', async () => {
      const killer = await connect()
      const victim = await connect()
      const victimId = await clientId(victim)

      killer.write(
        commandFrame('CLIENT', 'KILL', 'ID', victimId, 'MAXAGE', '999999'),
      )
      assert.deepStrictEqual(await killer.readRawFrame(), Buffer.from(':0\r\n'))

      victim.write(commandFrame('PING'))
      assert.deepStrictEqual(
        await victim.readRawFrame(),
        Buffer.from('+PONG\r\n'),
      )
    })
  },
)

async function clientId(connection: RawRedisConnection): Promise<string> {
  connection.write(commandFrame('CLIENT', 'ID'))
  const frame = (await connection.readRawFrame()).toString()
  assert.match(frame, /^:\d+\r\n$/)
  return frame.slice(1, -2)
}

async function assertCloses(connection: RawRedisConnection): Promise<void> {
  await Promise.race([
    connection.readUntilClose(),
    delay(3000).then(() => {
      throw new Error('connection did not close')
    }),
  ])
}
