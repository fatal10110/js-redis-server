import { after, afterEach, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame } from '../utils'
import { RawRedisConnection } from './raw-connection'

const testRunner = new TestRunner()

/** CLIENT NO-TOUCH arrived in Redis 7.2 and Valkey 7.2. */
const supportsNoTouch = !['redis-6.2', 'redis-7.0'].includes(activeProfile)

describe(`Raw TCP CLIENT REPLY / NO-TOUCH (${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  afterEach(() => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
  })

  after(async () => {
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    return connection
  }

  /** Pipeline `commands` in one write and read the next frame. */
  async function pipeline(
    connection: RawRedisConnection,
    ...commands: string[][]
  ): Promise<string> {
    connection.write(
      Buffer.concat(commands.map(command => commandFrame(...command))),
    )
    return (await connection.readRawFrame()).toString()
  }

  /** The connection has nothing else buffered: the next reply is PING's. */
  async function assertDrained(connection: RawRedisConnection): Promise<void> {
    assert.strictEqual(await pipeline(connection, ['PING']), '+PONG\r\n')
  }

  test('CLIENT REPLY ON answers +OK, an unknown mode is a syntax error', async () => {
    const client = await connect()
    assert.strictEqual(
      await pipeline(client, ['CLIENT', 'REPLY', 'ON']),
      '+OK\r\n',
    )
    assert.strictEqual(
      await pipeline(client, ['client', 'reply', 'on']),
      '+OK\r\n',
    )
    assert.strictEqual(
      await pipeline(client, ['CLIENT', 'REPLY', 'foo']),
      '-ERR syntax error\r\n',
    )
  })

  test('CLIENT REPLY SKIP suppresses exactly the next reply', async () => {
    const client = await connect()
    assert.strictEqual(
      await pipeline(
        client,
        ['CLIENT', 'REPLY', 'SKIP'],
        ['ECHO', '1'],
        ['ECHO', '2'],
      ),
      '$1\r\n2\r\n',
    )
    assert.strictEqual(
      await pipeline(
        client,
        ['CLIENT', 'REPLY', 'SKIP'],
        ['CLIENT', 'REPLY', 'SKIP'],
        ['ECHO', '1'],
        ['ECHO', '2'],
      ),
      '$1\r\n2\r\n',
    )
    assert.strictEqual(
      await pipeline(
        client,
        ['CLIENT', 'REPLY', 'SKIP'],
        ['CLIENT', 'ID'],
        ['ECHO', '2'],
      ),
      '$1\r\n2\r\n',
    )
    // An error reply is suppressed too.
    assert.strictEqual(
      await pipeline(
        client,
        ['CLIENT', 'REPLY', 'SKIP'],
        ['CLIENT', 'REPLY', 'foo'],
        ['ECHO', '2'],
      ),
      '$1\r\n2\r\n',
    )
    // CLIENT REPLY ON clears the skip before it answers.
    assert.strictEqual(
      await pipeline(
        client,
        ['CLIENT', 'REPLY', 'SKIP'],
        ['CLIENT', 'REPLY', 'ON'],
      ),
      '+OK\r\n',
    )
    await assertDrained(client)
  })

  test('CLIENT REPLY OFF suppresses every reply until CLIENT REPLY ON', async () => {
    const client = await connect()
    assert.strictEqual(
      await pipeline(
        client,
        ['CLIENT', 'REPLY', 'OFF'],
        ['ECHO', '1'],
        ['CLIENT', 'REPLY', 'SKIP'],
        ['ECHO', '2'],
        ['GET'],
        ['CLIENT', 'REPLY', 'ON'],
        ['ECHO', '3'],
      ),
      '+OK\r\n',
    )
    assert.strictEqual((await client.readRawFrame()).toString(), '$1\r\n3\r\n')
    await assertDrained(client)
  })

  test('RESET turns replies back on but not a pending skip', async () => {
    const client = await connect()
    assert.strictEqual(
      await pipeline(client, ['CLIENT', 'REPLY', 'OFF'], ['RESET']),
      '+RESET\r\n',
    )
    await assertDrained(client)
    assert.strictEqual(
      await pipeline(
        client,
        ['CLIENT', 'REPLY', 'SKIP'],
        ['RESET'],
        ['ECHO', '2'],
      ),
      '$1\r\n2\r\n',
    )
    await assertDrained(client)
  })

  test('a skip before MULTI suppresses only its +OK', async () => {
    const client = await connect()
    assert.strictEqual(
      await pipeline(
        client,
        ['CLIENT', 'REPLY', 'SKIP'],
        ['MULTI'],
        ['ECHO', '1'],
      ),
      '+QUEUED\r\n',
    )
    assert.strictEqual(await pipeline(client, ['EXEC']), '*1\r\n$1\r\n1\r\n')
  })

  test(
    'CLIENT NO-TOUCH sets the T flag; RESET clears it',
    { skip: !supportsNoTouch && `no CLIENT NO-TOUCH on ${activeProfile}` },
    async () => {
      const client = await connect()
      const flags = async () => {
        const info = await pipeline(client, ['CLIENT', 'INFO'])
        return /flags=(\S+)/.exec(info)?.[1]
      }

      assert.strictEqual(
        await pipeline(client, ['CLIENT', 'NO-TOUCH', 'on']),
        '+OK\r\n',
      )
      assert.strictEqual(await flags(), 'T')
      assert.strictEqual(
        await pipeline(client, ['CLIENT', 'NO-EVICT', 'ON']),
        '+OK\r\n',
      )
      assert.strictEqual(await flags(), 'eT')
      assert.strictEqual(
        await pipeline(client, ['CLIENT', 'NO-TOUCH', 'maybe']),
        '-ERR syntax error\r\n',
      )
      assert.strictEqual(
        await pipeline(client, ['CLIENT', 'NO-TOUCH', 'Off']),
        '+OK\r\n',
      )
      assert.strictEqual(await flags(), 'e')
      assert.strictEqual(
        await pipeline(client, ['CLIENT', 'NO-TOUCH', 'ON']),
        '+OK\r\n',
      )
      assert.strictEqual(await pipeline(client, ['RESET']), '+RESET\r\n')
      assert.strictEqual(await flags(), 'N')
    },
  )
})
