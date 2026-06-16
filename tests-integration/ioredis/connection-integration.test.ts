import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import {
  commandFrame,
  connectToSlotOwner,
  eventually,
  errorWithMessage,
  randomKey,
} from '../utils'
import {
  RawRedisConnection,
  respMapGet,
  respNumber,
  respText,
} from '../raw-tcp/raw-connection'

const testRunner = new TestRunner()

describe(`Connection commands integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined
  let directClient: Redis | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('connection-integration')
    directClient = await connectToSlotOwner(
      redisClient,
      `{connection:${randomKey()}}:probe`,
    )
  })

  after(async () => {
    directClient?.disconnect()
    await testRunner.cleanup()
  })

  test('PING and INFO expose Redis-compatible connection metadata', async () => {
    assert.strictEqual(await directClient?.ping(), 'PONG')
    assert.strictEqual(await directClient?.call('PING', 'hello'), 'hello')

    const info = (await directClient?.info()) ?? ''
    assert.match(info, /loading:0/)
    assert.match(info, /redis_mode:cluster/)
    assert.match(info, /cluster_enabled:1/)
  })

  test('CLIENT name, id, info, and list are connection-local', async () => {
    const name = `client-${randomKey()}`

    assert.strictEqual(
      await directClient?.call('CLIENT', 'SETNAME', name),
      'OK',
    )
    assert.strictEqual(await directClient?.call('CLIENT', 'GETNAME'), name)

    const id = await directClient?.call('CLIENT', 'ID')
    assert.strictEqual(typeof id, 'number')
    assert.ok((id as number) > 0)

    const info = (await directClient?.call('CLIENT', 'INFO')) as string
    assert.match(info, new RegExp(`name=${name}`))
    assert.match(info, /db=0/)

    const list = (await directClient?.call('CLIENT', 'LIST')) as string
    assert.match(list, new RegExp(`name=${name}`))
  })

  test('CLIENT LIST reports all active clients on the connected node', async () => {
    const key = `{client-list:${randomKey()}}:probe`
    const primary = await connectToSlotOwner(redisClient!, key)
    const secondary = await connectToSlotOwner(redisClient!, key)
    const primaryName = `primary-${randomKey()}`
    const secondaryName = `secondary-${randomKey()}`

    try {
      assert.strictEqual(
        await primary.call('CLIENT', 'SETNAME', primaryName),
        'OK',
      )
      assert.strictEqual(
        await secondary.call('CLIENT', 'SETNAME', secondaryName),
        'OK',
      )

      const list = (await primary.call('CLIENT', 'LIST')) as string
      assert.match(list, new RegExp(`name=${primaryName}`))
      assert.match(list, new RegExp(`name=${secondaryName}`))

      secondary.disconnect()
      await eventually(async () => {
        const updated = (await primary.call('CLIENT', 'LIST')) as string
        assert.match(updated, new RegExp(`name=${primaryName}`))
        assert.doesNotMatch(updated, new RegExp(`name=${secondaryName}`))
      })
    } finally {
      primary.disconnect()
      secondary.disconnect()
    }
  })

  test('HELLO can set the connection name and reports cluster mode', async () => {
    const name = `hello-${randomKey()}`
    const hello = (await directClient?.call(
      'HELLO',
      '2',
      'SETNAME',
      name,
    )) as unknown[]

    assertHelloEntry(hello, 'server', 'redis')
    assertHelloEntry(hello, 'proto', 2)
    assertHelloEntry(hello, 'mode', 'cluster')
    assert.strictEqual(await directClient?.call('CLIENT', 'GETNAME'), name)
  })

  test('HELLO 3 switches the connection to RESP3 replies', async () => {
    const port = testRunner.getClusterPorts()[0]
    assert.notStrictEqual(port, undefined)
    const connection = await RawRedisConnection.connect('127.0.0.1', port!)

    try {
      connection.write(commandFrame('HELLO', '3'))
      const hello = await connection.readFrame()

      assert.ok(hello instanceof Map)
      assert.strictEqual(respText(respMapGet(hello, 'server')), 'redis')
      assert.strictEqual(respNumber(respMapGet(hello, 'proto')), 3)
      assert.strictEqual(respText(respMapGet(hello, 'mode')), 'cluster')

      connection.write(commandFrame('CLIENT', 'GETNAME'))
      assert.strictEqual(await connection.readFrame(), null)

      connection.write(commandFrame('HELLO'))
      const repeatedHello = await connection.readFrame()
      assert.ok(repeatedHello instanceof Map)
      assert.strictEqual(respNumber(respMapGet(repeatedHello, 'proto')), 3)

      connection.write(commandFrame('CLIENT', 'INFO'))
      assert.match(
        respText(await connection.readFrame()),
        /(?:^| )resp=3(?: |\n)/,
      )

      connection.write(commandFrame('CLIENT', 'LIST'))
      assert.match(
        respText(await connection.readFrame()),
        /(?:^| )resp=3(?: |\n)/,
      )

      connection.write(commandFrame('RESET'))
      assert.deepStrictEqual(
        await connection.readRawFrame(),
        Buffer.from('+RESET\r\n'),
      )

      connection.write(commandFrame('CLIENT', 'GETNAME'))
      assert.deepStrictEqual(
        await connection.readRawFrame(),
        Buffer.from('$-1\r\n'),
      )
    } finally {
      connection.close()
    }
  })

  test('AUTH without configured password returns the Redis error', async () => {
    await assert.rejects(
      () => directClient?.auth('secret'),
      errorWithMessage(
        'ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?',
      ),
    )
  })

  test('RESET clears connection-local state', async () => {
    assert.strictEqual(
      await directClient?.call('CLIENT', 'SETNAME', 'reset-name'),
      'OK',
    )
    assert.strictEqual(await directClient?.call('RESET'), 'RESET')
    assert.strictEqual(await directClient?.call('CLIENT', 'GETNAME'), null)
  })

  test('SELECT 0 is allowed in cluster mode (no-op DB switch)', async () => {
    assert.strictEqual(await directClient?.select(0), 'OK')
  })

  test('SELECT of a non-zero DB is rejected in cluster mode', async () => {
    await assert.rejects(
      () => directClient?.select(1),
      errorWithMessage('ERR SELECT is not allowed in cluster mode'),
    )
    await assert.rejects(
      () => directClient?.select(99),
      errorWithMessage('ERR SELECT is not allowed in cluster mode'),
    )
  })

  test('SELECT with a non-integer index is a value error in cluster mode', async () => {
    await assert.rejects(
      () => directClient?.call('SELECT', 'abc'),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
  })
})

function assertHelloEntry(
  reply: unknown[],
  key: string,
  expected: string | number,
): void {
  const index = reply.indexOf(key)
  assert.notStrictEqual(index, -1)
  assert.strictEqual(reply[index + 1], expected)
}
