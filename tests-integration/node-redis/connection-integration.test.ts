import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()
const INVALID_CLIENT_NAME_ERROR =
  'ERR Client names cannot contain spaces, newlines or special characters.'

describe(`Connection commands integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType
  let directClient: RedisClientType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      `{connection:${randomKey()}}:probe`,
    )
  })

  after(async () => {
    directClient.destroy()
    await testRunner.cleanup()
  })

  test('PING and INFO expose Redis-compatible connection metadata', async () => {
    assert.strictEqual(await directClient.ping(), 'PONG')
    assert.strictEqual(
      await directClient.sendCommand(['PING', 'hello']),
      'hello',
    )

    const info = await directClient.info()
    assert.match(info, /loading:0/)
    assert.match(info, /redis_mode:cluster/)
    assert.match(info, /cluster_enabled:1/)
  })

  test('CLIENT name, id, info, and list are connection-local', async () => {
    const name = `client-${randomKey()}`

    assert.strictEqual(
      await directClient.sendCommand(['CLIENT', 'SETNAME', name]),
      'OK',
    )
    assert.strictEqual(
      await directClient.sendCommand(['CLIENT', 'GETNAME']),
      name,
    )

    const id = await directClient.sendCommand(['CLIENT', 'ID'])
    assert.strictEqual(typeof id, 'number')
    assert.ok((id as number) > 0)

    const info = (await directClient.sendCommand(['CLIENT', 'INFO'])) as string
    assert.match(info, new RegExp(`name=${name}`))
    assert.match(info, /db=0/)

    const list = (await directClient.sendCommand(['CLIENT', 'LIST'])) as string
    assert.match(list, new RegExp(`name=${name}`))
  })

  test('CLIENT SETNAME validates names like Redis', async () => {
    const validName = `client-${randomKey()}_!~`
    const invalidNames = [
      'has space',
      'line\nbreak',
      'control',
      String.fromCharCode(0x7f),
      'café',
    ]

    assert.strictEqual(
      await directClient.sendCommand(['CLIENT', 'SETNAME', validName]),
      'OK',
    )
    assert.strictEqual(
      await directClient.sendCommand(['CLIENT', 'GETNAME']),
      validName,
    )

    for (const invalidName of invalidNames) {
      await assert.rejects(
        () => directClient.sendCommand(['CLIENT', 'SETNAME', invalidName]),
        errorWithMessage(INVALID_CLIENT_NAME_ERROR),
      )
      assert.strictEqual(
        await directClient.sendCommand(['CLIENT', 'GETNAME']),
        validName,
      )
    }

    assert.strictEqual(
      await directClient.sendCommand(['CLIENT', 'SETNAME', '']),
      'OK',
    )
    assert.strictEqual(
      await directClient.sendCommand(['CLIENT', 'GETNAME']),
      null,
    )
  })

  test('HELLO SETNAME validates names like CLIENT SETNAME', async () => {
    const key = `{hello-setname:${randomKey()}}:probe`
    const client = await connectToNodeRedisSlotOwner(redisClient, key)
    const validName = `hello-${randomKey()}`

    try {
      const hello = (await client.sendCommand([
        'HELLO',
        '2',
        'SETNAME',
        validName,
      ])) as unknown
      assert.strictEqual(helloField(hello, 'server'), 'redis')
      assert.strictEqual(
        await client.sendCommand(['CLIENT', 'GETNAME']),
        validName,
      )

      await assert.rejects(
        () => client.sendCommand(['HELLO', '2', 'SETNAME', 'has space']),
        errorWithMessage(INVALID_CLIENT_NAME_ERROR),
      )
      assert.strictEqual(
        await client.sendCommand(['CLIENT', 'GETNAME']),
        validName,
      )
    } finally {
      client.destroy()
    }
  })

  test('CLIENT LIST reports all active clients on the connected node', async () => {
    const key = `{client-list:${randomKey()}}:probe`
    const primary = await connectToNodeRedisSlotOwner(redisClient, key)
    const secondary = await connectToNodeRedisSlotOwner(redisClient, key)
    const primaryName = `primary-${randomKey()}`
    const secondaryName = `secondary-${randomKey()}`

    try {
      assert.strictEqual(
        await primary.sendCommand(['CLIENT', 'SETNAME', primaryName]),
        'OK',
      )
      assert.strictEqual(
        await secondary.sendCommand(['CLIENT', 'SETNAME', secondaryName]),
        'OK',
      )

      // Both connections to the same node appear in CLIENT LIST.
      const list = (await primary.sendCommand(['CLIENT', 'LIST'])) as string
      assert.match(list, new RegExp(`name=${primaryName}`))
      assert.match(list, new RegExp(`name=${secondaryName}`))
    } finally {
      primary.destroy()
      secondary.destroy()
    }
  })

  test('HELLO can set the connection name and reports cluster mode', async () => {
    const key = `{hello-mode:${randomKey()}}:probe`
    const client = await connectToNodeRedisSlotOwner(redisClient, key)
    const name = `hello-${randomKey()}`
    try {
      const hello = (await client.sendCommand([
        'HELLO',
        '2',
        'SETNAME',
        name,
      ])) as unknown
      assert.strictEqual(helloField(hello, 'server'), 'redis')
      assert.strictEqual(helloField(hello, 'proto'), 2)
      assert.strictEqual(helloField(hello, 'mode'), 'cluster')
      assert.strictEqual(await client.sendCommand(['CLIENT', 'GETNAME']), name)
    } finally {
      client.destroy()
    }
  })

  test('HELLO with an unsupported protocol version returns NOPROTO', async () => {
    for (const version of ['4', '0', '-1']) {
      await assert.rejects(
        () => directClient.sendCommand(['HELLO', version]),
        errorWithMessage('NOPROTO unsupported protocol version'),
      )
    }
  })

  test('HELLO with a non-integer protocol version returns the HELLO-specific ERR', async () => {
    await assert.rejects(
      () => directClient.sendCommand(['HELLO', 'abc']),
      errorWithMessage(
        'ERR Protocol version is not an integer or out of range',
      ),
    )
  })

  test('the default RESP3 connection reports resp=3 and RESET restores defaults', async () => {
    const key = `{hello3:${randomKey()}}:probe`
    const client = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      // node-redis negotiates RESP3 on connect; HELLO reports proto 3.
      const hello = (await client.sendCommand(['HELLO'])) as unknown
      assert.strictEqual(helloField(hello, 'server'), 'redis')
      assert.strictEqual(helloField(hello, 'proto'), 3)
      assert.strictEqual(helloField(hello, 'mode'), 'cluster')

      assert.match(
        (await client.sendCommand(['CLIENT', 'INFO'])) as string,
        /(?:^| )resp=3(?: |\n)/,
      )
      assert.match(
        (await client.sendCommand(['CLIENT', 'LIST'])) as string,
        /(?:^| )resp=3(?: |\n)/,
      )

      assert.strictEqual(await client.sendCommand(['RESET']), 'RESET')
      assert.strictEqual(await client.sendCommand(['CLIENT', 'GETNAME']), null)
    } finally {
      client.destroy()
    }
  })

  test('AUTH without configured password returns the Redis error', async () => {
    await assert.rejects(
      () => directClient.sendCommand(['AUTH', 'secret']),
      errorWithMessage(
        'ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?',
      ),
    )
  })

  test('RESET clears connection-local state', async () => {
    assert.strictEqual(
      await directClient.sendCommand(['CLIENT', 'SETNAME', 'reset-name']),
      'OK',
    )
    assert.strictEqual(await directClient.sendCommand(['RESET']), 'RESET')
    assert.strictEqual(
      await directClient.sendCommand(['CLIENT', 'GETNAME']),
      null,
    )
  })

  test('SELECT 0 is allowed in cluster mode (no-op DB switch)', async () => {
    assert.strictEqual(await directClient.sendCommand(['SELECT', '0']), 'OK')
  })

  test('SELECT of a non-zero DB is rejected in cluster mode', async () => {
    await assert.rejects(
      () => directClient.sendCommand(['SELECT', '1']),
      errorWithMessage('ERR SELECT is not allowed in cluster mode'),
    )
    await assert.rejects(
      () => directClient.sendCommand(['SELECT', '99']),
      errorWithMessage('ERR SELECT is not allowed in cluster mode'),
    )
  })

  test('SELECT with a non-integer index is a value error in cluster mode', async () => {
    await assert.rejects(
      () => directClient.sendCommand(['SELECT', 'abc']),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
  })
})

// HELLO replies arrive as a flat array on RESP2 and a map (object) on RESP3.
function helloField(reply: unknown, key: string): unknown {
  if (Array.isArray(reply)) {
    const index = reply.indexOf(key)
    return index === -1 ? undefined : reply[index + 1]
  }
  return (reply as Record<string, unknown>)[key]
}
