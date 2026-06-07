import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../utils'

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

  test('SELECT is rejected in cluster mode', async () => {
    await assert.rejects(
      () => directClient?.select(1),
      errorWithMessage('ERR SELECT is not allowed in cluster mode'),
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
