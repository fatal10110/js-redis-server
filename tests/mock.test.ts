import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { createRedisMock, createRedisServer, type RedisMock } from '../src/mock'

describe('createRedisServer', () => {
  test('defaults to 16 databases on an OS-assigned port', async () => {
    const handle = await createRedisServer()
    try {
      assert.strictEqual(handle.state.databases.length, 16)
      assert.strictEqual(handle.host, '127.0.0.1')
      assert.ok(handle.port > 0)
    } finally {
      await handle.close()
    }
  })

  test('honors an explicit databaseCount', async () => {
    const handle = await createRedisServer({ databaseCount: 4 })
    try {
      assert.strictEqual(handle.state.databases.length, 4)
    } finally {
      await handle.close()
    }
  })
})

describe('createRedisMock standalone', () => {
  let mock: RedisMock

  afterEach(async () => {
    await mock?.close()
  })

  test('exposes connection helpers', async () => {
    mock = await createRedisMock()
    assert.strictEqual(mock.host, '127.0.0.1')
    assert.ok(mock.port > 0)
    assert.strictEqual(mock.url, `redis://127.0.0.1:${mock.port}`)
    assert.deepStrictEqual(mock.connectionOptions(), {
      host: '127.0.0.1',
      port: mock.port,
    })
    assert.deepStrictEqual(mock.clusterNodes(), [
      { host: '127.0.0.1', port: mock.port },
    ])
    assert.ok(mock.state)
    assert.strictEqual(mock.nodes, undefined)
  })

  test('seeds then flushes', async () => {
    mock = await createRedisMock()
    await mock.seed([{ key: 'k', type: 'string', value: 'v' }])
    assert.strictEqual(
      mock.state!.getDatabase(0).getString(Buffer.from('k'))?.toString(),
      'v',
    )
    await mock.flush()
    assert.strictEqual(
      mock.state!.getDatabase(0).getString(Buffer.from('k')),
      null,
    )
  })
})

describe('createRedisMock cluster', () => {
  let mock: RedisMock

  afterEach(async () => {
    await mock?.close()
  })

  test('exposes cluster node list and escape hatch', async () => {
    mock = await createRedisMock({ cluster: { masters: 3 } })
    assert.strictEqual(mock.clusterNodes().length, 3)
    assert.ok(mock.nodes)
    assert.strictEqual(mock.state, undefined)
  })

  test('seeds keys onto their slot-owning master', async () => {
    mock = await createRedisMock({ cluster: { masters: 3 } })
    await mock.seed([{ key: 'user:1', type: 'string', value: 'alice' }])

    const key = Buffer.from('user:1')
    const owner = mock.nodes!.find(
      node => node.server.getDatabase(0).getString(key) !== null,
    )
    assert.ok(owner, 'expected exactly one master to hold the seeded key')
    assert.strictEqual(
      owner!.server.getDatabase(0).getString(key)?.toString(),
      'alice',
    )
  })
})
