import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  CommandExecutor,
  InMemoryConnectionTransport,
  RedisResult,
  RedisServerState,
  RedisValue,
  Resp2SessionAdapter,
  createRedisCommandExecutor,
  createRedisCommandRegistry,
} from '../src'

describe('new foundation commands', () => {
  test('supports PING, SELECT, SET, and GET through the built-in registry', async () => {
    const { session } = createSession({ databaseCount: 2 })

    assert.deepStrictEqual(
      await session.execute('ping', []),
      RedisResult.create(RedisValue.simpleString('PONG')),
    )
    assert.deepStrictEqual(
      await session.execute('ping', [Buffer.from('hello')]),
      RedisResult.create(RedisValue.bulkString(Buffer.from('hello'))),
    )

    assert.deepStrictEqual(
      await session.execute('set', [Buffer.from('key'), Buffer.from('v0')]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(
      await session.execute('get', [Buffer.from('key')]),
      RedisResult.create(RedisValue.bulkString(Buffer.from('v0'))),
    )

    await session.execute('select', [Buffer.from('1')])
    assert.deepStrictEqual(
      await session.execute('get', [Buffer.from('key')]),
      RedisResult.create(RedisValue.bulkString(null)),
    )
  })

  test('implements SET condition, GET, expiration, and KEEPTTL options', async () => {
    const { session, server } = createSession()
    const db = server.getDatabase(0)
    const key = Buffer.from('key')

    assert.deepStrictEqual(
      await session.execute('set', [key, Buffer.from('v0'), Buffer.from('NX')]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(
      await session.execute('set', [key, Buffer.from('v1'), Buffer.from('NX')]),
      RedisResult.nil(),
    )
    assert.deepStrictEqual(db.getString(key), Buffer.from('v0'))

    assert.deepStrictEqual(
      await session.execute('set', [
        key,
        Buffer.from('v1'),
        Buffer.from('XX'),
        Buffer.from('GET'),
      ]),
      RedisResult.create(RedisValue.bulkString(Buffer.from('v0'))),
    )
    assert.deepStrictEqual(db.getString(key), Buffer.from('v1'))

    assert.deepStrictEqual(
      await session.execute('set', [
        key,
        Buffer.from('ttl'),
        Buffer.from('PX'),
        Buffer.from('5000'),
      ]),
      RedisResult.ok(),
    )
    const expires = db.getExpiration(key)
    assert.strictEqual(expires.kind, 'expires')

    assert.deepStrictEqual(
      await session.execute('set', [
        key,
        Buffer.from('kept'),
        Buffer.from('KEEPTTL'),
      ]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(db.getExpiration(key), expires)
  })

  test('returns Redis errors for wrong type and invalid SET options', async () => {
    const { session, server } = createSession()
    const key = Buffer.from('list')
    server.getDatabase(0).updateList(key, list => {
      list.values.push(Buffer.from('a'))
    })

    assert.deepStrictEqual(
      await session.execute('get', [key]),
      RedisResult.error(
        'Operation against a key holding the wrong kind of value',
        'WRONGTYPE',
      ),
    )
    assert.deepStrictEqual(
      await session.execute('set', [
        key,
        Buffer.from('value'),
        Buffer.from('GET'),
      ]),
      RedisResult.error(
        'Operation against a key holding the wrong kind of value',
        'WRONGTYPE',
      ),
    )
    assert.deepStrictEqual(
      await session.execute('set', [
        Buffer.from('key'),
        Buffer.from('value'),
        Buffer.from('NX'),
        Buffer.from('XX'),
      ]),
      RedisResult.error('syntax error', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('set', [
        Buffer.from('key'),
        Buffer.from('value'),
        Buffer.from('EX'),
        Buffer.from('0'),
      ]),
      RedisResult.error('invalid expire time in set command', 'ERR'),
    )
  })

  test('implements MGET without failing on non-string values', async () => {
    const { session, server } = createSession()
    const db = server.getDatabase(0)

    db.setString(Buffer.from('a'), Buffer.from('A'))
    db.updateList(Buffer.from('b'), list => {
      list.values.push(Buffer.from('B'))
    })

    assert.deepStrictEqual(
      await session.execute('mget', [
        Buffer.from('a'),
        Buffer.from('b'),
        Buffer.from('missing'),
      ]),
      RedisResult.create(
        RedisValue.array([
          RedisValue.bulkString(Buffer.from('A')),
          RedisValue.bulkString(null),
          RedisValue.bulkString(null),
        ]),
      ),
    )
  })

  test('implements generic key commands on the new state API', async () => {
    const { session, server } = createSession({ databaseCount: 2 })
    const db = server.getDatabase(0)
    const key = Buffer.from('key')

    db.setString(key, Buffer.from('value'))
    assert.deepStrictEqual(
      await session.execute('exists', [key, key, Buffer.from('missing')]),
      RedisResult.create(RedisValue.integer(2)),
    )
    assert.deepStrictEqual(
      await session.execute('type', [key]),
      RedisResult.create(RedisValue.simpleString('string')),
    )
    assert.deepStrictEqual(
      await session.execute('dbsize', []),
      RedisResult.create(RedisValue.integer(1)),
    )
    assert.deepStrictEqual(
      await session.execute('ttl', [key]),
      RedisResult.create(RedisValue.integer(-1)),
    )

    assert.deepStrictEqual(
      await session.execute('expire', [key, Buffer.from('10')]),
      RedisResult.create(RedisValue.integer(1)),
    )
    assert.strictEqual(db.getExpiration(key).kind, 'expires')
    assert.deepStrictEqual(
      await session.execute('persist', [key]),
      RedisResult.create(RedisValue.integer(1)),
    )
    assert.deepStrictEqual(db.getExpiration(key), { kind: 'persistent' })

    assert.deepStrictEqual(
      await session.execute('expire', [key, Buffer.from('0')]),
      RedisResult.create(RedisValue.integer(1)),
    )
    assert.strictEqual(db.getType(key), null)

    db.setString(Buffer.from('a'), Buffer.from('A'))
    await session.execute('select', [Buffer.from('1')])
    server.getDatabase(1).setString(Buffer.from('b'), Buffer.from('B'))

    assert.deepStrictEqual(
      await session.execute('flushdb', []),
      RedisResult.ok(),
    )
    assert.strictEqual(server.getDatabase(1).size(), 0)
    assert.strictEqual(server.getDatabase(0).size(), 1)

    await session.execute('select', [Buffer.from('0')])
    assert.deepStrictEqual(
      await session.execute('flushall', []),
      RedisResult.ok(),
    )
    assert.strictEqual(server.getDatabase(0).size(), 0)
    assert.strictEqual(server.getDatabase(1).size(), 0)
  })

  test('runs built-in commands through the RESP2 session adapter', async () => {
    const { session } = createSession()
    const transport = new InMemoryConnectionTransport()
    const adapter = new Resp2SessionAdapter({ transport, session })
    const running = adapter.run()

    transport.feed(
      Buffer.concat([
        commandFrame('PING', 'hello'),
        commandFrame('SET', 'key', 'value'),
        commandFrame('MGET', 'key', 'missing'),
      ]),
    )
    transport.endRead()

    await running

    assert.strictEqual(
      transport.getWrittenBuffer().toString(),
      '$5\r\nhello\r\n+OK\r\n*2\r\n$5\r\nvalue\r\n$-1\r\n',
    )
  })
})

function createSession(options?: { databaseCount?: number }) {
  const server = new RedisServerState({
    databaseCount: options?.databaseCount ?? 1,
  })
  const registry = createRedisCommandRegistry()
  const executor = createRedisCommandExecutor()
  const session = new ClientSession({ server, executor })

  return { server, registry, executor, session }
}

function commandFrame(...items: string[]): Buffer {
  return Buffer.from(
    `*${items.length}\r\n${items
      .map(item => `$${Buffer.byteLength(item)}\r\n${item}\r\n`)
      .join('')}`,
  )
}
