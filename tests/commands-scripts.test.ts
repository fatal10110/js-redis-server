import crypto from 'node:crypto'
import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  InMemoryConnectionTransport,
  RedisClusterTopology,
  RedisResult,
  RedisServerState,
  RedisValue,
  Resp2SessionAdapter,
  createClusterPolicy,
  createRedisCommandExecutor,
} from '../src'

describe('new script commands', () => {
  test('loads scripts into the server-wide script cache', async () => {
    const { session, server } = createSession()
    const script = Buffer.from('return "hello"')
    const sha = scriptSha(script)

    assert.deepStrictEqual(
      await session.execute('script', [Buffer.from('load'), script]),
      RedisResult.create(RedisValue.bulkString(Buffer.from(sha))),
    )
    assert.deepStrictEqual(server.scriptCache.get(sha), script)
  })

  test('evaluates Lua scripts with KEYS and ARGV', async () => {
    const { session, server } = createSession()
    const script = Buffer.from('return {KEYS[1], ARGV[1]}')
    const sha = scriptSha(script)

    assert.deepStrictEqual(
      await session.execute('eval', [
        script,
        Buffer.from('1'),
        Buffer.from('key'),
        Buffer.from('arg'),
      ]),
      RedisResult.create(
        RedisValue.array([
          RedisValue.bulkString(Buffer.from('key')),
          RedisValue.bulkString(Buffer.from('arg')),
        ]),
      ),
    )
    assert.deepStrictEqual(server.scriptCache.get(sha), script)
  })

  test('runs redis.call through the command executor', async () => {
    const { session, server } = createSession()
    const key = Buffer.from('script-key')
    const value = Buffer.from([0, 255, 1, 2, 3])

    assert.deepStrictEqual(
      await session.execute('eval', [
        Buffer.from('return redis.call("set", KEYS[1], ARGV[1])'),
        Buffer.from('1'),
        key,
        value,
      ]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(server.getDatabase(0).getString(key), value)
    assert.deepStrictEqual(
      await session.execute('eval', [
        Buffer.from('return redis.call("get", KEYS[1])'),
        Buffer.from('1'),
        key,
      ]),
      RedisResult.create(RedisValue.bulkString(value)),
    )
  })

  test('evaluates cached scripts through EVALSHA', async () => {
    const { session, server } = createSession()
    const script = Buffer.from('return ARGV[1]')
    const sha = server.scriptCache.load(script)

    assert.deepStrictEqual(
      await session.execute('evalsha', [
        Buffer.from(sha),
        Buffer.from('0'),
        Buffer.from('cached-value'),
      ]),
      RedisResult.create(RedisValue.bulkString(Buffer.from('cached-value'))),
    )
  })

  test('returns Redis errors for EVAL and EVALSHA failures', async () => {
    const { session } = createSession()
    const badScript = Buffer.from("return redis.set('x', 1)")
    const sha = scriptSha(badScript)

    assert.deepStrictEqual(
      await session.execute('evalsha', [
        Buffer.from('missing'),
        Buffer.from('0'),
      ]),
      RedisResult.error('No matching script. Please use EVAL.', 'NOSCRIPT'),
    )
    assert.deepStrictEqual(
      await session.execute('eval', [
        Buffer.from('return 1'),
        Buffer.from('2'),
        Buffer.from('only-one-key'),
      ]),
      RedisResult.error(
        `Number of keys can't be greater than number of args`,
        'ERR',
      ),
    )

    const result = await session.execute('eval', [badScript, Buffer.from('0')])
    assert.ok(result instanceof RedisResult)
    assert.strictEqual(result.value.kind, 'error')
    assert.strictEqual(result.value.code, 'ERR')
    assert.strictEqual(
      result.value.message,
      `user_script:1: attempt to call field 'set' (a nil value) script: ${sha}, on @user_script:1.`,
    )
  })

  test('checks script existence and flushes the script cache', async () => {
    const { session, server } = createSession()
    const first = Buffer.from('return "first"')
    const second = Buffer.from('return "second"')
    const firstSha = server.scriptCache.load(first)

    assert.deepStrictEqual(
      await session.execute('script', [
        Buffer.from('exists'),
        Buffer.from(firstSha),
        Buffer.from(scriptSha(second)),
      ]),
      RedisResult.create(
        RedisValue.array([RedisValue.integer(1), RedisValue.integer(0)]),
      ),
    )

    assert.deepStrictEqual(
      await session.execute('script', [Buffer.from('flush')]),
      RedisResult.ok(),
    )
    assert.strictEqual(server.scriptCache.exists(firstSha), false)
  })

  test('supports SCRIPT FLUSH mode, KILL, DEBUG, and HELP', async () => {
    const { session } = createSession()

    assert.deepStrictEqual(
      await session.execute('script', [
        Buffer.from('flush'),
        Buffer.from('ASYNC'),
      ]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(
      await session.execute('script', [Buffer.from('kill')]),
      RedisResult.ok(),
    )
    assert.deepStrictEqual(
      await session.execute('script', [
        Buffer.from('debug'),
        Buffer.from('SYNC'),
      ]),
      RedisResult.ok(),
    )

    const help = await session.execute('script', [Buffer.from('help')])
    assert.ok(help instanceof RedisResult)
    assert.strictEqual(help.value.kind, 'array')
    assert.ok(
      help.value.items.some(
        item =>
          item.kind === 'bulk-string' &&
          item.value?.toString().includes('SCRIPT <subcommand>'),
      ),
    )
  })

  test('returns Redis errors for script syntax and arity failures', async () => {
    const { session } = createSession()

    assert.deepStrictEqual(
      await session.execute('script', []),
      RedisResult.error(
        "wrong number of arguments for 'script' command",
        'ERR',
      ),
    )
    assert.deepStrictEqual(
      await session.execute('script', [Buffer.from('missing')]),
      RedisResult.error(
        "unknown subcommand 'missing'. Try SCRIPT HELP.",
        'ERR',
      ),
    )
    assert.deepStrictEqual(
      await session.execute('script', [Buffer.from('load')]),
      RedisResult.error(
        "wrong number of arguments for 'script|load' command",
        'ERR',
      ),
    )
    assert.deepStrictEqual(
      await session.execute('script', [
        Buffer.from('debug'),
        Buffer.from('invalid'),
      ]),
      RedisResult.error('Use SCRIPT DEBUG YES/SYNC/NO', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('script', [
        Buffer.from('debug'),
        Buffer.from('YES'),
        Buffer.from('extra'),
      ]),
      RedisResult.error(
        "wrong number of arguments for 'script|debug' command",
        'ERR',
      ),
    )
    assert.deepStrictEqual(
      await session.execute('script', [
        Buffer.from('flush'),
        Buffer.from('invalid'),
      ]),
      RedisResult.error('SCRIPT FLUSH only support SYNC|ASYNC option', 'ERR'),
    )
    assert.deepStrictEqual(
      await session.execute('script', [
        Buffer.from('flush'),
        Buffer.from('SYNC'),
        Buffer.from('extra'),
      ]),
      RedisResult.error('SCRIPT FLUSH only support SYNC|ASYNC option', 'ERR'),
    )
  })

  test('queues SCRIPT LOAD in transactions before mutating script cache', async () => {
    const { session, server } = createSession()
    const script = Buffer.from('return "queued"')
    const sha = scriptSha(script)

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('script', [Buffer.from('load'), script]),
      RedisResult.create(RedisValue.simpleString('QUEUED')),
    )
    assert.strictEqual(server.scriptCache.exists(sha), false)

    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.create(
        RedisValue.array([RedisValue.bulkString(Buffer.from(sha))]),
      ),
    )
    assert.deepStrictEqual(server.scriptCache.get(sha), script)
  })

  test('queues EVAL in transactions before executing scripts', async () => {
    const { session, server } = createSession()
    const key = Buffer.from('tx-script-key')
    const script = Buffer.from('return redis.call("set", KEYS[1], ARGV[1])')

    assert.deepStrictEqual(await session.execute('multi', []), RedisResult.ok())
    assert.deepStrictEqual(
      await session.execute('eval', [
        script,
        Buffer.from('1'),
        key,
        Buffer.from('value'),
      ]),
      RedisResult.create(RedisValue.simpleString('QUEUED')),
    )
    assert.strictEqual(server.getDatabase(0).getString(key), null)

    assert.deepStrictEqual(
      await session.execute('exec', []),
      RedisResult.create(RedisValue.array([RedisValue.simpleString('OK')])),
    )
    assert.deepStrictEqual(
      server.getDatabase(0).getString(key),
      Buffer.from('value'),
    )
  })

  test('runs redis.call through cluster policy inside Lua', async () => {
    const { session, topology } = createClusterSession()
    const key = findKeyOwnedBy(topology, 'remote')
    const slot = topology.calculateSlot(key)

    assert.deepStrictEqual(
      await session.execute('eval', [
        Buffer.from('return redis.call("get", ARGV[1])'),
        Buffer.from('0'),
        key,
      ]),
      RedisResult.error(`${slot} 127.0.0.1:7001`, 'MOVED'),
    )
  })

  test('runs SCRIPT commands through the RESP2 adapter', async () => {
    const { session } = createSession()
    const transport = new InMemoryConnectionTransport()
    const adapter = new Resp2SessionAdapter({ transport, session })
    const running = adapter.run()
    const script = 'return 1'
    const sha = scriptSha(Buffer.from(script))

    transport.feed(commandFrame('SCRIPT', 'LOAD', script))
    transport.endRead()

    await running

    assert.strictEqual(
      transport.getWrittenBuffer().toString(),
      `$40\r\n${sha}\r\n`,
    )
  })
})

function createSession() {
  const server = new RedisServerState()
  const executor = createRedisCommandExecutor()
  const session = new ClientSession({ server, executor })

  return { server, executor, session }
}

function createClusterSession() {
  const topology = new RedisClusterTopology([
    {
      id: 'local',
      role: 'master',
      host: '127.0.0.1',
      port: 7000,
      slots: [[0, 8191]],
    },
    {
      id: 'remote',
      role: 'master',
      host: '127.0.0.1',
      port: 7001,
      slots: [[8192, 16383]],
    },
  ])
  const server = new RedisServerState({ clusterTopology: topology })
  const executor = createRedisCommandExecutor({
    policies: [createClusterPolicy({ localNodeId: 'local' })],
  })
  const session = new ClientSession({ server, executor })

  return { server, executor, session, topology }
}

function findKeyOwnedBy(
  topology: RedisClusterTopology,
  nodeId: string,
): Buffer {
  for (let index = 0; index < 100000; index++) {
    const key = Buffer.from(`key-${nodeId}-${index}`)
    const slot = topology.calculateSlot(key)
    if (topology.nodeOwnsSlot(nodeId, slot)) {
      return key
    }
  }

  throw new Error(`Could not find key owned by ${nodeId}`)
}

function scriptSha(script: Buffer): string {
  return crypto.createHash('sha1').update(script).digest('hex')
}

function commandFrame(...items: string[]): Buffer {
  return Buffer.from(
    `*${items.length}\r\n${items
      .map(item => `$${Buffer.byteLength(item)}\r\n${item}\r\n`)
      .join('')}`,
  )
}
