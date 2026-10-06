import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  ClientSession,
  RedisServerState,
  createRedisCommandExecutor,
  encodeRedisValue,
  type CompatibilitySpec,
  type RedisResult,
} from '../src/internal'

function createServer(compatibility: CompatibilitySpec = 'redis-8.0') {
  const server = new RedisServerState({ compatibility, databaseCount: 16 })
  const executor = createRedisCommandExecutor({ compatibility: server.profile })
  const connect = (options: { localAddress?: string; fd?: number } = {}) =>
    new ClientSession({
      server,
      executor,
      clientAddress: '127.0.0.1:50000',
      ...options,
    })
  return { server, connect }
}

function buf(...values: string[]): Buffer[] {
  return values.map(value => Buffer.from(value))
}

function text(result: RedisResult): string {
  assert.strictEqual(result.value.kind, 'verbatim')
  assert.strictEqual(result.value.format, 'txt')
  return result.value.value.toString()
}

/** `key=value` pairs of one CLIENT INFO / LIST line, in order. */
function fields(line: string): Array<[string, string]> {
  return line
    .trimEnd()
    .split(' ')
    .map(field => {
      const at = field.indexOf('=')
      return [field.slice(0, at), field.slice(at + 1)]
    })
}

function field(line: string, name: string): string | undefined {
  return fields(line).find(([key]) => key === name)?.[1]
}

async function clientInfo(session: ClientSession): Promise<string> {
  return text(await session.execute('client', buf('INFO')))
}

/** The CLIENT LIST line of the session named `name`. */
async function listLine(
  observer: ClientSession,
  name: string,
): Promise<string> {
  const list = text(await observer.execute('client', buf('LIST')))
  const line = list.split('\n').find(entry => entry.includes(` name=${name} `))
  assert.ok(line, `no CLIENT LIST line for ${name} in ${JSON.stringify(list)}`)
  return line
}

async function infoSection(
  session: ClientSession,
  section: string,
): Promise<string> {
  return text(await session.execute('info', buf(section)))
}

function infoField(info: string, name: string): string | undefined {
  const line = info.split('\r\n').find(entry => entry.startsWith(`${name}:`))
  return line?.slice(name.length + 1)
}

// The field order of `catClientInfoString` in each version's networking.c
// (redis 6.2.14 / 7.0.15 / 7.2.4 / 7.4.4 / 8.0.0, valkey 8.0.0 / 9.0.0); the
// 7.0 list is also what redis-server 7.0.15 prints.
const REDIS_62 =
  'id addr laddr fd name age idle flags db sub psub multi qbuf qbuf-free argv-mem obl oll omem tot-mem events cmd user redir'
const REDIS_70 =
  'id addr laddr fd name age idle flags db sub psub ssub multi qbuf qbuf-free argv-mem multi-mem rbs rbp obl oll omem tot-mem events cmd user redir resp'
const REDIS_72 = `${REDIS_70} lib-name lib-ver`
const REDIS_74 =
  'id addr laddr fd name age idle flags db sub psub ssub multi watch qbuf qbuf-free argv-mem multi-mem rbs rbp obl oll omem tot-mem events cmd user redir resp lib-name lib-ver'
const REDIS_80 = `${REDIS_74} io-thread`
const VALKEY_80 = `${REDIS_74} tot-net-in tot-net-out tot-cmds`
const VALKEY_90 =
  'id addr laddr fd name age idle flags capa db sub psub ssub multi watch qbuf qbuf-free argv-mem multi-mem rbs rbp obl oll omem tot-mem events cmd user redir resp lib-name lib-ver tot-net-in tot-net-out tot-cmds'

describe('CLIENT INFO / CLIENT LIST fields', () => {
  for (const [profile, expected] of [
    ['redis-6.2', REDIS_62],
    ['redis-7.0', REDIS_70],
    ['redis-7.2', REDIS_72],
    ['redis-7.4', REDIS_74],
    ['redis-8.0', REDIS_80],
    ['valkey-8.0', VALKEY_80],
    ['valkey-9.0', VALKEY_90],
  ] as const) {
    test(`${profile}: the fields and their order`, async () => {
      const session = createServer(profile).connect()
      const line = await clientInfo(session)
      assert.ok(line.endsWith('\n'), 'each client line ends in a newline')
      assert.strictEqual(
        fields(line)
          .map(([key]) => key)
          .join(' '),
        expected,
      )
    })
  }

  test('lib-name and lib-ver are printed empty before CLIENT SETINFO', async () => {
    const session = createServer('redis-7.2').connect()
    let line = await clientInfo(session)
    assert.strictEqual(field(line, 'lib-name'), '')
    assert.strictEqual(field(line, 'lib-ver'), '')

    await session.execute('client', buf('SETINFO', 'LIB-NAME', 'mylib'))
    await session.execute('client', buf('SETINFO', 'LIB-VER', '1.2.3'))
    line = await clientInfo(session)
    assert.strictEqual(field(line, 'lib-name'), 'mylib')
    assert.strictEqual(field(line, 'lib-ver'), '1.2.3')
  })

  test('laddr is the listening address and fd the socket descriptor', async () => {
    const { connect } = createServer()
    const tcp = connect({ localAddress: '127.0.0.1:6400', fd: 9 })
    const line = await clientInfo(tcp)
    assert.strictEqual(field(line, 'laddr'), '127.0.0.1:6400')
    assert.strictEqual(field(line, 'fd'), '9')

    // Without a socket, Redis's own spelling for "no descriptor".
    const socketless = connect()
    assert.strictEqual(field(await clientInfo(socketless), 'fd'), '-1')
  })

  test('cmd is the last command, by its command-table name', async () => {
    const { connect } = createServer('redis-7.0')
    const observer = connect()
    const client = connect()
    await client.execute('client', buf('SETNAME', 'subject'))

    // CLIENT INFO reports itself.
    assert.strictEqual(field(await clientInfo(client), 'cmd'), 'client|info')

    const lastCmd = async () =>
      field(await listLine(observer, 'subject'), 'cmd') ?? ''
    await client.execute('GET', buf('k'))
    assert.strictEqual(await lastCmd(), 'get')

    // Recorded at lookup, before the arity check rejects the command.
    await client.execute('get', [])
    assert.strictEqual(await lastCmd(), 'get')

    // Lookup failures leave no command (7.0.15: cmd=NULL).
    await client.execute('nosuchcommand', [])
    assert.strictEqual(await lastCmd(), 'NULL')
    await client.execute('client', buf('BOGUS'))
    assert.strictEqual(await lastCmd(), 'NULL')

    await client.execute('config', buf('get', 'maxmemory'))
    assert.strictEqual(await lastCmd(), 'config|get')

    await client.execute('multi', [])
    await client.execute('set', buf('k', 'v'))
    assert.strictEqual(await lastCmd(), 'set')
    await client.execute('exec', [])
    assert.strictEqual(await lastCmd(), 'exec')

    // A fresh connection has not sent anything yet.
    const fresh = connect()
    await observer.execute('client', buf('LIST'))
    assert.strictEqual(fresh.lastCommand, null)
  })

  test('6.2 records the container itself, whatever the subcommand', async () => {
    const { connect } = createServer('redis-6.2')
    const client = connect()
    assert.strictEqual(field(await clientInfo(client), 'cmd'), 'client')
    await client.execute('client', buf('BOGUS'))
    assert.strictEqual(client.lastCommand, 'client')
  })

  test('flags, multi and watch follow the connection state', async () => {
    const { connect } = createServer('redis-7.4')
    const observer = connect()
    const client = connect()
    await client.execute('client', buf('SETNAME', 'subject'))
    const line = () => listLine(observer, 'subject')

    assert.strictEqual(field(await line(), 'flags'), 'N')
    assert.strictEqual(field(await line(), 'multi'), '-1')
    assert.strictEqual(field(await line(), 'watch'), '0')

    await client.execute('watch', buf('w1', 'w2'))
    assert.strictEqual(field(await line(), 'watch'), '2')
    await observer.execute('set', buf('w1', 'changed'))
    assert.strictEqual(field(await line(), 'flags'), 'd')

    await client.execute('multi', [])
    await client.execute('get', buf('a'))
    await client.execute('get', buf('b'))
    assert.strictEqual(field(await line(), 'flags'), 'xd')
    assert.strictEqual(field(await line(), 'multi'), '2')
    await client.execute('exec', [])
    assert.strictEqual(field(await line(), 'flags'), 'N')
    assert.strictEqual(field(await line(), 'watch'), '0')

    await client.execute('client', buf('NO-EVICT', 'on'))
    assert.strictEqual(field(await line(), 'flags'), 'e')
    await client.execute('client', buf('NO-EVICT', 'off'))

    await client.execute('subscribe', buf('ch'))
    assert.strictEqual(field(await line(), 'flags'), 'P')
  })

  test('a parked blocking command shows flags=b and counts in blocked_clients', async () => {
    const { connect } = createServer()
    const observer = connect()
    const waiter = connect()
    await waiter.execute('client', buf('SETNAME', 'waiter'))
    assert.strictEqual(
      infoField(await infoSection(observer, 'clients'), 'blocked_clients'),
      '0',
    )

    const reply = waiter.execute('blpop', buf('q', '0'))
    await new Promise(resolve => setImmediate(resolve))

    assert.strictEqual(
      infoField(await infoSection(observer, 'clients'), 'blocked_clients'),
      '1',
    )
    const line = await listLine(observer, 'waiter')
    assert.strictEqual(field(line, 'flags'), 'b')
    assert.strictEqual(field(line, 'cmd'), 'blpop')

    await observer.execute('rpush', buf('q', 'v'))
    await reply
    assert.strictEqual(
      infoField(await infoSection(observer, 'clients'), 'blocked_clients'),
      '0',
    )
    assert.strictEqual(field(await listLine(observer, 'waiter'), 'flags'), 'N')
  })

  test('a blocking command inside MULTI never blocks', async () => {
    const { connect } = createServer()
    const client = connect()
    await client.execute('multi', [])
    await client.execute('blpop', buf('q', '0'))
    await client.execute('exec', [])
    assert.strictEqual(
      infoField(await infoSection(client, 'clients'), 'blocked_clients'),
      '0',
    )
  })
})

describe('INFO keyspace', () => {
  test('counts the keys with a TTL and averages their remaining TTL', async () => {
    const session = createServer('redis-7.2').connect()
    assert.strictEqual(await infoSection(session, 'keyspace'), '# Keyspace\r\n')

    await session.execute('set', buf('plain', 'v'))
    await session.execute('set', buf('a', 'v', 'PX', '100000'))
    await session.execute('set', buf('b', 'v', 'PX', '200000'))
    const info = await infoSection(session, 'keyspace')
    const match = /^db0:keys=3,expires=2,avg_ttl=(\d+)$/m.exec(info)
    assert.ok(match, info)
    const avgTtl = Number(match[1])
    assert.ok(avgTtl <= 150000 && avgTtl > 149000, `avg_ttl ${avgTtl}`)

    await session.execute('persist', buf('a'))
    assert.match(
      await infoSection(session, 'keyspace'),
      /^db0:keys=3,expires=1,avg_ttl=\d+$/m,
    )
  })

  test('drops expired keys and reports each database', async () => {
    const session = createServer('redis-7.2').connect()
    await session.execute('set', buf('gone', 'v', 'PX', '1'))
    await session.execute('select', buf('3'))
    await session.execute('set', buf('kept', 'v'))
    await new Promise(resolve => setTimeout(resolve, 5))

    assert.strictEqual(
      await infoSection(session, 'keyspace'),
      '# Keyspace\r\ndb3:keys=1,expires=0,avg_ttl=0\r\n',
    )
  })

  test('Redis 7.4+ counts the hashes with field TTLs as subexpiry', async () => {
    const session = createServer('redis-7.4').connect()
    await session.execute('hset', buf('h1', 'f', 'v', 'g', 'v'))
    await session.execute('hset', buf('h2', 'f', 'v'))
    await session.execute('hexpire', buf('h1', '100', 'FIELDS', '2', 'f', 'g'))
    assert.strictEqual(
      await infoSection(session, 'keyspace'),
      '# Keyspace\r\ndb0:keys=2,expires=0,avg_ttl=0,subexpiry=1\r\n',
    )
  })

  test('Valkey 9.0 names the same count keys_with_volatile_items', async () => {
    const session = createServer('valkey-9.0').connect()
    await session.execute('hset', buf('h1', 'f', 'v'))
    await session.execute('hexpire', buf('h1', '100', 'FIELDS', '1', 'f'))
    assert.strictEqual(
      await infoSection(session, 'keyspace'),
      '# Keyspace\r\ndb0:keys=1,expires=0,avg_ttl=0,keys_with_volatile_items=1\r\n',
    )
  })

  test('Valkey 8.0 has neither', async () => {
    const session = createServer('valkey-8.0').connect()
    await session.execute('set', buf('k', 'v'))
    assert.strictEqual(
      await infoSection(session, 'keyspace'),
      '# Keyspace\r\ndb0:keys=1,expires=0,avg_ttl=0\r\n',
    )
  })
})

describe('INFO / CLIENT INFO / CLIENT LIST reply type', () => {
  test('a txt verbatim string on RESP3, a bulk string on RESP2', async () => {
    const session = createServer().connect()
    for (const args of [
      buf('info', 'bogus'),
      buf('info', 'keyspace'),
      buf('client', 'INFO'),
      buf('client', 'LIST'),
    ]) {
      const [command, ...rest] = args
      const result = await session.execute(command, rest)
      const body = text(result)
      assert.strictEqual(
        encodeRedisValue(result.value, { version: 3 }).toString(),
        `=${Buffer.byteLength(body) + 4}\r\ntxt:${body}\r\n`,
      )
      assert.strictEqual(
        encodeRedisValue(result.value, { version: 2 }).toString(),
        `$${Buffer.byteLength(body)}\r\n${body}\r\n`,
      )
    }
  })
})
