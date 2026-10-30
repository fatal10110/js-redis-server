import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection, respText } from './raw-connection'

// SELECT inside a script moves only the script (#566): real Redis runs a
// script's commands on its own client, which starts each run on the caller's
// database. The caller's database, its later commands and MONITOR's view of
// them are untouched. Byte for byte against redis-server 6.2.14, 7.0.15,
// 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0, 9.0.0.
const testRunner = new TestRunner()
const legacy = activeProfile === 'redis-6.2'

const bulk = (value: string) => `$${Buffer.byteLength(value)}\r\n${value}\r\n`

describe(`SELECT inside scripts (${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) connection.close()
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    return connection
  }

  async function send(
    connection: RawRedisConnection,
    ...args: string[]
  ): Promise<string> {
    connection.write(commandFrame(...args))
    return (await connection.readRawFrame()).toString('binary')
  }

  async function clientDb(connection: RawRedisConnection): Promise<string> {
    const info = await send(connection, 'CLIENT', 'INFO')
    return / db=(\d+) /.exec(info)?.[1] ?? info
  }

  const key = (name: string) => `script-select:${name}:${randomKey()}`

  test('EVAL: the caller keeps its database', async () => {
    const connection = await connect()
    const k = key('k')
    const p = key('p')
    assert.strictEqual(await send(connection, 'SELECT', '3'), '+OK\r\n')
    assert.strictEqual(
      await send(
        connection,
        'EVAL',
        "redis.call('select','5') redis.call('set',KEYS[1],'in5') return redis.call('get',KEYS[1])",
        '1',
        k,
      ),
      bulk('in5'),
    )
    assert.strictEqual(await clientDb(connection), '3')
    assert.strictEqual(await send(connection, 'SET', p, '1'), '+OK\r\n')

    // A later script starts from the caller's database too.
    assert.strictEqual(
      await send(
        connection,
        'EVAL',
        "return redis.call('get',KEYS[1])",
        '1',
        p,
      ),
      bulk('1'),
    )

    assert.strictEqual(await send(connection, 'SELECT', '5'), '+OK\r\n')
    assert.strictEqual(await send(connection, 'GET', k), bulk('in5'))
    assert.strictEqual(await send(connection, 'GET', p), '$-1\r\n')
    assert.strictEqual(await send(connection, 'SELECT', '3'), '+OK\r\n')
    assert.strictEqual(await send(connection, 'GET', p), bulk('1'))
    assert.strictEqual(await send(connection, 'GET', k), '$-1\r\n')
  })

  test('the script keeps the database it selected last', async () => {
    const connection = await connect()
    const z = key('z')
    assert.strictEqual(await send(connection, 'SELECT', '3'), '+OK\r\n')
    assert.strictEqual(
      await send(
        connection,
        'EVAL',
        "redis.call('select','5') redis.call('select','0') return redis.call('set',KEYS[1],'z')",
        '1',
        z,
      ),
      '+OK\r\n',
    )
    assert.strictEqual(await clientDb(connection), '3')
    assert.strictEqual(await send(connection, 'GET', z), '$-1\r\n')
    assert.strictEqual(await send(connection, 'SELECT', '0'), '+OK\r\n')
    assert.strictEqual(await send(connection, 'GET', z), bulk('z'))
  })

  test('EVALSHA: the caller keeps its database', async () => {
    const connection = await connect()
    const p = key('p')
    const script = "redis.call('select','7') return redis.call('incr',KEYS[1])"
    const sha = createHash('sha1').update(script).digest('hex')
    assert.strictEqual(
      await send(connection, 'SCRIPT', 'LOAD', script),
      bulk(sha),
    )
    assert.strictEqual(await send(connection, 'SELECT', '2'), '+OK\r\n')
    assert.strictEqual(await send(connection, 'EVALSHA', sha, '1', p), ':1\r\n')
    assert.strictEqual(await clientDb(connection), '2')
    assert.strictEqual(await send(connection, 'GET', p), '$-1\r\n')
    assert.strictEqual(await send(connection, 'SELECT', '7'), '+OK\r\n')
    assert.strictEqual(await send(connection, 'GET', p), bulk('1'))
  })

  test('MULTI: a queued SELECT, then a script that selects', async () => {
    const connection = await connect()
    const cnt = key('cnt')
    const cnt2 = key('cnt2')
    assert.strictEqual(await send(connection, 'MULTI'), '+OK\r\n')
    assert.strictEqual(await send(connection, 'SELECT', '2'), '+QUEUED\r\n')
    assert.strictEqual(
      await send(
        connection,
        'EVAL',
        "redis.call('select','4') return redis.call('incr',KEYS[1])",
        '1',
        cnt,
      ),
      '+QUEUED\r\n',
    )
    assert.strictEqual(await send(connection, 'INCR', cnt2), '+QUEUED\r\n')
    assert.strictEqual(
      await send(connection, 'EXEC'),
      '*3\r\n+OK\r\n:1\r\n:1\r\n',
    )
    assert.strictEqual(await clientDb(connection), '2')
    assert.strictEqual(await send(connection, 'GET', cnt2), bulk('1'))
    assert.strictEqual(await send(connection, 'GET', cnt), '$-1\r\n')
    assert.strictEqual(await send(connection, 'SELECT', '4'), '+OK\r\n')
    assert.strictEqual(await send(connection, 'GET', cnt), bulk('1'))
  })

  test(
    'FCALL: the caller keeps its database',
    { skip: legacy && 'functions are 7.0+' },
    async () => {
      const connection = await connect()
      const fk = key('fk')
      const p = key('p')
      const library = `script_select_${randomKey().replace(/\W/g, '_')}`
      assert.strictEqual(
        await send(
          connection,
          'FUNCTION',
          'LOAD',
          `#!lua name=${library}\nredis.register_function('${library}_f', function(keys) redis.call('select','6') return redis.call('set',keys[1],'v') end)`,
        ),
        bulk(library),
      )
      try {
        assert.strictEqual(await send(connection, 'SELECT', '3'), '+OK\r\n')
        assert.strictEqual(
          await send(connection, 'FCALL', `${library}_f`, '1', fk),
          '+OK\r\n',
        )
        assert.strictEqual(await clientDb(connection), '3')
        assert.strictEqual(await send(connection, 'SET', p, '1'), '+OK\r\n')
        assert.strictEqual(await send(connection, 'GET', p), bulk('1'))
        assert.strictEqual(await send(connection, 'GET', fk), '$-1\r\n')
        assert.strictEqual(await send(connection, 'SELECT', '6'), '+OK\r\n')
        assert.strictEqual(await send(connection, 'GET', fk), bulk('v'))
        assert.strictEqual(await send(connection, 'GET', p), '$-1\r\n')
      } finally {
        await send(connection, 'FUNCTION', 'DELETE', library)
      }
    },
  )

  test('MONITOR: script lines carry its database, the caller keeps its own', async () => {
    const monitor = await connect()
    const actor = await connect()
    const k = key('k')
    const p = key('p')
    assert.strictEqual(await send(monitor, 'MONITOR'), '+OK\r\n')
    assert.strictEqual(await send(actor, 'SELECT', '3'), '+OK\r\n')
    assert.strictEqual(
      await send(
        actor,
        'EVAL',
        "redis.call('select','5') return redis.call('set',KEYS[1],'v')",
        '1',
        k,
      ),
      '+OK\r\n',
    )
    assert.strictEqual(await send(actor, 'SET', p, '1'), '+OK\r\n')

    // SELECT, EVAL, the script's select and set, then SET.
    const lines: string[] = []
    for (let i = 0; i < 5; i++) {
      lines.push(respText(await monitor.readFrame()))
    }
    const line = (pattern: RegExp) => {
      const found = lines.find(candidate => pattern.test(candidate))
      assert.ok(found, `${pattern} in ${JSON.stringify(lines)}`)
      return found
    }
    assert.match(line(/"EVAL"/), / \[3 [^\]]+\] "EVAL"/)
    assert.match(line(/"set" "[^"]+:k:/), / \[5 lua\] "set"/)
    assert.match(line(/"SET" "[^"]+:p:/), / \[3 [^\]]+\] "SET"/)
  })
})
