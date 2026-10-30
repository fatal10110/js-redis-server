import assert from 'node:assert'
import { setTimeout as delay } from 'node:timers/promises'
import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply, expectReplyPrefix, send } from '../raw-tcp/helpers'

/**
 * Numeric argument edges per profile (#577, #592). Checked against
 * redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0,
 * 9.0.0.
 *
 * - Blocking timeouts in seconds go through `string2ld()` (`strtold`): a
 *   leading or trailing space, an empty token, `0b11` or NaN is not a float;
 *   hex floats are fine. Redis 7.2 / Valkey 7.2 refuse a timeout whose end
 *   time passes LLONG_MAX ms (`inf`, `1e400`) as out of range; 6.2 / 7.0 cast
 *   it to a negative number. Redis 7.0.8 rounds the milliseconds up, so on
 *   6.2 a timeout under 1 ms blocks forever.
 * - SET / SETEX / PSETEX / GETEX / EXPIRE take int64 times and answer
 *   `invalid expire time` when the end time overflows; GETEX checks the time
 *   only once it found a string under the key.
 * - LPOS RANK takes int64; from 7.2 RANK -9223372036854775808 is out of
 *   range, before that it acts as RANK -1.
 * - XREAD BLOCK LLONG_MAX: accepted by 6.2 / 7.0, out of range from 7.2.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const pre72 = legacy || profile === 'redis-7.0'

const INT64_MAX = '9223372036854775807'
const INT64_MIN = '-9223372036854775808'
const NOT_FLOAT = '-ERR timeout is not a float or out of range\r\n'
const NEGATIVE = '-ERR timeout is negative\r\n'
const OUT_OF_RANGE = '-ERR timeout is out of range\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const SYNTAX = '-ERR syntax error\r\n'
const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const invalidExpire = (command: string) =>
  legacy
    ? `-ERR invalid expire time in ${command}\r\n`
    : `-ERR invalid expire time in '${command}' command\r\n`
const bulk = (value: string) => `$${Buffer.byteLength(value)}\r\n${value}\r\n`

type BlockingCommand = {
  name: string
  type: 'list' | 'zset'
  since70?: boolean
  args: (key: string, timeout: string) => string[]
}

const BLOCKING: BlockingCommand[] = [
  { name: 'BLPOP', type: 'list', args: (k, t) => ['BLPOP', k, t] },
  { name: 'BRPOP', type: 'list', args: (k, t) => ['BRPOP', k, t] },
  {
    name: 'BLMOVE',
    type: 'list',
    args: (k, t) => ['BLMOVE', k, `${k}:dst`, 'LEFT', 'RIGHT', t],
  },
  {
    name: 'BLMPOP',
    type: 'list',
    since70: true,
    args: (k, t) => ['BLMPOP', t, '1', k, 'LEFT'],
  },
  { name: 'BZPOPMIN', type: 'zset', args: (k, t) => ['BZPOPMIN', k, t] },
  { name: 'BZPOPMAX', type: 'zset', args: (k, t) => ['BZPOPMAX', k, t] },
  {
    name: 'BZMPOP',
    type: 'zset',
    since70: true,
    args: (k, t) => ['BZMPOP', t, '1', k, 'MIN'],
  },
]

describe(`Numeric edge profile rows (${testRunner.getBackendName()}, ${profile})`, () => {
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

  async function fill(
    conn: RawRedisConnection,
    command: BlockingCommand,
    key: string,
  ): Promise<void> {
    if (command.type === 'list') {
      await send(conn, ['RPUSH', key, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'])
    } else {
      await send(conn, ['ZADD', key, '1', 'a', '2', 'b', '3', 'c', '4', 'd'])
      await send(conn, ['ZADD', key, '5', 'e', '6', 'f', '7', 'g', '8', 'h'])
    }
  }

  for (const command of BLOCKING) {
    test(`${command.name} parses its timeout like string2ld`, async t => {
      if (command.since70 && legacy) {
        t.skip('not in Redis 6.2')
        return
      }
      const conn = await connect()
      const key = randomKey()
      await fill(conn, command, key)

      for (const timeout of [' 1', '1 ', '', '\t1', '0b11', 'nan', '1_0']) {
        await expectReply(conn, command.args(key, timeout), NOT_FLOAT)
      }
      for (const timeout of ['-1', '-inf', '-1e400']) {
        await expectReply(conn, command.args(key, timeout), NEGATIVE)
      }
      for (const timeout of ['inf', '+inf', 'infinity', '1e400', '1e19']) {
        await expectReply(
          conn,
          command.args(key, timeout),
          pre72 ? NEGATIVE : OUT_OF_RANGE,
        )
      }
      // Accepted, and the key has data: the command answers at once.
      for (const timeout of ['0x1p0', '0x1.8p1', '-0.0001', '.5', '1e-400']) {
        conn.write(commandFrame(...command.args(key, timeout)))
        const reply = (await conn.readRawFrame()).toString()
        assert.match(
          reply,
          /^[*$][1-9]/,
          `${timeout}: ${JSON.stringify(reply)}`,
        )
      }
    })
  }

  test('a bad timeout beats WRONGTYPE and fails inside EXEC', async () => {
    const conn = await connect()
    const key = randomKey()
    await send(conn, ['SET', key, 'v'])
    await expectReply(conn, ['BLPOP', key, ' 1'], NOT_FLOAT)
    await expectReply(
      conn,
      ['BZPOPMIN', key, '1e400'],
      pre72 ? NEGATIVE : OUT_OF_RANGE,
    )
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['BLPOP', key, ' 1'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], `*1\r\n${NOT_FLOAT}`)
  })

  test('a script runs the timeout parse from 7.2', async t => {
    if (pre72) {
      t.skip('BLPOP is not allowed from scripts before 7.2')
      return
    }
    const conn = await connect()
    const key = randomKey()
    await send(conn, ['RPUSH', key, 'a'])
    const script = "return redis.pcall('BLPOP', KEYS[1], ARGV[1])"
    await expectReply(conn, ['EVAL', script, '1', key, ' 1'], NOT_FLOAT)
    await expectReply(conn, ['EVAL', script, '1', key, '1e400'], OUT_OF_RANGE)
  })

  test('a timeout past the 32-bit timer range still blocks', async () => {
    const blocked = await connect()
    const writer = await connect()
    const key = randomKey()
    blocked.write(commandFrame('BLPOP', key, '100000000'))
    await delay(200)
    await expectReply(writer, ['RPUSH', key, 'x'], ':1\r\n')
    assert.strictEqual(
      (await blocked.readRawFrame()).toString(),
      `*2\r\n${bulk(key)}${bulk('x')}`,
    )
  })

  for (const timeout of ['0.0001', '0.001']) {
    test(`a timeout of ${timeout} s blocks forever only on 6.2`, async () => {
      const blocked = await connect()
      const writer = await connect()
      const key = randomKey()
      blocked.write(commandFrame('BLPOP', key, timeout))
      await delay(300)
      await expectReply(writer, ['RPUSH', key, 'x'], ':1\r\n')
      assert.strictEqual(
        (await blocked.readRawFrame()).toString(),
        legacy ? `*2\r\n${bulk(key)}${bulk('x')}` : '*-1\r\n',
      )
    })
  }

  test('a timeout past the 32-bit timer range does not block in MULTI', async () => {
    const conn = await connect()
    const list = randomKey()
    const stream = randomKey()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['BLPOP', list, '100000000'], '+QUEUED\r\n')
    await expectReply(
      conn,
      ['XREAD', 'BLOCK', '2147483648', 'STREAMS', stream, '$'],
      '+QUEUED\r\n',
    )
    await expectReply(conn, ['EXEC'], '*2\r\n*-1\r\n*-1\r\n')
    await expectReply(conn, ['PING'], '+PONG\r\n')
  })

  test('the milliseconds come from long double arithmetic', async () => {
    // Inside MULTI a timeout of 0 answers a null array instead of blocking.
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    const timeouts = [
      '-0.001',
      '-1e-3',
      '-0.002',
      '9223372036854775',
      '9223372036854775.807',
    ]
    for (const timeout of timeouts) {
      await expectReply(conn, ['BLPOP', key, timeout], '+QUEUED\r\n')
    }
    const wrapped = pre72 ? '*-1\r\n' : OUT_OF_RANGE
    await expectReply(
      conn,
      ['EXEC'],
      `*5\r\n*-1\r\n*-1\r\n${NEGATIVE}${wrapped}${wrapped}`,
    )
  })

  test('XREAD BLOCK LLONG_MAX is out of range from 7.2', async () => {
    const conn = await connect()
    const key = randomKey()
    await send(conn, ['XADD', key, '1-1', 'f', 'v'])
    const args = ['XREAD', 'BLOCK', INT64_MAX, 'STREAMS', key, '0']
    if (pre72) {
      await expectReplyPrefix(conn, args, '*1\r\n')
    } else {
      await expectReply(conn, args, OUT_OF_RANGE)
    }
    await expectReply(
      conn,
      ['XREAD', 'BLOCK', '9223372036854775808', 'STREAMS', key, '0'],
      '-ERR timeout is not an integer or out of range\r\n',
    )
  })

  test('SET and SETEX refuse an int64 expire time that overflows', async () => {
    const conn = await connect()
    const key = randomKey()
    for (const [unit, value] of [
      ['EX', INT64_MAX],
      ['EX', '9223372036854775'],
      ['PX', INT64_MAX],
      ['EXAT', '9223372036854776'],
      ['EX', INT64_MIN],
    ]) {
      await expectReply(
        conn,
        ['SET', key, 'v', unit!, value!],
        invalidExpire('set'),
      )
    }
    await expectReply(
      conn,
      ['SET', key, 'v', 'EX', INT64_MAX, 'NX'],
      invalidExpire('set'),
    )
    await expectReply(
      conn,
      ['SET', key, 'v', 'EX', '9223372036854775808'],
      NOT_INTEGER,
    )
    await expectReply(
      conn,
      ['SETEX', key, INT64_MAX, 'v'],
      invalidExpire('setex'),
    )
    await expectReply(
      conn,
      ['PSETEX', key, INT64_MAX, 'v'],
      invalidExpire('psetex'),
    )
    for (const [unit, value] of [
      ['PX', '9223372036854775'],
      ['EXAT', '9223372036854775'],
      ['PXAT', INT64_MAX],
    ]) {
      await expectReply(conn, ['SET', key, 'v', unit!, value!], '+OK\r\n')
      await expectReplyPrefix(conn, ['PTTL', key], ':92')
    }
  })

  test('EXPIRE family refuses an int64 time that overflows', async () => {
    const conn = await connect()
    const key = randomKey()
    const missing = randomKey()
    await send(conn, ['SET', key, 'v'])
    await expectReply(conn, ['EXPIRE', key, INT64_MAX], invalidExpire('expire'))
    await expectReply(
      conn,
      ['EXPIRE', missing, INT64_MAX],
      invalidExpire('expire'),
    )
    await expectReply(conn, ['EXPIRE', key, INT64_MIN], invalidExpire('expire'))
    await expectReply(
      conn,
      ['EXPIRE', key, '9223372036854775'],
      invalidExpire('expire'),
    )
    await expectReply(
      conn,
      ['PEXPIRE', key, INT64_MAX],
      invalidExpire('pexpire'),
    )
    await expectReply(
      conn,
      ['EXPIREAT', key, '9223372036854776'],
      invalidExpire('expireat'),
    )
    await expectReply(conn, ['EXPIREAT', key, '9223372036854775'], ':1\r\n')
    await expectReply(conn, ['PEXPIREAT', key, INT64_MAX], ':1\r\n')
    await expectReplyPrefix(conn, ['PTTL', key], ':92')
    await expectReply(conn, ['EXPIRE', key, '9223372036854775808'], NOT_INTEGER)
    await expectReply(conn, ['PEXPIREAT', key, INT64_MIN], ':1\r\n')
    await expectReply(conn, ['EXISTS', key], ':0\r\n')
  })

  test('GETEX checks the expire time only on a string key', async () => {
    const conn = await connect()
    const missing = randomKey()
    for (const [unit, value] of [
      ['EX', '0'],
      ['EX', '-1'],
      ['PX', '0'],
      ['EXAT', '0'],
      ['PXAT', '0'],
      ['EX', INT64_MAX],
      ['EX', 'abc'],
    ]) {
      await expectReply(conn, ['GETEX', missing, unit!, value!], '$-1\r\n')
    }
    await expectReply(conn, ['GETEX', missing, 'EX', '0', 'PERSIST'], SYNTAX)
    await expectReply(conn, ['GETEX', missing, 'EX'], SYNTAX)

    const key = randomKey()
    await send(conn, ['SET', key, 'v'])
    await expectReply(
      conn,
      ['GETEX', key, 'EX', INT64_MAX],
      invalidExpire('getex'),
    )
    await expectReply(
      conn,
      ['GETEX', key, 'EX', INT64_MIN],
      invalidExpire('getex'),
    )
    await expectReply(
      conn,
      ['GETEX', key, 'PX', INT64_MAX],
      invalidExpire('getex'),
    )
    await expectReply(conn, ['GETEX', key, 'EX', 'abc'], NOT_INTEGER)
    await expectReply(conn, ['GETEX', key, 'EX', INT64_MAX, 'PX', '1'], SYNTAX)
    await expectReply(conn, ['GETEX', key, 'PXAT', INT64_MAX], bulk('v'))
    await expectReplyPrefix(conn, ['PTTL', key], ':92')

    const list = randomKey()
    await send(conn, ['RPUSH', list, 'a'])
    await expectReply(conn, ['GETEX', list, 'EX', '0'], WRONGTYPE)
  })

  test('LPOS takes int64 RANK, COUNT and MAXLEN', async () => {
    const conn = await connect()
    const key = randomKey()
    const missing = randomKey()
    await send(conn, ['RPUSH', key, 'a', 'b', 'a', 'c', 'a'])
    const rankRange =
      '-ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807\r\n'
    if (pre72) {
      await expectReply(conn, ['LPOS', key, 'a', 'RANK', INT64_MIN], ':4\r\n')
      await expectReply(
        conn,
        ['LPOS', key, 'a', 'RANK', INT64_MIN, 'COUNT', '0'],
        '*3\r\n:4\r\n:2\r\n:0\r\n',
      )
      await expectReply(
        conn,
        ['LPOS', missing, 'a', 'RANK', INT64_MIN],
        '$-1\r\n',
      )
    } else {
      await expectReply(conn, ['LPOS', key, 'a', 'RANK', INT64_MIN], rankRange)
      await expectReply(
        conn,
        ['LPOS', missing, 'a', 'RANK', INT64_MIN],
        rankRange,
      )
    }
    await expectReply(
      conn,
      ['LPOS', key, 'a', 'RANK', '-9223372036854775807'],
      '$-1\r\n',
    )
    await expectReply(
      conn,
      ['LPOS', key, 'a', 'RANK', '-9223372036854775807', 'COUNT', '0'],
      '*0\r\n',
    )
    await expectReply(conn, ['LPOS', key, 'a', 'RANK', INT64_MAX], '$-1\r\n')
    await expectReply(
      conn,
      ['LPOS', key, 'a', 'RANK', '9223372036854775808'],
      NOT_INTEGER,
    )
    await expectReply(conn, ['LPOS', key, 'c', 'MAXLEN', INT64_MAX], ':3\r\n')
    await expectReply(
      conn,
      ['LPOS', key, 'a', 'COUNT', INT64_MAX],
      '*3\r\n:0\r\n:2\r\n:4\r\n',
    )
  })
})
