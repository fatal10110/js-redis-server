import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'

/**
 * XREAD / XREADGROUP with BLOCK from a script (#563). Up to Redis 7.2 /
 * Valkey 7.2 the option parser refuses BLOCK from a script, naming the
 * command as the script sent it, before it reads the timeout, the key or the
 * group. From Redis 7.4 / Valkey 8.0 the command runs and, with nothing to
 * read, answers the timeout reply at once, even with BLOCK 0. Byte for byte
 * against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server
 * 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const blockAllowed =
  profile !== 'redis-6.2' && profile !== 'redis-7.0' && profile !== 'redis-7.2'

const sha1 = (script: string) => createHash('sha1').update(script).digest('hex')

const refusal = (command: string) =>
  `ERR ${command} command is not allowed with BLOCK option from scripts`

/** The reply a one-line script aborting with `error` gets on this profile. */
function aborted(script: string, error: string): string {
  return legacy
    ? `-ERR Error running script (call to f_${sha1(script)}): @user_script:1: ${error}\r\n`
    : `-${error} script: ${sha1(script)}, on @user_script:1.\r\n`
}

/** The reply to one stream with the single entry `1-1 a b`. */
const entryReply = (key: string) =>
  `*1\r\n*2\r\n$${Buffer.byteLength(key)}\r\n${key}\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n`

describe(`XREAD / XREADGROUP BLOCK from scripts (${testRunner.getBackendName()}, ${profile})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) connection.close()
    await testRunner.cleanup()
  })

  async function connect(resp3 = false): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    if (resp3) {
      connection.write(commandFrame('HELLO', '3'))
      await connection.readRawFrame()
    }
    return connection
  }

  async function send(
    connection: RawRedisConnection,
    ...args: string[]
  ): Promise<string> {
    connection.write(commandFrame(...args))
    return (await connection.readRawFrame()).toString('binary')
  }

  /** A stream `key` holding `1-1 a b`, with group `g` at 0. */
  async function stream(connection: RawRedisConnection): Promise<string> {
    const key = `script-stream-block:${randomKey()}`
    assert.strictEqual(
      await send(connection, 'XADD', key, '1-1', 'a', 'b'),
      '$3\r\n1-1\r\n',
    )
    assert.strictEqual(
      await send(connection, 'XGROUP', 'CREATE', key, 'g', '0'),
      '+OK\r\n',
    )
    return key
  }

  const pcall = (call: string) => `return redis.pcall(${call})`

  test('redis.pcall', async () => {
    const connection = await connect()
    const key = await stream(connection)
    const evalKey = (script: string) =>
      send(connection, 'EVAL', script, '1', key)

    const cases: [string, string, string][] = [
      // [call, reply up to 7.2, reply from 7.4]
      [
        "'xread','block',100,'streams',KEYS[1],'0'",
        `-${refusal('xread')}\r\n`,
        entryReply(key),
      ],
      [
        `"XREAD","BLOCK",100,"streams",KEYS[1],"$"`,
        `-${refusal('XREAD')}\r\n`,
        '$-1\r\n',
      ],
      [
        "'xread','count',1,'block',0,'streams',KEYS[1],'$'",
        `-${refusal('xread')}\r\n`,
        '$-1\r\n',
      ],
      [
        "'xread','block','x','streams',KEYS[1],'0'",
        `-${refusal('xread')}\r\n`,
        '-ERR timeout is not an integer or out of range\r\n',
      ],
      [
        "'xread','block','-1','streams',KEYS[1],'0'",
        `-${refusal('xread')}\r\n`,
        '-ERR timeout is negative\r\n',
      ],
      [
        "'xreadgroup','group','nogrp','c','block',100,'streams',KEYS[1],'>'",
        `-${refusal('xreadgroup')}\r\n`,
        `-NOGROUP No such key '${key}' or consumer group 'nogrp' in XREADGROUP with GROUP option\r\n`,
      ],
      [
        "'XREADGROUP','GROUP','g','c','BLOCK',100,'STREAMS',KEYS[1],'>'",
        `-${refusal('XREADGROUP')}\r\n`,
        entryReply(key),
      ],
      // The group's entry is delivered now: nothing new to read.
      [
        "'xreadgroup','group','g','c','block',0,'streams',KEYS[1],'>'",
        `-${refusal('xreadgroup')}\r\n`,
        '$-1\r\n',
      ],
      // BLOCK after STREAMS is a stream id, not the option.
      [
        "'xread','streams',KEYS[1],'0','block',100",
        '-ERR Invalid stream ID specified as stream command argument\r\n',
        '-ERR Invalid stream ID specified as stream command argument\r\n',
      ],
    ]
    for (const [call, refused, allowed] of cases) {
      assert.strictEqual(
        await evalKey(pcall(call)),
        blockAllowed ? allowed : refused,
        call,
      )
    }

    // Refused before the key is looked up: a missing key and a key of
    // another type get the refusal too.
    const other = `${key}:string`
    assert.strictEqual(await send(connection, 'SET', other, 'v'), '+OK\r\n')
    for (const target of [other, `${key}:missing`]) {
      assert.strictEqual(
        await send(
          connection,
          'EVAL',
          pcall("'xread','block',100,'streams',KEYS[1],'$'"),
          '1',
          target,
        ),
        blockAllowed
          ? target === other
            ? '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
            : '$-1\r\n'
          : `-${refusal('xread')}\r\n`,
        target,
      )
    }

    assert.strictEqual(await send(connection, 'PING'), '+PONG\r\n')
  })

  test('redis.call', async () => {
    const connection = await connect()
    const key = await stream(connection)
    const script = "return redis.call('xread','block',0,'streams',KEYS[1],'$')"
    assert.strictEqual(
      await send(connection, 'EVAL', script, '1', key),
      blockAllowed ? '$-1\r\n' : aborted(script, refusal('xread')),
    )
    assert.strictEqual(await send(connection, 'PING'), '+PONG\r\n')
  })

  test('RESP3', async () => {
    const connection = await connect(true)
    const key = await stream(connection)
    const evalKey = (script: string) =>
      send(connection, 'EVAL', script, '1', key)
    assert.strictEqual(
      await evalKey(pcall("'xread','block',100,'streams',KEYS[1],'0'")),
      blockAllowed ? entryReply(key) : `-${refusal('xread')}\r\n`,
    )
    assert.strictEqual(
      await evalKey(pcall("'xread','block',0,'streams',KEYS[1],'$'")),
      blockAllowed ? '_\r\n' : `-${refusal('xread')}\r\n`,
    )
  })

  test('EVAL_RO', { skip: legacy && 'EVAL_RO is 7.0+' }, async () => {
    const connection = await connect()
    const key = await stream(connection)
    assert.strictEqual(
      await send(
        connection,
        'EVAL_RO',
        pcall("'xread','block',100,'streams',KEYS[1],'$'"),
        '1',
        key,
      ),
      blockAllowed ? '$-1\r\n' : `-${refusal('xread')}\r\n`,
    )
  })
})
