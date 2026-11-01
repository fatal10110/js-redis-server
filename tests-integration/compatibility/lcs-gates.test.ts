import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * LCS and STRALGO by profile (#206). LCS exists from Redis 7.0 (Valkey has it
 * from its 7.2 fork); STRALGO LCS, its predecessor, exists only on Redis 6.2
 * and was removed in 7.0. Checked against redis-server 6.2.14, 7.0.15, 7.2.4,
 * 7.4.4, 8.0.0 and valkey-server 8.0.0, 9.0.0. The LCS replies themselves are
 * in raw-tcp/lcs.test.ts; STRALGO runs the same algorithm (6.2 t_string.c
 * stralgoLCS), so only its own parsing is covered here.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const docs72 = !legacy && profile !== 'redis-7.0'

function unknownCommand(name: string, args: string[]): string {
  return legacy
    ? `-ERR unknown command \`${name}\`, with args beginning with: ${args.map(arg => `\`${arg}\`, `).join('')}\r\n`
    : `-ERR unknown command '${name}', with args beginning with: ${args.map(arg => `'${arg}' `).join('')}\r\n`
}

function bulk(text: string): string {
  return `$${Buffer.byteLength(text)}\r\n${text}\r\n`
}

function docsArgument(
  name: string,
  type: string,
  options: { keySpec?: number; token?: string },
): string {
  const fields = [bulk('name') + bulk(name), bulk('type') + bulk(type)]
  if (docs72) fields.push(bulk('display_text') + bulk(name))
  if (options.keySpec !== undefined) {
    fields.push(bulk('key_spec_index') + `:${options.keySpec}\r\n`)
  }
  if (options.token) {
    fields.push(bulk('token') + bulk(options.token))
    fields.push(bulk('flags') + '*1\r\n+optional\r\n')
  }
  return `*${fields.length * 2}\r\n${fields.join('')}`
}

const LCS_COMMAND_INFO =
  '*1\r\n*10\r\n$3\r\nlcs\r\n:-3\r\n*1\r\n+readonly\r\n:1\r\n:2\r\n:1\r\n' +
  '*3\r\n+@read\r\n+@string\r\n+@slow\r\n*0\r\n' +
  '*1\r\n*6\r\n$5\r\nflags\r\n*2\r\n+RO\r\n+access\r\n' +
  '$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n*2\r\n$5\r\nindex\r\n:1\r\n' +
  '$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n' +
  '*6\r\n$7\r\nlastkey\r\n:1\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n'

const STRALGO_COMMAND_INFO =
  '*1\r\n*7\r\n$7\r\nstralgo\r\n:-2\r\n*2\r\n+readonly\r\n+movablekeys\r\n' +
  ':0\r\n:0\r\n:0\r\n*3\r\n+@read\r\n+@string\r\n+@slow\r\n'

// ohmytext / mynewtext, as LCS answers it.
const IDX_BODY =
  '$7\r\nmatches\r\n*2\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n' +
  '*2\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n$3\r\nlen\r\n:6\r\n'

const SYNTAX = '-ERR syntax error\r\n'
const STRINGS_OR_KEYS = '-ERR Either use STRINGS or KEYS\r\n'
const NO_STRINGS =
  '-ERR Please specify two strings: STRINGS or KEYS options are mandatory\r\n'
const LEN_WITH_IDX =
  '-ERR If you want both the length and indexes, please just use IDX.\r\n'
const NOT_STRINGS = '-ERR The specified keys must contain string values\r\n'

describe(`LCS / STRALGO by profile (${testRunner.getBackendName()}, ${profile})`, () => {
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

  test('LCS exists from 7.0, STRALGO only on 6.2', async () => {
    const conn = await connect()
    const a = `lcs:${randomKey()}:a`
    const b = `lcs:${randomKey()}:b`
    await expectReply(conn, ['SET', a, 'ohmytext'], '+OK\r\n')
    await expectReply(conn, ['SET', b, 'mynewtext'], '+OK\r\n')

    if (legacy) {
      await expectReply(
        conn,
        ['LCS', 'k1', 'k2'],
        unknownCommand('LCS', ['k1', 'k2']),
      )
      await expectReply(
        conn,
        ['STRALGO', 'LCS', 'KEYS', a, b],
        '$6\r\nmytext\r\n',
      )
    } else {
      await expectReply(conn, ['LCS', a, b], '$6\r\nmytext\r\n')
      await expectReply(
        conn,
        ['STRALGO', 'LCS', 'KEYS', 'k1', 'k2'],
        unknownCommand('STRALGO', ['LCS', 'KEYS', 'k1', 'k2']),
      )
    }
  })

  test('COMMAND INFO, DOCS and GETKEYS', async () => {
    const conn = await connect()
    if (legacy) {
      await expectReply(conn, ['COMMAND', 'INFO', 'lcs'], '*1\r\n$-1\r\n')
      await expectReply(
        conn,
        ['COMMAND', 'INFO', 'stralgo'],
        STRALGO_COMMAND_INFO,
      )
      await expectReply(
        conn,
        ['COMMAND', 'GETKEYS', 'LCS', 'a', 'b'],
        '-ERR Invalid command specified\r\n',
      )
      await expectReply(
        conn,
        ['COMMAND', 'GETKEYS', 'STRALGO', 'LCS', 'IDX', 'KEYS', 'a', 'b'],
        '*2\r\n$1\r\na\r\n$1\r\nb\r\n',
      )
      await expectReply(
        conn,
        ['COMMAND', 'GETKEYS', 'STRALGO', 'LCS', 'KEYS', 'a', 'b', 'KEYS'],
        '*2\r\n$1\r\na\r\n$1\r\nb\r\n',
      )
      for (const tail of [
        ['STRINGS', 'a', 'b', 'KEYS', 'c', 'd'],
        ['KEYS', 'a'],
      ]) {
        await expectReply(
          conn,
          ['COMMAND', 'GETKEYS', 'STRALGO', 'LCS', ...tail],
          '-ERR Invalid arguments specified for command\r\n',
        )
      }
      return
    }

    await expectReply(conn, ['COMMAND', 'INFO', 'lcs'], LCS_COMMAND_INFO)
    await expectReply(
      conn,
      ['COMMAND', 'GETKEYS', 'LCS', 'a', 'b', 'IDX'],
      '*2\r\n$1\r\na\r\n$1\r\nb\r\n',
    )
    const summary = docs72
      ? 'Finds the longest common substring.'
      : 'Find longest common substring'
    const args = [
      docsArgument('key1', 'key', { keySpec: 0 }),
      docsArgument('key2', 'key', { keySpec: 0 }),
      docsArgument('len', 'pure-token', { token: 'LEN' }),
      docsArgument('idx', 'pure-token', { token: 'IDX' }),
      docsArgument(docs72 ? 'min-match-len' : 'len', 'integer', {
        token: 'MINMATCHLEN',
      }),
      docsArgument('withmatchlen', 'pure-token', { token: 'WITHMATCHLEN' }),
    ]
    await expectReply(
      conn,
      ['COMMAND', 'DOCS', 'lcs'],
      '*2\r\n$3\r\nlcs\r\n*10\r\n' +
        bulk('summary') +
        bulk(summary) +
        bulk('since') +
        bulk('7.0.0') +
        bulk('group') +
        bulk('string') +
        bulk('complexity') +
        bulk(
          'O(N*M) where N and M are the lengths of s1 and s2, respectively',
        ) +
        bulk('arguments') +
        `*${args.length}\r\n${args.join('')}`,
    )
  })

  test('STRALGO LCS STRINGS / KEYS and its options (6.2)', async t => {
    if (!legacy) {
      t.skip('STRALGO exists only on Redis 6.2')
      return
    }
    const conn = await connect()
    const a = `lcs:${randomKey()}:a`
    const b = `lcs:${randomKey()}:b`
    const list = `lcs:${randomKey()}:list`
    await expectReply(conn, ['SET', a, 'ohmytext'], '+OK\r\n')
    await expectReply(conn, ['SET', b, 'mynewtext'], '+OK\r\n')
    await expectReply(conn, ['RPUSH', list, 'x'], ':1\r\n')

    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'KEYS', a, b, 'IDX'],
      `*4\r\n${IDX_BODY}`,
    )
    await expectReply(conn, ['STRALGO', 'LCS', 'KEYS', a, b, 'LEN'], ':6\r\n')
    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'STRINGS', 'ohmytext', 'mynewtext', 'IDX'],
      `*4\r\n${IDX_BODY}`,
    )
    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'IDX', 'KEYS', a, b],
      `*4\r\n${IDX_BODY}`,
    )
    await expectReply(
      conn,
      [
        'STRALGO',
        'LCS',
        'STRINGS',
        'abc',
        'abd',
        'IDX',
        'MINMATCHLEN',
        '-1',
        'WITHMATCHLEN',
      ],
      '*4\r\n$7\r\nmatches\r\n*1\r\n*3\r\n*2\r\n:0\r\n:1\r\n*2\r\n:0\r\n:1\r\n:2\r\n$3\r\nlen\r\n:2\r\n',
    )
    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'STRINGS', '', '', 'IDX'],
      '*4\r\n$7\r\nmatches\r\n*0\r\n$3\r\nlen\r\n:0\r\n',
    )
    await expectReply(
      conn,
      ['stralgo', 'lcs', 'keys', `${a}:missing`, `${b}:missing`],
      '$0\r\n\r\n',
    )

    await expectReply(
      conn,
      ['STRALGO'],
      "-ERR wrong number of arguments for 'stralgo' command\r\n",
    )
    await expectReply(conn, ['STRALGO', 'LCS'], NO_STRINGS)
    await expectReply(conn, ['STRALGO', 'LCS', 'IDX'], NO_STRINGS)
    await expectReply(conn, ['STRALGO', 'FOO', 'KEYS', a, b], SYNTAX)
    await expectReply(conn, ['STRALGO', 'LCS', 'FOO', a, b], SYNTAX)
    await expectReply(conn, ['STRALGO', 'LCS', 'KEYS', a], SYNTAX)
    await expectReply(conn, ['STRALGO', 'LCS', 'STRINGS', a], SYNTAX)
    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'KEYS', a, b, 'MINMATCHLEN'],
      SYNTAX,
    )
    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'KEYS', a, b, 'MINMATCHLEN', 'abc'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'STRINGS', 'a', 'b', 'KEYS', a, b],
      STRINGS_OR_KEYS,
    )
    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'KEYS', a, b, 'KEYS', a, b],
      STRINGS_OR_KEYS,
    )
    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'IDX', 'LEN', 'STRINGS', 'a', 'b'],
      LEN_WITH_IDX,
    )
    await expectReply(conn, ['STRALGO', 'LCS', 'KEYS', list, a], NOT_STRINGS)
    // KEYS looks its keys up as it is parsed: before the options after it,
    // after the ones before it.
    await expectReply(
      conn,
      ['STRALGO', 'LCS', 'KEYS', list, a, 'FOO'],
      NOT_STRINGS,
    )
    await expectReply(conn, ['STRALGO', 'LCS', 'FOO', 'KEYS', list, a], SYNTAX)
  })

  test('STRALGO on RESP3, from a script and inside MULTI (6.2)', async t => {
    if (!legacy) {
      t.skip('STRALGO exists only on Redis 6.2')
      return
    }
    const conn = await connect()
    const a = `lcs:${randomKey()}:a`
    await expectReply(conn, ['SET', a, 'ohmytext'], '+OK\r\n')

    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('STRALGO', 'LCS', 'KEYS', KEYS[1], KEYS[1], 'IDX')",
        '1',
        a,
      ],
      '*4\r\n$7\r\nmatches\r\n*1\r\n*2\r\n*2\r\n:0\r\n:7\r\n*2\r\n:0\r\n:7\r\n$3\r\nlen\r\n:8\r\n',
    )

    // Arity is -2, so a bare STRALGO LCS queues and fails inside EXEC.
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['STRALGO', 'LCS'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], `*1\r\n${NO_STRINGS}`)

    const resp3 = await connect()
    resp3.write(commandFrame('HELLO', '3'))
    await resp3.readRawFrame()
    await expectReply(
      resp3,
      ['STRALGO', 'LCS', 'STRINGS', 'ohmytext', 'mynewtext', 'IDX'],
      `%2\r\n${IDX_BODY}`,
    )
    await expectReply(
      resp3,
      ['STRALGO', 'LCS', 'STRINGS', '', '', 'IDX', 'WITHMATCHLEN'],
      '%2\r\n$7\r\nmatches\r\n*0\r\n$3\r\nlen\r\n:0\r\n',
    )
  })
})
