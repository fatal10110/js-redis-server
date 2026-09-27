import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * COMMAND / COMMAND INFO / COMMAND HELP byte for byte, per profile (#494).
 *
 * Flags, ACL categories and key-spec flags are status strings in sets, tips
 * bulk strings in a set, key specs maps (RESP3 shows the types; RESP2 renders
 * sets as arrays and maps as flat arrays). Redis 6.2's entries have 7 fields
 * and no QUIT; its COMMAND dispatches on the argument count alone and has no
 * LIST. An entry without subcommands ends in an empty set on Redis and an
 * empty array on Valkey.
 *
 * Every expectation follows `REDIS_COMPAT`, so the suite also runs against a
 * real server of the matching version (`TEST_BACKEND=real
 * REDIS_STANDALONE_PORT=<port>`). Checked against redis-server 6.2.24,
 * 7.0.15, 7.2.4, 7.4.4, 8.0.6 and valkey 8.0.11 / 9.0.6.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const valkey = profile.startsWith('valkey-')

type Protocol = 2 | 3

function encoders(protocol: Protocol) {
  const bulk = (value: string) => `$${Buffer.byteLength(value)}\r\n${value}\r\n`
  const int = (value: number) => `:${value}\r\n`
  const array = (items: string[]) => `*${items.length}\r\n${items.join('')}`
  const set = (items: string[]) =>
    `${protocol === 3 ? '~' : '*'}${items.length}\r\n${items.join('')}`
  const statusSet = (items: string[]) => set(items.map(item => `+${item}\r\n`))
  const map = (entries: Array<[string, string]>) =>
    protocol === 3
      ? `%${entries.length}\r\n${entries.flat().join('')}`
      : array(entries.flat())
  const nil = protocol === 3 ? '_\r\n' : '$-1\r\n'
  return { bulk, int, array, set, statusSet, map, nil }
}

type Spec = {
  notes?: string
  flags: string[]
  begin: ['index', number] | ['unknown']
  find: ['range', number, number, number] | ['unknown']
}

function commandEntry(
  protocol: Protocol,
  name: string,
  arity: number,
  flags: string[],
  range: [number, number, number],
  categories: string[],
  specs: Spec[],
): string {
  const e = encoders(protocol)
  const search = (type: string, spec: Array<[string, string]>) =>
    e.map([
      [e.bulk('type'), e.bulk(type)],
      [e.bulk('spec'), e.map(spec)],
    ])
  const keySpec = (spec: Spec) => {
    const entries: Array<[string, string]> = []
    if (spec.notes) {
      entries.push([e.bulk('notes'), e.bulk(spec.notes)])
    }
    entries.push(
      [e.bulk('flags'), e.statusSet(spec.flags)],
      [
        e.bulk('begin_search'),
        spec.begin[0] === 'index'
          ? search('index', [[e.bulk('index'), e.int(spec.begin[1])]])
          : search('unknown', []),
      ],
      [
        e.bulk('find_keys'),
        spec.find[0] === 'range'
          ? search('range', [
              [e.bulk('lastkey'), e.int(spec.find[1])],
              [e.bulk('keystep'), e.int(spec.find[2])],
              [e.bulk('limit'), e.int(spec.find[3])],
            ])
          : search('unknown', []),
      ],
    )
    return e.map(entries)
  }

  const fields = [
    e.bulk(name),
    e.int(arity),
    e.statusSet(flags),
    ...range.map(e.int),
    e.statusSet(categories),
  ]
  if (!legacy) {
    fields.push(
      e.set([]),
      e.set(specs.map(keySpec)),
      valkey ? e.array([]) : e.set([]),
    )
  }
  return e.array(fields)
}

function infoReplies(protocol: Protocol): string {
  const e = encoders(protocol)
  const get = commandEntry(
    protocol,
    'get',
    2,
    ['readonly', 'fast'],
    [1, 1, 1],
    ['@read', '@string', '@fast'],
    [
      {
        flags: ['RO', 'access'],
        begin: ['index', 1],
        find: ['range', 0, 1, 0],
      },
    ],
  )
  const sort = commandEntry(
    protocol,
    'sort',
    -2,
    ['write', 'denyoom', 'movablekeys'],
    [1, 1, 1],
    ['@write', '@set', '@sortedset', '@list', '@slow', '@dangerous'],
    [
      {
        flags: ['RO', 'access'],
        begin: ['index', 1],
        find: ['range', 0, 1, 0],
      },
      {
        notes:
          "For the optional BY/GET keyword. It is marked 'unknown' because the key names derive from the content of the key we sort",
        flags: ['RO', 'access'],
        begin: ['unknown'],
        find: ['unknown'],
      },
      {
        notes:
          "For the optional STORE keyword. It is marked 'unknown' because the keyword can appear anywhere in the argument array",
        flags: ['OW', 'update'],
        begin: ['unknown'],
        find: ['unknown'],
      },
    ],
  )
  return e.array([get, sort, e.nil])
}

/** A HELP reply: status lines plus the footer, `Print` from Redis 7.2. */
function helpReply(lines: string[]): string {
  const footer =
    profile === 'redis-6.2' || profile === 'redis-7.0'
      ? '    Prints this help.'
      : '    Print this help.'
  const all = [...lines, 'HELP', footer]
  return `*${all.length}\r\n${all.map(line => `+${line}\r\n`).join('')}`
}

const LEGACY_HELP = [
  'COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
  '(no subcommand)',
  '    Return details about all Redis commands.',
  'COUNT',
  '    Return the total number of commands in this Redis server.',
  'GETKEYS <full-command>',
  '    Return the keys from a full Redis command.',
  'INFO [<command-name> ...]',
  '    Return details about multiple Redis commands.',
]

const HELP = [
  'COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
  '(no subcommand)',
  '    Return details about all Redis commands.',
  'COUNT',
  '    Return the total number of commands in this Redis server.',
  'LIST',
  '    Return a list of all commands in this Redis server.',
  'INFO [<command-name> ...]',
  '    Return details about multiple Redis commands.',
  '    If no command names are given, documentation details for all',
  '    commands are returned.',
  'DOCS [<command-name> ...]',
  '    Return documentation details about multiple Redis commands.',
  '    If no command names are given, documentation details for all',
  '    commands are returned.',
  'GETKEYS <full-command>',
  '    Return the keys from a full Redis command.',
  'GETKEYSANDFLAGS <full-command>',
  '    Return the keys and the access flags from a full Redis command.',
]

function commandHelp(): string {
  if (legacy) {
    return helpReply(LEGACY_HELP)
  }
  // Valkey 8.0+ drops "Redis" from the text.
  return helpReply(
    valkey ? HELP.map(line => line.replace(/ Redis /g, ' ')) : HELP,
  )
}

const syntaxError = (subcommand: string) =>
  `-ERR Unknown subcommand or wrong number of arguments for '${subcommand}'. Try COMMAND HELP.\r\n`
const arityError = (subcommand: string) =>
  `-ERR wrong number of arguments for 'command|${subcommand}' command\r\n`

// [command, reply] rows of the 6.2 dispatch and its 7.0+ counterparts.
const DISPATCH: Array<[string[], string]> = legacy
  ? [
      [['COMMAND', 'GETKEYS'], syntaxError('GETKEYS')],
      [['COMMAND', 'COUNT', 'x'], syntaxError('COUNT')],
      [['COMMAND', 'HELP', 'x'], syntaxError('HELP')],
      [['COMMAND', 'LIST'], syntaxError('LIST')],
      [['COMMAND', 'list'], syntaxError('list')],
      [['COMMAND', 'DOCS'], syntaxError('DOCS')],
      [
        ['COMMAND', 'GETKEYSANDFLAGS', 'GET', 'k'],
        syntaxError('GETKEYSANDFLAGS'),
      ],
      [['COMMAND', 'INFO'], '*0\r\n'],
      [['COMMAND', 'GETKEYS', 'QUIT'], '-ERR Invalid command specified\r\n'],
    ]
  : [
      [['COMMAND', 'GETKEYS'], arityError('getkeys')],
      [['COMMAND', 'COUNT', 'x'], arityError('count')],
      [['COMMAND', 'HELP', 'x'], arityError('help')],
      [['COMMAND', 'LIST', 'x'], '-ERR syntax error\r\n'],
    ]

describe(`COMMAND INFO profile rows (${testRunner.getBackendName()}, ${profile})`, () => {
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

  async function connect(protocol: Protocol): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    if (protocol === 3) {
      connection.write(commandFrame('HELLO', '3'))
      await connection.readRawFrame()
    }
    return connection
  }

  for (const protocol of [2, 3] as const) {
    test(`COMMAND INFO entries (RESP${protocol})`, async () => {
      const conn = await connect(protocol)
      await expectReply(
        conn,
        ['COMMAND', 'INFO', 'get', 'SORT', 'nosuchcommand'],
        infoReplies(protocol),
      )
    })
  }

  // Redis 6.2 answers QUIT in its connection loop, before command lookup.
  test('QUIT has a command-table entry from 7.0', async () => {
    const conn = await connect(2)
    conn.write(commandFrame('COMMAND', 'INFO', 'quit'))
    const reply = (await conn.readRawFrame()).toString()
    if (legacy) {
      assert.strictEqual(reply, '*1\r\n$-1\r\n')
    } else {
      assert.ok(reply.startsWith('*1\r\n*10\r\n$4\r\nquit\r\n:-1\r\n'), reply)
    }
  })

  test('COMMAND HELP', async () => {
    const conn = await connect(2)
    await expectReply(conn, ['COMMAND', 'HELP'], commandHelp())
  })

  test('COMMAND subcommand dispatch', async () => {
    const conn = await connect(2)
    for (const [command, reply] of DISPATCH) {
      await expectReply(conn, command, reply)
    }
  })
})
