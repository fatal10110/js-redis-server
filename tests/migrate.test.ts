import { after, describe, test } from 'node:test'
import assert from 'node:assert'
import { createServer, type Server, type Socket } from 'node:net'
import { ClientSession, type RedisValue } from '../src/internal'
import { buildClusterNodes } from '../src/cluster'
import { createRedisCommandExecutor } from '../src/commands'
import { RedisServerState } from '../src/state'
import { MigrateSocketCache } from '../src/commands/migrate-socket'
import type { CompatibilitySpec } from '../src/core/compatibility/profile'

// #205: MIGRATE's wire traffic to the target, as migrateCommand (redis 7.2.4
// cluster.c:6818-7163) writes and reads it, checked against a scripted fake
// target: AUTH / SELECT / RESTORE in one write, SELECT only when the cached
// socket last selected another database (none at all for db -1 on a fresh
// one), RESTORE-ASKING from a cluster node, one retry on a socket that fails
// before the first reply but none after a timeout, and partial success.

function show(value: RedisValue): string {
  switch (value.kind) {
    case 'error':
      return `-${value.code ? `${value.code} ` : ''}${value.message}`
    case 'simple-string':
      return `+${value.value}`
    case 'bulk-string':
      return value.value === null ? 'nil' : value.value.toString()
    case 'integer':
      return `:${value.value}`
    case 'array':
      return `[${value.items.map(show).join(',')}]`
    default:
      return JSON.stringify(value)
  }
}

type Reply = (
  command: string[],
  connection: number,
) => string | null | { answer: string; afterMs: number }

/**
 * A target that parses each command and answers what `reply` returns (no
 * answer for null; `close` drops the connection; `afterMs` delays the
 * answer), recording the commands per
 * connection.
 */
async function fakeTarget(reply: Reply) {
  const connections: string[][][] = []
  const sockets: Socket[] = []
  let closed = 0
  const server: Server = createServer(socket => {
    sockets.push(socket)
    const index = connections.length
    const commands: string[][] = []
    connections.push(commands)
    let input = Buffer.alloc(0)
    socket.on('data', chunk => {
      input = Buffer.concat([input, chunk])
      for (;;) {
        const parsed = parseCommand(input)
        if (!parsed) return
        input = input.subarray(parsed.length)
        commands.push(parsed.command)
        const answer = reply(parsed.command, index)
        if (answer === 'close') {
          socket.destroy()
          return
        }
        if (answer === null) continue
        if (typeof answer === 'string') socket.write(answer)
        else setTimeout(() => socket.write(answer.answer), answer.afterMs)
      }
    })
    socket.on('error', () => {})
    socket.on('close', () => {
      closed++
    })
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('no port')
  return {
    port: String(address.port),
    connections,
    closed: () => closed,
    close: () =>
      new Promise<void>(resolve => {
        for (const socket of sockets) socket.destroy()
        server.close(() => resolve())
      }),
  }
}

function parseCommand(
  input: Buffer,
): { command: string[]; length: number } | null {
  const text = input.toString('latin1')
  if (!text.startsWith('*')) return null
  let pos = text.indexOf('\r\n')
  if (pos < 0) return null
  const count = Number(text.slice(1, pos))
  pos += 2
  const command: string[] = []
  for (let i = 0; i < count; i++) {
    const end = text.indexOf('\r\n', pos)
    if (end < 0) return null
    const length = Number(text.slice(pos + 1, end))
    if (text.length < end + 2 + length + 2) return null
    command.push(text.slice(end + 2, end + 2 + length))
    pos = end + 2 + length + 2
  }
  return { command, length: pos }
}

function standalone(compatibility: CompatibilitySpec = 'redis-8.0') {
  const state = new RedisServerState({ databaseCount: 16, compatibility })
  const executor = createRedisCommandExecutor({ compatibility: state.profile })
  const session = new ClientSession({ server: state, executor })
  const run = async (...args: string[]): Promise<string> => {
    const [name, ...rest] = args.map(arg => Buffer.from(arg))
    return show((await session.execute(name, rest)).value)
  }
  return { state, run }
}

const closers: Array<() => unknown> = []
after(async () => {
  for (const close of closers) await close()
})

async function setup(reply: Reply, compatibility?: CompatibilitySpec) {
  const target = await fakeTarget(reply)
  const source = standalone(compatibility)
  closers.push(() => source.state.close(), target.close)
  return { target, ...source }
}

const okToAll: Reply = () => '+OK\r\n'

describe('MIGRATE wire traffic (#205)', () => {
  test('AUTH, SELECT and RESTORE with the TTL and REPLACE, in order', async () => {
    const { target, run } = await setup(okToAll)
    await run('SET', 'k', 'v')
    await run('PEXPIRE', 'k', '50000')
    await run('SET', 'p', 'w')
    assert.strictEqual(
      await run(
        'MIGRATE',
        '127.0.0.1',
        target.port,
        '',
        '9',
        '1000',
        'REPLACE',
        'AUTH2',
        'u',
        'pw',
        'AUTH',
        'pw2',
        'KEYS',
        'k',
        'missing',
        'p',
      ),
      '+OK',
    )
    const [commands] = target.connections
    // AUTH2's username survives the later AUTH, which replaces the password.
    assert.deepStrictEqual(commands[0], ['AUTH', 'u', 'pw2'])
    assert.deepStrictEqual(commands[1], ['SELECT', '9'])
    assert.strictEqual(commands[2][0], 'RESTORE')
    assert.strictEqual(commands[2][1], 'k')
    const ttl = Number(commands[2][2])
    assert.ok(ttl > 49000 && ttl <= 50000, `ttl ${ttl}`)
    assert.strictEqual(commands[2][4], 'REPLACE')
    assert.deepStrictEqual(
      [commands[3][1], commands[3][2], commands[3].length],
      ['p', '0', 5],
    )
    assert.strictEqual(commands.length, 4)
    assert.strictEqual(await run('EXISTS', 'k', 'p'), ':0')
  })

  test('the cached socket skips SELECT for the database it last selected', async () => {
    const { target, run } = await setup(okToAll)
    await run('SET', 'a', '1')
    await run('SET', 'b', '1')
    await run('SET', 'c', '1')
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'a', '3', '1000'),
      '+OK',
    )
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'b', '3', '1000', 'COPY'),
      '+OK',
    )
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'c', '4', '1000'),
      '+OK',
    )
    assert.strictEqual(target.connections.length, 1)
    assert.deepStrictEqual(
      target.connections[0].map(command => command[0] + ' ' + command[1]),
      ['SELECT 3', 'RESTORE a', 'RESTORE b', 'SELECT 4', 'RESTORE c'],
    )
    // COPY kept b.
    assert.strictEqual(await run('EXISTS', 'a', 'b', 'c'), ':1')
  })

  test('db -1 on a fresh socket sends no SELECT', async () => {
    const { target, run } = await setup(okToAll)
    await run('SET', 'a', '1')
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'a', '-1', '1000'),
      '+OK',
    )
    assert.deepStrictEqual(
      target.connections[0].map(command => command[0]),
      ['RESTORE'],
    )
  })

  test('a target error resets the cached database and keeps the key', async () => {
    let fail = true
    const { target, run } = await setup(command =>
      command[0] === 'RESTORE' && fail
        ? '-BUSYKEY Target key name already exists.\r\n'
        : '+OK\r\n',
    )
    await run('SET', 'a', '1')
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'a', '2', '1000'),
      '-ERR Target instance replied with error: BUSYKEY Target key name already exists.',
    )
    assert.strictEqual(await run('EXISTS', 'a'), ':1')
    fail = false
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'a', '2', '1000'),
      '+OK',
    )
    assert.deepStrictEqual(
      target.connections[0].map(command => command[0]),
      ['SELECT', 'RESTORE', 'SELECT', 'RESTORE'],
    )
  })

  test('partial success: acknowledged keys move, the first error answers', async () => {
    const { target, run } = await setup(command =>
      command[1] === 'b' ? '-ERR boom\r\n' : '+OK\r\n',
    )
    for (const key of ['a', 'b', 'c']) await run('SET', key, '1')
    assert.strictEqual(
      await run(
        'MIGRATE',
        '127.0.0.1',
        target.port,
        '',
        '0',
        '1000',
        'KEYS',
        'a',
        'b',
        'c',
      ),
      '-ERR Target instance replied with error: ERR boom',
    )
    assert.strictEqual(await run('EXISTS', 'a'), ':0')
    assert.strictEqual(await run('EXISTS', 'b'), ':1')
    assert.strictEqual(await run('EXISTS', 'c'), ':0')
  })

  test('a connection dropped before the first reply is retried once', async () => {
    const { target, run } = await setup((_command, connection) =>
      connection === 0 ? 'close' : '+OK\r\n',
    )
    await run('SET', 'a', '1')
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'a', '0', '1000'),
      '+OK',
    )
    assert.strictEqual(target.connections.length, 2)
    assert.strictEqual(await run('EXISTS', 'a'), ':0')
  })

  test('a drop after some replies is not retried', async () => {
    const { target, run } = await setup(command =>
      command[1] === 'b' ? 'close' : '+OK\r\n',
    )
    await run('SET', 'a', '1')
    await run('SET', 'b', '1')
    assert.strictEqual(
      await run(
        'MIGRATE',
        '127.0.0.1',
        target.port,
        '',
        '0',
        '1000',
        'KEYS',
        'a',
        'b',
      ),
      '-IOERR error or timeout reading to target instance',
    )
    assert.strictEqual(target.connections.length, 1)
    assert.strictEqual(await run('EXISTS', 'a', 'b'), ':1')
  })

  test('a read timeout answers IOERR without a retry, keeping the key', async () => {
    const { target, run } = await setup(() => null)
    await run('SET', 'a', '1')
    const started = Date.now()
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'a', '0', '200'),
      '-IOERR error or timeout reading to target instance',
    )
    assert.ok(Date.now() - started >= 190)
    assert.strictEqual(target.connections.length, 1)
    assert.strictEqual(await run('EXISTS', 'a'), ':1')
  })

  test('a cluster node sends RESTORE-ASKING', async () => {
    const target = await fakeTarget(okToAll)
    closers.push(target.close)
    const { nodes } = buildClusterNodes({
      masters: 2,
      basePort: 7000,
      compatibility: 'redis-8.0',
    })
    closers.push(() => nodes.forEach(node => node.state.close()))
    const [a] = nodes
    const session = new ClientSession({
      server: a.state,
      executor: a.executor,
      nodeRole: a.role,
    })
    const run = async (...args: string[]) => {
      const [name, ...rest] = args.map(arg => Buffer.from(arg))
      return show((await session.execute(name, rest)).value)
    }
    // `b` hashes to slot 3300, served by the first master.
    assert.strictEqual(await run('SET', 'b', '1'), '+OK')
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'b', '0', '1000'),
      '+OK',
    )
    assert.deepStrictEqual(
      target.connections[0].map(command => command[0]),
      ['SELECT', 'RESTORE-ASKING'],
    )
  })

  test('scripts get NOKEY, and an error instead of the I/O', async () => {
    const { target, run } = await setup(okToAll)
    const script = `return redis.pcall('migrate', '127.0.0.1', '${target.port}', KEYS[1], '0', '1000')`
    assert.strictEqual(await run('EVAL', script, '1', 'missing'), '+NOKEY')
    await run('SET', 'a', '1')
    // Real servers block on the target here; the mock cannot from a script.
    assert.strictEqual(
      await run('EVAL', script, '1', 'a'),
      '-ERR MIGRATE cannot run asynchronously from scripts',
    )
    assert.strictEqual(target.connections.length, 0)
    assert.strictEqual(await run('EXISTS', 'a'), ':1')
  })

  test('a reply slower than the idle period arrives on the same socket', async () => {
    const { target, run } = await setup(() => ({
      answer: '+OK\r\n',
      afterMs: 300,
    }))
    const idleMs = MigrateSocketCache.idleMs
    MigrateSocketCache.idleMs = 50
    try {
      await run('SET', 'a', '1')
      assert.strictEqual(
        await run('MIGRATE', '127.0.0.1', target.port, 'a', '0', '2000'),
        '+OK',
      )
      assert.strictEqual(target.connections.length, 1)
      assert.deepStrictEqual(
        target.connections[0].map(command => command[0]),
        ['SELECT', 'RESTORE'],
      )
      assert.strictEqual(await run('EXISTS', 'a'), ':0')
      // Used longer than the idle period: closed once released.
      await new Promise<void>(resolve => {
        const check = setInterval(() => {
          if (target.closed() === 1) {
            clearInterval(check)
            resolve()
          }
        }, 5)
      })
    } finally {
      MigrateSocketCache.idleMs = idleMs
    }
  })

  test('closing the server closes its cached sockets', async () => {
    const { target, state, run } = await setup(okToAll)
    await run('SET', 'a', '1')
    assert.strictEqual(
      await run('MIGRATE', '127.0.0.1', target.port, 'a', '0', '1000'),
      '+OK',
    )
    assert.strictEqual(target.closed(), 0)
    state.close()
    await new Promise<void>(resolve => {
      const check = setInterval(() => {
        if (target.closed() === 1) {
          clearInterval(check)
          resolve()
        }
      }, 5)
    })
  })
})

describe('MIGRATE connect failures (#205)', () => {
  const cases: Array<[string, string, string]> = [
    ['an unresolvable host', 'nosuchhost.invalid', 'connecting to the client'],
    ['a negative port', '-1', 'connecting to the client'],
    ['an empty host', '', 'connecting to the client'],
    ['a refused port', '1', 'writing to target instance'],
    ['port 0', '0', 'writing to target instance'],
    ['a port that is not a number', 'abc', 'writing to target instance'],
  ]
  for (const [what, arg, wording] of cases) {
    test(what, async () => {
      const { state, run } = standalone()
      closers.push(() => state.close())
      await run('SET', 'a', '1')
      const isHost = what.includes('host')
      const reply = await run(
        'MIGRATE',
        isHost ? arg : '127.0.0.1',
        isHost ? '6379' : arg,
        'a',
        '0',
        '500',
      )
      assert.strictEqual(reply, `-IOERR error or timeout ${wording}`)
      assert.strictEqual(await run('EXISTS', 'a'), ':1')
    })
  }
})
