import { asciiUpperCase } from '../core/ascii-case'
import { defineCommand, type KeyWithFlags } from '../core/command-definition'
import { t } from '../core/command-schema'
import type { CompatibilityProfile } from '../core/compatibility'
import type { RedisExecutionContext } from '../core/redis-context'
import { errors, RedisCommandError } from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import type { RedisDataValue } from '../state'
import { createDumpPayload, rdbFormatFor } from '../state/rdb/payload'
import { configuredEncodingLimits } from './config'
import { ok, parseInt64Token, simpleString } from './helpers'
import { MigrateConnection, migrateSocketCache } from './migrate-socket'

/**
 * MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE]
 * [AUTH password | AUTH2 username password] [KEYS key ...]
 * (cluster.c `migrateCommand`): DUMPs the keys, sends them to the target as
 * RESTORE (RESTORE-ASKING from a cluster node) over a TCP connection it
 * caches per `host:port`, and deletes every key the target acknowledged
 * unless COPY is given. The target can be a real server or another mock.
 *
 * The I/O awaits while the command keeps the server's turn, so other clients
 * of the source wait as they do on a real server (which blocks), and a
 * MIGRATE to the server itself times out reading, as on a real one. In-process
 * servers without a TCP listener (the socketless client mocks) cannot be
 * reached: dialing their synthetic addresses fails with an IOERR.
 */

type MigrateArgs = {
  host: Buffer
  port: Buffer
  db: Buffer
  timeout: Buffer
  copy: boolean
  replace: boolean
  username?: Buffer
  password?: Buffer
  keys: readonly Buffer[]
}

/**
 * The options from argv[6] on, in order, as `migrateCommand` reads them:
 * every syntax and KEYS error comes before the timeout and database are
 * parsed. AUTH2's username survives a later AUTH, which only replaces the
 * password, as on a real server.
 */
const migrateSchema = t.custom<MigrateArgs>(
  { min: 5, keys: [2] },
  (input, index) => {
    const [host, port, key, db, timeout] = input.slice(index, index + 5)
    let copy = false
    let replace = false
    let username: Buffer | undefined
    let password: Buffer | undefined
    let keys: readonly Buffer[] = [key]
    for (let j = index + 5; j < input.length; j++) {
      const token = asciiUpperCase(input[j].toString('latin1'))
      const moreArgs = input.length - 1 - j
      if (token === 'COPY') {
        copy = true
      } else if (token === 'REPLACE') {
        replace = true
      } else if (token === 'AUTH') {
        if (moreArgs < 1) throw errors.syntax()
        password = input[++j]
      } else if (token === 'AUTH2') {
        if (moreArgs < 2) throw errors.syntax()
        username = input[++j]
        password = input[++j]
      } else if (token === 'KEYS') {
        if (key.length !== 0) throw errors.migrateKeysNeedEmptyKey()
        keys = input.slice(j + 1)
        break
      } else {
        throw errors.syntax()
      }
    }
    return {
      value: {
        host,
        port,
        db,
        timeout,
        copy,
        replace,
        username,
        password,
        keys,
      },
      nextIndex: input.length,
    }
  },
)

const KEY_FLAGS = ['RW', 'access', 'delete'] as const

/**
 * `migrateGetKeys` (db.c): the key argument, or every argument after KEYS
 * when the key is empty. From Redis 7.0 (the key-spec rewrite) AUTH and
 * AUTH2 skip their arguments and KEYS after a non-empty key gives no keys;
 * 6.2 takes the first KEYS whatever precedes it, and only with an empty key.
 */
export function migrateGetKeys(
  argv: readonly Buffer[],
  profile?: CompatibilityProfile,
): KeyWithFlags[] {
  const rewritten = profile?.has('command.getkeysandflags') ?? true
  let first = 3
  let num = 1
  const skips: Record<string, number> = {
    COPY: 0,
    REPLACE: 0,
    AUTH: 1,
    AUTH2: 2,
  }
  for (let i = 6; i < argv.length; i++) {
    const token = asciiUpperCase(argv[i].toString('latin1'))
    if (!rewritten) {
      if (token === 'KEYS' && argv[3].length === 0) {
        first = i + 1
        num = argv.length - first
        break
      }
      continue
    }
    if (token === 'KEYS') {
      if (argv[3].length > 0) {
        num = 0
      } else {
        first = i + 1
        num = argv.length - first
      }
      break
    }
    i += skips[token] ?? 0
  }
  return argv.slice(first, first + num).map(key => ({ key, flags: KEY_FLAGS }))
}

type MigrateKey = {
  key: Buffer
  value: RedisDataValue
  expiresAt?: number
  payload: Buffer
}

/** C `atoi` of the port, wrapped to the 16 bits a socket address keeps. */
function portNumber(raw: Buffer): number {
  const match = /^[ \t\n\v\f\r]*([+-]?\d+)/.exec(raw.toString('latin1'))
  if (!match) return 0
  const value = Number(BigInt.asIntN(32, BigInt(match[1])))
  return value < 0 ? value : value & 0xffff
}

function bulkString(value: Buffer | string): Buffer[] {
  const bytes = typeof value === 'string' ? Buffer.from(value) : value
  return [Buffer.from(`$${bytes.length}\r\n`), bytes, Buffer.from('\r\n')]
}

function encodeCommand(parts: readonly (Buffer | string)[]): Buffer[] {
  return [Buffer.from(`*${parts.length}\r\n`), ...parts.flatMap(bulkString)]
}

const ioError = (what: 'writing' | 'reading') =>
  new RedisCommandError(`error or timeout ${what} to target instance`, 'IOERR')

const connectError = () =>
  new RedisCommandError('error or timeout connecting to the client', 'IOERR')

export const migrateCommand = defineCommand({
  name: 'migrate',
  schema: migrateSchema,
  flags: ['write'],
  rawKeys: migrateGetKeys,
  keys: args => args.keys,
  // Not `async`: everything up to the first byte sent answers synchronously,
  // so NOKEY and the argument errors stay what scripts get from real servers.
  execute: (args, ctx) => {
    let timeout = parseInt64Token(args.timeout)
    const dbid = parseInt64Token(args.db)
    if (timeout <= 0n) timeout = 1000n

    const now = Date.now()
    const format = rdbFormatFor(ctx.server.profile)
    const limits = configuredEncodingLimits(ctx.server)
    const keys: MigrateKey[] = []
    for (const key of args.keys) {
      const value = ctx.db.get(key)
      if (!value) {
        ctx.db.announceKeyMiss(key)
        continue
      }
      const expiration = ctx.db.getExpiration(key)
      keys.push({
        key,
        value,
        expiresAt:
          expiration.kind === 'expires' ? expiration.expiresAt : undefined,
        payload: createDumpPayload(value, format, limits),
      })
    }
    if (keys.length === 0) {
      return simpleString('NOKEY')
    }
    if (ctx.inScript) {
      // A script cannot wait for the target: real servers block on it, the
      // mock refuses before anything is sent.
      throw new RedisCommandError(
        'MIGRATE cannot run asynchronously from scripts',
      )
    }
    const timeoutMs =
      timeout > BigInt(MAX_TIMER_MS) ? MAX_TIMER_MS : Number(timeout)
    return migrateKeys(args, ctx, keys, timeoutMs, dbid, now)
  },
})

/**
 * One pass over the socket. `socket-error` closes the socket and is retried
 * once on a new one unless it timed out or `retry` is false (some reply was
 * already read); `done` ends the command, closing the socket first when a
 * read failed after the target had answered with an error.
 */
type Attempt =
  | { kind: 'done'; result: RedisResult | RedisCommandError; close: boolean }
  | {
      kind: 'socket-error'
      writeError: boolean
      timedOut: boolean
      retry: boolean
    }

/** setTimeout's ceiling; a longer MIGRATE timeout waits as if forever. */
const MAX_TIMER_MS = 2 ** 31 - 1

async function migrateKeys(
  args: MigrateArgs,
  ctx: RedisExecutionContext,
  keys: readonly MigrateKey[],
  timeout: number,
  dbid: bigint,
  now: number,
): Promise<RedisResult> {
  const cache = migrateSocketCache(ctx.server)
  const host = args.host.toString('latin1')
  const name = `${host}:${args.port.toString('latin1')}`
  let mayRetry = true
  for (;;) {
    let connection = cache.get(name)
    if (!connection) {
      connection = new MigrateConnection()
      const port = portNumber(args.port)
      const outcome =
        host.length === 0 || port < 0
          ? 'failed'
          : port === 0
            ? connection.refuse()
            : await connection.connect(host, port, timeout)
      if (
        outcome === 'failed' ||
        (outcome === 'timeout' &&
          ctx.server.profile.has('migrate.connect-timeout-error'))
      ) {
        connection.close()
        throw connectError()
      }
      cache.add(name, connection)
    }

    const attempt = await attemptMigrate(
      args,
      ctx,
      keys,
      connection,
      timeout,
      dbid,
      now,
    )
    if (attempt.kind === 'done') {
      if (attempt.close) cache.close(name)
      if (attempt.result instanceof RedisCommandError) throw attempt.result
      return attempt.result
    }
    cache.close(name)
    if (attempt.timedOut || !attempt.retry || !mayRetry) {
      throw ioError(attempt.writeError ? 'writing' : 'reading')
    }
    mayRetry = false
  }
}

async function attemptMigrate(
  args: MigrateArgs,
  ctx: RedisExecutionContext,
  keys: readonly MigrateKey[],
  connection: MigrateConnection,
  timeout: number,
  dbid: bigint,
  now: number,
): Promise<Attempt> {
  const { password, username, copy, replace } = args
  const out: Buffer[] = []
  if (password) {
    out.push(
      ...encodeCommand(
        username ? ['AUTH', username, password] : ['AUTH', password],
      ),
    )
  }
  const select = connection.lastDbid !== dbid
  if (select) {
    out.push(...encodeCommand(['SELECT', dbid.toString()]))
  }
  const restore = ctx.server.clusterEnabled ? 'RESTORE-ASKING' : 'RESTORE'
  const sent: MigrateKey[] = []
  for (const entry of keys) {
    let ttl = 0
    if (entry.expiresAt !== undefined) {
      ttl = entry.expiresAt - now
      if (ttl < 0) continue
      if (ttl < 1) ttl = 1
    }
    sent.push(entry)
    const parts: (Buffer | string)[] = [
      restore,
      entry.key,
      String(ttl),
      entry.payload,
    ]
    if (replace) parts.push('REPLACE')
    out.push(...encodeCommand(parts))
  }

  const socketError = (writeError: boolean, retry = true): Attempt => ({
    kind: 'socket-error',
    writeError,
    timedOut: connection.timedOut,
    retry,
  })

  if (!(await connection.write(Buffer.concat(out), timeout))) {
    return socketError(true)
  }
  const authReply = password ? await connection.readLine(timeout) : undefined
  if (authReply === null) return socketError(false)
  const selectReply = select ? await connection.readLine(timeout) : undefined
  if (selectReply === null) return socketError(false)

  let targetError: Buffer | null = null
  let readFailed = false
  let read = 0
  for (const entry of sent) {
    const reply = await connection.readLine(timeout)
    if (reply === null) {
      readFailed = true
      break
    }
    read++
    const failed =
      authReply?.[0] === 0x2d || selectReply?.[0] === 0x2d || reply[0] === 0x2d
    if (failed) {
      if (!targetError) {
        connection.lastDbid = -1n
        targetError =
          authReply?.[0] === 0x2d
            ? authReply
            : selectReply?.[0] === 0x2d
              ? selectReply
              : reply
      }
    } else if (!copy) {
      // The key moved: deleted as DEL does it (`del`, WATCH).
      ctx.db.delete(entry.key)
    }
  }

  if (!targetError && readFailed) {
    // Retried only when nothing was read: otherwise some keys already moved
    // (and were deleted here) and the rest stay.
    return socketError(false, read === 0)
  }
  if (targetError) {
    return {
      kind: 'done',
      close: readFailed,
      result: new RedisCommandError(
        Buffer.concat([
          Buffer.from('Target instance replied with error: '),
          targetError.subarray(1),
        ]),
      ),
    }
  }
  connection.lastDbid = dbid
  return { kind: 'done', result: ok(), close: false }
}

export const migrateCommands = [migrateCommand]
