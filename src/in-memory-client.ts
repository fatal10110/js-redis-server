import { ClientSession } from './core/client-session'
import type { CommandExecutor } from './core/command-executor'
import { RedisCommandError } from './core/redis-error'
import type { RedisValue } from './core/redis-value'
import { RedisResult } from './core/redis-result'
import { isResponseStream } from './core/response-stream'
import type { RedisServerState } from './state'

/** A command argument the in-memory client accepts. */
export type RedisCommandArgument = string | number | Buffer

/** Native JS value an in-memory reply decodes to. */
export type RedisNativeReply =
  | string
  | number
  | bigint
  | boolean
  | Buffer
  | null
  | RedisNativeReply[]
  | { [key: string]: RedisNativeReply }

export type InMemoryRedisClientOptions = {
  server: RedisServerState
  executor: CommandExecutor
  /** Initial selected database (default 0). */
  database?: number
  /** Return `Buffer`s for bulk-string/verbatim replies instead of utf8 strings. */
  returnBuffers?: boolean
}

/**
 * Socketless, high-level client that drives the **same** command pipeline as a
 * networked client through an in-process {@link ClientSession} — bypassing both
 * the TCP loopback and RESP encoding/decoding. Intended for tests that just need
 * to issue commands and read native JS replies without pulling in a real client
 * library.
 *
 * Streaming commands (SUBSCRIBE / PSUBSCRIBE / MONITOR) are not supported here;
 * use a real client for those.
 */
export class InMemoryRedisClient {
  private readonly session: ClientSession
  private readonly returnBuffers: boolean
  private closed = false

  constructor(options: InMemoryRedisClientOptions) {
    this.session = new ClientSession({
      server: options.server,
      executor: options.executor,
      database: options.database,
    })
    this.returnBuffers = options.returnBuffers ?? false
  }

  /**
   * Run a single command (e.g. `client.command('SET', 'k', 'v')`) and resolve
   * to its native reply. Throws a {@link RedisCommandError} for `-ERR` replies,
   * mirroring what a real client surfaces.
   */
  async command(
    name: string,
    ...args: RedisCommandArgument[]
  ): Promise<RedisNativeReply> {
    if (this.closed) {
      throw new Error('InMemoryRedisClient is closed')
    }

    const result = await this.session.execute(
      Buffer.from(name),
      args.map(toBuffer),
    )

    if (isResponseStream(result)) {
      result.close(
        'streaming commands are not supported by InMemoryRedisClient',
      )
      throw new RedisCommandError(
        `${name.toUpperCase()} is a streaming command; use a real client`,
      )
    }

    return this.decode((result as RedisResult).value)
  }

  /** Alias for {@link InMemoryRedisClient.command}. */
  send(
    name: string,
    ...args: RedisCommandArgument[]
  ): Promise<RedisNativeReply> {
    return this.command(name, ...args)
  }

  close(): void {
    if (this.closed) {
      return
    }
    this.closed = true
    this.session.close()
  }

  private decode(value: RedisValue): RedisNativeReply {
    switch (value.kind) {
      case 'simple-string':
        return value.value
      case 'bulk-string':
        if (value.value === null) {
          return null
        }
        return this.returnBuffers ? value.value : value.value.toString('utf8')
      case 'verbatim':
        return this.returnBuffers ? value.value : value.value.toString('utf8')
      case 'integer':
        // Integer replies are plain numbers in real clients; only widen to
        // bigint when the value genuinely overflows a JS safe integer.
        if (typeof value.value === 'bigint') {
          return isSafeBigInt(value.value) ? Number(value.value) : value.value
        }
        return value.value
      case 'double':
        return value.value
      case 'boolean':
        return value.value
      case 'big-number':
        return value.value
      case 'array':
      case 'set':
      case 'push':
        return value.items.map(item => this.decode(item))
      case 'map':
      case 'map-pairs': {
        const out: { [key: string]: RedisNativeReply } = {}
        for (const [key, val] of value.entries) {
          out[decodeKey(key)] = this.decode(val)
        }
        return out
      }
      case 'flat-pairs':
        // Flat on the wire in RESP2; keep the flat array shape here too.
        return value.entries.flatMap(([key, val]) => [
          this.decode(key),
          this.decode(val),
        ])
      case 'null':
      case 'null-array':
        return null
      case 'error':
        throw new RedisCommandError(value.message)
    }
  }
}

export function createInMemoryClient(
  options: InMemoryRedisClientOptions,
): InMemoryRedisClient {
  return new InMemoryRedisClient(options)
}

function isSafeBigInt(value: bigint): boolean {
  return (
    value >= BigInt(Number.MIN_SAFE_INTEGER) &&
    value <= BigInt(Number.MAX_SAFE_INTEGER)
  )
}

function toBuffer(arg: RedisCommandArgument): Buffer {
  if (Buffer.isBuffer(arg)) {
    return arg
  }
  return Buffer.from(typeof arg === 'number' ? String(arg) : arg)
}

/** Map keys are always plain strings, regardless of `returnBuffers`. */
function decodeKey(value: RedisValue): string {
  switch (value.kind) {
    case 'simple-string':
      return value.value
    case 'bulk-string':
      return value.value === null ? '' : value.value.toString('utf8')
    case 'verbatim':
      return value.value.toString('utf8')
    case 'integer':
    case 'double':
    case 'big-number':
      return String(value.value)
    case 'boolean':
      return String(value.value)
    default:
      return ''
  }
}
