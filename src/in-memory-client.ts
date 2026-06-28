import { createRedisCommandExecutor } from './commands'
import { ClientSession } from './core/client-session'
import type { CommandExecutor } from './core/command-executor'
import { RedisCommandError } from './core/redis-error'
import type { RedisValue } from './core/redis-value'
import { RedisResult } from './core/redis-result'
import { isResponseStream, type ResponseStream } from './core/response-stream'
import { seedStandalone, type SeedEntry } from './seed'
import { RedisServerState } from './state'

const DEFAULT_DATABASE_COUNT = 16

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
  /** Called once when the client is closed — used to tear down owned state. */
  onClose?: () => void
}

/** Aborts when any of the given signals abort (or immediately if one already has). */
function anySignal(signals: readonly AbortSignal[]): AbortSignal {
  const controller = new AbortController()
  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort()
      break
    }
    signal.addEventListener('abort', () => controller.abort(), { once: true })
  }
  return controller.signal
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
  private readonly onClose?: () => void
  private closed = false
  /** Aborted on close — tears down any active stream and push readers. */
  private readonly lifetime = new AbortController()
  /**
   * Set once a streaming command (e.g. MONITOR) hands back a `ResponseStream`.
   * `command()` consumes its first frame as the immediate reply; {@link pushes}
   * drains the rest. Pub/sub doesn't set this — its messages flow through the
   * session push channel instead (see {@link pushes}).
   */
  private activeStream?: ResponseStream
  private streamFrames?: AsyncIterator<RedisResult>

  constructor(options: InMemoryRedisClientOptions) {
    this.session = new ClientSession({
      server: options.server,
      executor: options.executor,
      database: options.database,
    })
    this.returnBuffers = options.returnBuffers ?? false
    this.onClose = options.onClose
  }

  /**
   * Run a single command (e.g. `client.command('SET', 'k', 'v')`) and resolve
   * to its native reply. Throws a {@link RedisCommandError} for `-ERR` replies,
   * mirroring what a real client surfaces.
   *
   * Streaming commands (MONITOR / SUBSCRIBE / …) resolve to their *immediate*
   * reply (MONITOR's `OK`, the subscribe confirmation); their server-initiated
   * frames are delivered through {@link pushes}.
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
      // A long-lived (MONITOR) or finite (multi-channel subscribe) stream: keep
      // the iterator so pushes() continues it, and return the first frame as the
      // immediate reply.
      this.activeStream = result
      this.streamFrames = result
        .frames(this.lifetime.signal)
        [Symbol.asyncIterator]()
      const first = await this.streamFrames.next()
      return first.done ? null : this.decode(first.value.value)
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

  /**
   * True once the last command put the connection into *push mode* — an active
   * MONITOR stream, or a SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE that entered subscribed
   * mode. Consumers should switch to draining {@link pushes} while this holds.
   */
  get streaming(): boolean {
    return this.activeStream !== undefined || this.session.mode === 'subscribed'
  }

  /**
   * Server-initiated frames for a connection in *push mode* — pub/sub messages
   * (via the session push channel) and the tail of a MONITOR stream — decoded to
   * native replies. Iterate it after issuing SUBSCRIBE/PSUBSCRIBE/MONITOR. Ends
   * when `signal` (or the connection) is closed.
   */
  async *pushes(signal?: AbortSignal): AsyncIterable<RedisNativeReply> {
    const sig = signal
      ? anySignal([this.lifetime.signal, signal])
      : this.lifetime.signal

    const sources: AsyncIterator<RedisResult>[] = []
    if (this.streamFrames) {
      sources.push(this.streamFrames)
    }
    sources.push(this.session.readPushes(sig)[Symbol.asyncIterator]())

    const queue: RedisResult[] = []
    let finished = 0
    let wake: (() => void) | null = null
    const ping = () => {
      wake?.()
      wake = null
    }

    for (const source of sources) {
      void (async () => {
        try {
          for (;;) {
            const { value, done } = await source.next()
            if (done) {
              break
            }
            queue.push(value)
            ping()
          }
        } finally {
          finished++
          ping()
        }
      })()
    }

    while (!sig.aborted) {
      const frame = queue.shift()
      if (frame) {
        yield this.decode(frame.value)
        continue
      }
      if (finished === sources.length) {
        return
      }
      await new Promise<void>(resolve => {
        wake = resolve
        sig.addEventListener('abort', () => resolve(), { once: true })
      })
    }
  }

  close(): void {
    if (this.closed) {
      return
    }
    this.closed = true
    this.lifetime.abort()
    this.activeStream?.close('client closed')
    this.session.close()
    this.onClose?.()
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
        return value.items.map(item => this.decode(item))
      case 'push':
        // RESP2 encodes a push as `[name, ...items]` on the wire (e.g. a pub/sub
        // message is `["message", channel, payload]`); keep the type tag so
        // push-mode consumers see the same shape a real client would.
        return [value.name, ...value.items.map(item => this.decode(item))]
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
        // Surface the full error text a real client sees — `<CODE> <message>`
        // (e.g. `MOVED 1234 host:port`, `WRONGTYPE …`), not just the detail.
        throw new RedisCommandError(
          value.code ? `${value.code} ${value.message}` : value.message,
          value.code,
        )
    }
  }
}

export type CreateInMemoryRedisOptions = {
  /** Logical database count (defaults to 16, matching real Redis). */
  databaseCount?: number
  /** Pre-populate the keyspace before any connection is opened. */
  seed?: readonly SeedEntry[]
}

/** Per-connection options for {@link InMemoryRedis.connect}. */
export type ConnectOptions = {
  /** Initial selected database (default 0). */
  database?: number
  /** Return `Buffer`s for bulk-string/verbatim replies instead of utf8 strings. */
  returnBuffers?: boolean
}

/**
 * A socketless in-memory Redis *instance* — one shared keyspace + command
 * pipeline that many {@link InMemoryRedisClient} connections can drive at once.
 * Because connections share the underlying {@link RedisServerState}, a `MONITOR`
 * / `SUBSCRIBE` on one connection observes commands run on another, and a
 * `BLPOP` blocks until another connection writes the key — exactly like real
 * Redis. `close()` tears the whole instance down.
 */
export class InMemoryRedis {
  constructor(
    private readonly state: RedisServerState,
    private readonly executor: CommandExecutor,
  ) {}

  /** Open a new connection (its own {@link ClientSession}) over this keyspace. */
  connect(options: ConnectOptions = {}): InMemoryRedisClient {
    return new InMemoryRedisClient({
      server: this.state,
      executor: this.executor,
      database: options.database,
      returnBuffers: options.returnBuffers,
    })
  }

  close(): void {
    this.state.close()
  }
}

/**
 * Build a socketless {@link InMemoryRedis} instance with its own keyspace +
 * command pipeline. Open one or more connections with {@link InMemoryRedis.connect}.
 */
export async function createInMemoryRedis(
  options: CreateInMemoryRedisOptions = {},
): Promise<InMemoryRedis> {
  const state = new RedisServerState({
    databaseCount: options.databaseCount ?? DEFAULT_DATABASE_COUNT,
  })
  const executor = createRedisCommandExecutor()

  if (options.seed) {
    await seedStandalone(state, options.seed)
  }

  return new InMemoryRedis(state, executor)
}

export type CreateInMemoryClientOptions = CreateInMemoryRedisOptions &
  ConnectOptions

/**
 * Convenience wrapper: an {@link InMemoryRedis} instance with a single owned
 * connection. `client.close()` tears the whole instance down. For multiple
 * connections over one keyspace (pub/sub, MONITOR, cross-connection blocking),
 * use {@link createInMemoryRedis} and call `connect()` per connection.
 */
export async function createInMemoryClient(
  options: CreateInMemoryClientOptions = {},
): Promise<InMemoryRedisClient> {
  const instance = await createInMemoryRedis(options)
  const client = instance.connect({
    database: options.database,
    returnBuffers: options.returnBuffers,
  })
  return wrapWithInstanceClose(client, instance)
}

/** Wires a single client's `close()` to also tear down the instance it owns. */
function wrapWithInstanceClose(
  client: InMemoryRedisClient,
  instance: InMemoryRedis,
): InMemoryRedisClient {
  const close = client.close.bind(client)
  client.close = () => {
    close()
    instance.close()
  }
  return client
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
