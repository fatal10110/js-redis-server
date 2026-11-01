import { connect, type Socket } from 'node:net'
import type { RedisServerState } from '../state/server-state'

/**
 * MIGRATE's outbound connections (cluster.c `migrateGetSocket` and friends):
 * one TCP socket per `host:port` a server has migrated to, kept for reuse
 * with the database it last selected there, and closed after
 * {@link MIGRATE_SOCKET_IDLE_MS} unused. Each operation takes the command's
 * timeout like the blocking `connBlockingConnect` / `connSyncWrite` /
 * `connSyncReadLine` real servers use, but awaits instead of blocking: the
 * source server still holds its turn for the whole call, so its other
 * clients wait exactly as long as on a real server.
 */

/** `MIGRATE_SOCKET_CACHE_ITEMS`. */
const MIGRATE_SOCKET_CACHE_ITEMS = 64
/** `MIGRATE_SOCKET_CACHE_TTL`: 10 s without use closes the socket. */
const MIGRATE_SOCKET_IDLE_MS = 10_000
/** `connSyncReadLine`'s buffer: a longer line is cut, as on a real server. */
const READ_LINE_MAX = 1023

export type ConnectOutcome =
  /** Connected, or the connect failed after the address was resolved. */
  | 'connected'
  /** The address could not be resolved, or the port is not a port. */
  | 'failed'
  /** Nothing answered the connect within the timeout. */
  | 'timeout'

type IoOutcome = 'ok' | 'error' | 'timeout'

export class MigrateConnection {
  /** `last_dbid`: the database the last successful MIGRATE selected. */
  lastDbid = -1n
  private socket: Socket | null = null
  private connected = false
  private failed = false
  private input: Buffer = Buffer.alloc(0)
  private readonly waiters = new Set<() => void>()
  /** Set when the last operation failed on its timeout (`ETIMEDOUT`). */
  timedOut = false

  /**
   * Start the connect and wait for it at most `timeoutMs`. A connect the
   * kernel refuses (or resets) still counts as `connected` here: a real
   * server's non-blocking connect only waits for the socket to become
   * writable, so the failure surfaces at the first write.
   */
  async connect(
    host: string,
    port: number,
    timeoutMs: number,
  ): Promise<ConnectOutcome> {
    let lookupFailed = false
    let socket: Socket
    try {
      socket = connect({ host, port })
    } catch {
      return 'failed'
    }
    this.socket = socket
    socket.unref()
    socket.setNoDelay(true)
    socket.on('lookup', err => {
      if (err) lookupFailed = true
    })
    socket.on('connect', () => {
      this.connected = true
      this.wake()
    })
    socket.on('data', (chunk: Buffer) => {
      this.input =
        this.input.length === 0 ? chunk : Buffer.concat([this.input, chunk])
      this.wake()
    })
    socket.on('error', () => {
      this.failed = true
      this.wake()
    })
    socket.on('close', () => {
      this.failed = true
      this.wake()
    })

    const outcome = await this.waitUntil(
      () => this.connected || this.failed,
      timeoutMs,
    )
    if (outcome === 'timeout') {
      return 'timeout'
    }
    return lookupFailed ? 'failed' : 'connected'
  }

  /**
   * A connect to port 0 (what `atoi` makes of a port that is not a number):
   * refused, which shows only at the first write.
   */
  refuse(): ConnectOutcome {
    this.failed = true
    return 'connected'
  }

  /** `connSyncWrite`: hand all of `data` to the kernel within `timeoutMs`. */
  async write(data: Buffer, timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs
    const socket = this.socket
    if (!socket) return false
    const ready = await this.waitUntil(
      () => this.connected || this.failed,
      timeoutMs,
    )
    if (ready !== 'ok' || this.failed) return false

    let written = false
    let writeFailed = false
    socket.write(data, err => {
      if (err) writeFailed = true
      else written = true
      this.wake()
    })
    const outcome = await this.waitUntil(
      () => written || writeFailed,
      Math.max(0, deadline - Date.now()),
    )
    return outcome === 'ok' && written
  }

  /**
   * `connSyncReadLine`: the next reply line without its `\r\n`, or null when
   * none arrives within `timeoutMs` or the connection fails. An empty line
   * counts as a failure, as it does for the real caller (`<= 0`).
   */
  async readLine(timeoutMs: number): Promise<Buffer | null> {
    const outcome = await this.waitUntil(
      () => this.input.includes(0x0a),
      timeoutMs,
    )
    if (outcome !== 'ok') return null
    const end = this.input.indexOf(0x0a)
    let line = this.input.subarray(0, end)
    this.input = this.input.subarray(end + 1)
    if (line.length > 0 && line[line.length - 1] === 0x0d) {
      line = line.subarray(0, line.length - 1)
    }
    if (line.length === 0) return null
    return Buffer.from(line.subarray(0, READ_LINE_MAX))
  }

  close(): void {
    this.failed = true
    this.socket?.destroy()
    this.socket = null
    this.wake()
  }

  /** Whether the peer closed the connection or it failed. */
  get broken(): boolean {
    return this.failed
  }

  /**
   * Wait until `ready()` holds (`ok`), the connection fails (`error`), or
   * `timeoutMs` passes (`timeout`, recorded in {@link timedOut}). `ready` is
   * checked before the failure, so data that arrived before the peer closed
   * is still read.
   */
  private waitUntil(
    ready: () => boolean,
    timeoutMs: number,
  ): Promise<IoOutcome> {
    this.timedOut = false
    return new Promise(resolve => {
      const check = (): boolean => {
        if (ready()) {
          finish('ok')
        } else if (this.failed) {
          finish('error')
        } else {
          return false
        }
        return true
      }
      const finish = (outcome: IoOutcome) => {
        clearTimeout(timer)
        this.waiters.delete(onWake)
        if (outcome === 'timeout') this.timedOut = true
        resolve(outcome)
      }
      const onWake = () => {
        check()
      }
      const timer = setTimeout(() => finish('timeout'), timeoutMs)
      if (!check()) {
        this.waiters.add(onWake)
      }
    })
  }

  private wake(): void {
    for (const waiter of [...this.waiters]) {
      waiter()
    }
  }
}

type CachedSocket = {
  connection: MigrateConnection
  /** When a MIGRATE last took it (`last_use_time`). */
  lastUse: number
  /** Armed only while no MIGRATE is using the socket. */
  idleTimer: ReturnType<typeof setTimeout> | undefined
}

/**
 * `server.migrate_cached_sockets` of one server: at most
 * {@link MIGRATE_SOCKET_CACHE_ITEMS} entries keyed `host:port` exactly as
 * given (so `localhost:6379` and `127.0.0.1:6379` are two sockets).
 *
 * A socket taken by {@link get} or {@link add} is in use until
 * {@link release}: nothing closes it meanwhile, as real servers'
 * `migrateCloseTimedoutSockets` only runs from serverCron, which cannot run
 * while MIGRATE blocks on the target, so a reply slower than the idle period
 * still arrives on the same socket. The idle period counts from when the
 * socket was taken (`last_use_time`), so one used longer than that closes as
 * soon as it is released, as the next cron run would close it.
 */
export class MigrateSocketCache {
  /** The idle period; tests shorten it. */
  static idleMs = MIGRATE_SOCKET_IDLE_MS

  private readonly sockets = new Map<string, CachedSocket>()

  /**
   * The cached socket for `name`, now in use, or undefined. A socket whose
   * peer has since closed is dropped instead: the real one would fail its
   * next read and be replaced by the retry, which comes to the same
   * reconnect.
   */
  get(name: string): MigrateConnection | undefined {
    const cached = this.sockets.get(name)
    if (!cached) return undefined
    if (cached.connection.broken) {
      this.close(name)
      return undefined
    }
    clearTimeout(cached.idleTimer)
    cached.idleTimer = undefined
    cached.lastUse = Date.now()
    return cached.connection
  }

  /** Caches a new socket for `name`, in use until {@link release}. */
  add(name: string, connection: MigrateConnection): void {
    if (this.sockets.size >= MIGRATE_SOCKET_CACHE_ITEMS) {
      // Real servers drop a random entry; the oldest is as good.
      const oldest = this.sockets.keys().next().value
      if (oldest !== undefined) this.close(oldest)
    }
    this.sockets.set(name, {
      connection,
      lastUse: Date.now(),
      idleTimer: undefined,
    })
  }

  /** Ends the use of `name`'s socket: its idle clock starts again. */
  release(name: string): void {
    const cached = this.sockets.get(name)
    if (!cached) return
    clearTimeout(cached.idleTimer)
    const left = MigrateSocketCache.idleMs - (Date.now() - cached.lastUse)
    const idleTimer = setTimeout(() => this.close(name), Math.max(0, left))
    idleTimer.unref()
    cached.idleTimer = idleTimer
  }

  /** `migrateCloseSocket`. */
  close(name: string): void {
    const cached = this.sockets.get(name)
    if (!cached) return
    clearTimeout(cached.idleTimer)
    cached.connection.close()
    this.sockets.delete(name)
  }

  closeAll(): void {
    for (const name of [...this.sockets.keys()]) {
      this.close(name)
    }
  }
}

const caches = new WeakMap<RedisServerState, MigrateSocketCache>()

/** The socket cache of `server`, closed with it. */
export function migrateSocketCache(
  server: RedisServerState,
): MigrateSocketCache {
  let cache = caches.get(server)
  if (!cache) {
    const created = new MigrateSocketCache()
    cache = created
    caches.set(server, created)
    server.onClose(() => {
      created.closeAll()
      caches.delete(server)
    })
  }
  return cache
}
