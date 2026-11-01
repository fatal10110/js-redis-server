import { RawRedisConnection } from './raw-connection'
import { commandFrame } from '../utils'

/** Byte-level helpers for the CLIENT TRACKING suites. */
export const bulk = (value: string) =>
  `$${Buffer.byteLength(value)}\r\n${value}\r\n`

/** A RESP3 `invalidate` push for `keys`. */
export const invalidate = (...keys: string[]) =>
  `>2\r\n${bulk('invalidate')}*${keys.length}\r\n${keys.map(bulk).join('')}`

/** A RESP3 flush invalidation from a RESP3 tracker. */
export const invalidateAll = `>2\r\n${bulk('invalidate')}_\r\n`

/** A RESP2 redirect target's `__redis__:invalidate` message for `keys`. */
export const invalidateMessage = (...keys: string[]) =>
  `*3\r\n${bulk('message')}${bulk('__redis__:invalidate')}*${keys.length}\r\n${keys.map(bulk).join('')}`

/** The flush invalidation a RESP2 redirect target gets from a RESP2 tracker. */
export const invalidateAllMessage = `*3\r\n${bulk('message')}${bulk('__redis__:invalidate')}$-1\r\n`

const PONG = '+PONG\r\n'
const SUBSCRIBED_PONG = `*2\r\n${bulk('pong')}${bulk('')}`

/**
 * A raw connection that reads whole frames as latin1 strings. A FLUSHDB /
 * FLUSHALL anywhere on a shared real server sends every tracking client a
 * null invalidation, so those are dropped unless a test asks for them.
 */
export class TrackingClient {
  private constructor(
    private readonly conn: RawRedisConnection,
    private readonly subscribedResp2 = false,
  ) {}

  static async open(
    port: number,
    resp3: boolean,
    db: string,
  ): Promise<TrackingClient> {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    const client = new TrackingClient(conn)
    if (resp3) {
      conn.write(commandFrame('HELLO', '3'))
      await conn.readRawFrame()
    }
    await client.call('SELECT', db)
    return client
  }

  /** The same connection, PINGed as a RESP2 subscriber. */
  asSubscriber(): TrackingClient {
    return new TrackingClient(this.conn, true)
  }

  async id(): Promise<string> {
    const reply = await this.call('CLIENT', 'ID')
    return reply.slice(1, -2)
  }

  /** Send a command and read its reply. */
  async call(...args: string[]): Promise<string> {
    const [reply] = await this.callFrames(1, ...args)
    return reply
  }

  /** Send a command and read `count` frames: the reply, then pushes. */
  async callFrames(count: number, ...args: string[]): Promise<string[]> {
    this.conn.write(commandFrame(...args))
    const frames: string[] = []
    while (frames.length < count) {
      const frame = await this.next()
      if (!isFlushNull(frame)) {
        frames.push(frame)
      }
    }
    return frames
  }

  /**
   * PING and return every frame queued ahead of its reply: the pushes
   * already sent to this connection. Flush nulls are dropped unless
   * `keepFlushes`.
   */
  async pending(keepFlushes = false): Promise<string[]> {
    const pong = this.subscribedResp2 ? SUBSCRIBED_PONG : PONG
    this.conn.write(commandFrame('PING'))
    const frames: string[] = []
    for (;;) {
      const frame = await this.next()
      if (frame === pong) {
        return frames
      }
      if (keepFlushes || !isFlushNull(frame)) {
        frames.push(frame)
      }
    }
  }

  /** The next frame, verbatim. */
  async next(): Promise<string> {
    return (await this.conn.readRawFrame()).toString('latin1')
  }

  write(...args: string[]): void {
    this.conn.write(commandFrame(...args))
  }

  close(): void {
    this.conn.close()
  }
}

function isFlushNull(frame: string): boolean {
  return (
    frame === invalidateAll ||
    frame === `>2\r\n${bulk('invalidate')}$-1\r\n` ||
    frame === invalidateAllMessage ||
    frame === `*3\r\n${bulk('message')}${bulk('__redis__:invalidate')}_\r\n`
  )
}
