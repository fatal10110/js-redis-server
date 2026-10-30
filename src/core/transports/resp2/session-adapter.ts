import { ClientSession } from '../../client-session'
import { RedisCommandError } from '../../redis-error'
import { RedisResult } from '../../redis-result'
import { RedisValue } from '../../redis-value'
import { encodeRedisResult } from '../../resp-encoder'
import type { Logger } from '../../../logger'
import type { ConnectionTransport } from '../connection-transport'
import {
  Resp2CommandDecoder,
  Resp2ParseError,
  type Resp2CommandFrame,
} from './decoder'

export type Resp2SessionAdapterOptions = {
  transport: ConnectionTransport
  session: ClientSession
  logger?: Pick<Logger, 'error'>
}

export class Resp2SessionAdapter {
  private readonly decoder: Resp2CommandDecoder
  private readonly transport: ConnectionTransport
  private readonly session: ClientSession
  private readonly logger?: Pick<Logger, 'error'>
  private writeChain: Promise<void> = Promise.resolve()
  private input?: AsyncIterator<Buffer>
  /** The read in flight, if any; {@link pull} shares it between callers. */
  private pulling?: Promise<void>
  /** The client's input is over: EOF, or the read loop failed. */
  private inputEnded = false
  private inputError: unknown
  private readingWhileParked = false

  constructor(options: Resp2SessionAdapterOptions) {
    this.transport = options.transport
    this.session = options.session
    this.logger = options.logger
    this.decoder = new Resp2CommandDecoder({
      // Read per bulk header, not captured once: `CONFIG SET
      // proto-max-bulk-len` moves the ceiling for every connection immediately.
      maxBulkLength: () => this.session.server.protoMaxBulkLen,
      profile: this.session.server.profile,
      // Live as well: AUTH lifts the unauthenticated request caps and RESET
      // puts them back, from the very next request header.
      authRequired: () =>
        Boolean(this.session.server.requirepass) &&
        !this.session.isAuthenticated,
    })
  }

  async run(): Promise<void> {
    const pushWriter = this.writeSessionPushes()
    this.input = this.transport.read()[Symbol.asyncIterator]()
    // Redis keeps reading a blocked client's socket (it only stops parsing
    // it), so the EOF of a client that hangs up while blocked frees it at
    // once. Between commands the loop below reads; while one is parked, this
    // does, and buffers what arrives for the loop to parse afterwards.
    const unsubscribePark = this.session.onPark(() => {
      void this.readWhileParked()
    })

    try {
      for (;;) {
        let frame: Resp2CommandFrame | null
        try {
          // Pulled one at a time so each command has run — and any config it
          // changed has landed — before the next frame is parsed.
          frame = this.decoder.next()
        } catch (err) {
          if (!(err instanceof Resp2ParseError)) {
            throw err
          }
          // Valid frames before the bad one were answered by earlier passes
          // of this loop; now report the protocol error and close, matching
          // real Redis.
          await this.writeError(err)
          this.transport.close('resp2 protocol error')
          return
        }

        if (frame) {
          await this.handleFrame(frame)
          if (this.transport.signal.aborted) {
            return
          }
          continue
        }

        if (this.inputEnded) {
          if (this.inputError !== undefined) {
            throw this.inputError
          }
          return
        }

        await this.pull()
      }
    } catch (err) {
      // A command aborted because the client hung up while it was parked
      // has no one to answer.
      if (!this.transport.signal.aborted && !this.inputEnded) {
        await this.writeError(err)
        this.transport.close('resp2 adapter error')
      }
    } finally {
      unsubscribePark()
      // Not awaited: a read still in flight settles once the stream ends.
      // `destroyOnReturn: false` keeps this from destroying the stream.
      void this.input.return?.().catch(() => {})
      try {
        this.session.close()
        await pushWriter
      } finally {
        // The read loop is over, so close our side as Redis does: a half-close
        // that lets output already queued go out first. This is the real
        // close whichever end finished first — on a client EOF the transport
        // defers its own teardown by an immediate, so it has not happened yet
        // — and it is what still delivers e.g. every confirmation of a
        // `SUBSCRIBE a b c` sent together with the EOF. Last, so the push
        // writer gets to finish; in a finally, so a throw from session.close()
        // cannot skip it.
        this.transport.close('session ended')
      }
    }
  }

  /**
   * Read the next chunk into the decoder, or note the end of input (which
   * aborts a parked command, see {@link ClientSession.endInput}). Concurrent
   * callers share one read, so every chunk is pushed exactly once.
   */
  private pull(): Promise<void> {
    this.pulling ??= (async () => {
      try {
        const next = await this.input!.next()
        if (!next.done) {
          this.decoder.push(next.value)
          return
        }
        this.inputEnded = true
      } catch (err) {
        this.inputEnded = true
        this.inputError = err
      } finally {
        this.pulling = undefined
      }
      this.session.endInput()
    })()
    return this.pulling
  }

  private async readWhileParked(): Promise<void> {
    if (this.readingWhileParked) {
      return
    }
    this.readingWhileParked = true
    try {
      while (this.session.parked && !this.inputEnded) {
        await this.pull()
      }
    } finally {
      this.readingWhileParked = false
    }
  }

  private async handleFrame(frame: Resp2CommandFrame): Promise<void> {
    const result = await this.session.execute(frame.command, frame.args)
    await this.writeRedisResult(result)
  }

  private async writeRedisResult(result: RedisResult): Promise<void> {
    await this.chainWrites(result)
  }

  /**
   * Appends to the write chain every push the session has queued, then
   * `result` if given, all synchronously. Redis has one output buffer per
   * client, so a push queued before a reply (another client's keyspace
   * notification while this one's BLPOP was parked) goes out before it; taking
   * pushes one per awaited write would let a reply slip between two of them.
   */
  private chainWrites(result?: RedisResult): Promise<void> {
    for (const push of this.session.takePushes()) {
      this.chainWrite(push)
    }
    if (result) {
      this.chainWrite(result)
    }
    return this.writeChain
  }

  private chainWrite(result: RedisResult): void {
    const write = async () => {
      if (!result.options?.omitReply) {
        await this.transport.write(
          encodeRedisResult(result, {
            version: this.session.protocolVersion,
            profile: this.session.server.profile,
          }),
        )
      }

      result.options?.afterReply?.()

      if (result.options?.close || result.options?.disconnect) {
        this.transport.close('command requested close')
      }
    }

    this.writeChain = this.writeChain.catch(() => {}).then(write)
  }

  private async writeSessionPushes(): Promise<void> {
    try {
      while (await this.session.waitForPushes(this.transport.signal)) {
        await this.chainWrites()
      }
    } catch (err) {
      if (!this.transport.signal.aborted) {
        this.logger?.error(err)
        this.transport.close('resp2 push writer error')
      }
    }
  }

  private async writeError(err: unknown): Promise<void> {
    if (this.transport.signal.aborted) {
      return
    }

    if (err instanceof RedisCommandError) {
      await this.writeRedisResult(RedisResult.fromError(err))
      return
    }

    if (err instanceof Resp2ParseError) {
      // Pre-encoded from `messageBytes`, not `message`: some protocol errors
      // echo a raw client byte that a string would re-encode as UTF-8.
      await this.writeRedisResult(
        RedisResult.preEncoded(
          RedisValue.error(err.message, 'ERR'),
          Buffer.concat([
            Buffer.from('-ERR '),
            err.messageBytes,
            Buffer.from('\r\n'),
          ]),
        ),
      )
      return
    }

    this.logger?.error(err)
    await this.writeRedisResult(
      RedisResult.error('internal server error', 'ERR'),
    )
  }
}
