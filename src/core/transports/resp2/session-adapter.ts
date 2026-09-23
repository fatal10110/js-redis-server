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

  constructor(options: Resp2SessionAdapterOptions) {
    this.transport = options.transport
    this.session = options.session
    this.logger = options.logger
    this.decoder = new Resp2CommandDecoder({
      // Read per bulk header, not captured once: `CONFIG SET
      // proto-max-bulk-len` moves the ceiling for every connection immediately.
      maxBulkLength: () => this.session.server.protoMaxBulkLen,
      profile: this.session.server.profile,
    })
  }

  async run(): Promise<void> {
    const pushWriter = this.writeSessionPushes()

    try {
      for await (const chunk of this.transport.read()) {
        this.decoder.push(chunk)

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

          if (!frame) {
            break
          }

          await this.handleFrame(frame)
          if (this.transport.signal.aborted) {
            return
          }
        }
      }
    } catch (err) {
      if (!this.transport.signal.aborted) {
        await this.writeError(err)
        this.transport.close('resp2 adapter error')
      }
    } finally {
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

  private async handleFrame(frame: Resp2CommandFrame): Promise<void> {
    const result = await this.session.execute(frame.command, frame.args)
    await this.writeRedisResult(result)
  }

  private async writeRedisResult(result: RedisResult): Promise<void> {
    const write = async () => {
      if (!result.options?.omitReply) {
        await this.transport.write(
          encodeRedisResult(result, {
            version: this.session.protocolVersion,
          }),
        )
      }

      result.options?.afterReply?.()

      if (result.options?.close || result.options?.disconnect) {
        this.transport.close('command requested close')
      }
    }

    this.writeChain = this.writeChain.catch(() => {}).then(write)
    await this.writeChain
  }

  private async writeSessionPushes(): Promise<void> {
    try {
      for await (const frame of this.session.readPushes(
        this.transport.signal,
      )) {
        await this.writeRedisResult(frame)
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
