import { ClientSession } from '../../client-session'
import { RedisCommandError } from '../../redis-error'
import { RedisResult } from '../../redis-result'
import { RedisValue } from '../../redis-value'
import { encodeRedisResult } from '../../resp-encoder'
import { isResponseStream } from '../../response-stream'
import type { ResponseStream } from '../../response-stream'
import type { ExecutorResult } from '../../command-executor'
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
  private readonly activeStreams = new Set<Promise<void>>()

  constructor(options: Resp2SessionAdapterOptions) {
    this.transport = options.transport
    this.session = options.session
    this.logger = options.logger
    this.decoder = new Resp2CommandDecoder({
      // Read per bulk header, not captured once: `CONFIG SET
      // proto-max-bulk-len` moves the ceiling for every connection immediately.
      maxBulkLength: () => this.session.server.protoMaxBulkLen,
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
      this.session.close()
      await pushWriter
      // Streams are torn down by session.close() (resetResponseStreams aborts
      // them); wait for their drain tasks to settle so nothing writes after we
      // return.
      await Promise.allSettled(this.activeStreams)
    }
  }

  private async handleFrame(frame: Resp2CommandFrame): Promise<void> {
    const result = await this.session.execute(frame.command, frame.args)
    await this.writeExecutorResult(result)
  }

  private async writeExecutorResult(result: ExecutorResult): Promise<void> {
    if (isResponseStream(result)) {
      // A ResponseStream can be long-lived (MONITOR, future SUBSCRIBE). Draining
      // it inline would pin the frame loop until the stream closed, so the
      // connection could never send another command. Drain it as a background
      // task instead, multiplexed onto the shared writeChain, while run() keeps
      // reading and dispatching subsequent frames.
      this.spawnStreamDrain(result)
      return
    }

    await this.writeRedisResult(result)
  }

  private spawnStreamDrain(stream: ResponseStream): void {
    const drain = this.drainStream(stream)
    this.activeStreams.add(drain)
    void drain.finally(() => {
      this.activeStreams.delete(drain)
    })
  }

  private async drainStream(stream: ResponseStream): Promise<void> {
    try {
      for await (const frame of stream.frames(this.transport.signal)) {
        await this.writeRedisResult(frame)
        if (this.transport.signal.aborted) {
          stream.close('transport closed')
          return
        }
      }
    } catch (err) {
      stream.close('resp2 stream error')
      if (!this.transport.signal.aborted) {
        this.logger?.error(err)
        this.transport.close('resp2 stream error')
      }
    }
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
      await this.writeRedisResult(RedisResult.error(err.message, err.code))
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
