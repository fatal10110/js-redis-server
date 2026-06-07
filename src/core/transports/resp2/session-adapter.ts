import { ClientSession } from '../../client-session'
import { RedisCommandError } from '../../redis-error'
import { RedisResult } from '../../redis-result'
import { encodeRedisResult } from '../../resp-encoder'
import { isResponseStream } from '../../response-stream'
import type { ExecutorResult } from '../../command-executor'
import type { RespEncodeOptions } from '../../resp-encoder'
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
  encoder?: RespEncodeOptions
}

export class Resp2SessionAdapter {
  private readonly decoder = new Resp2CommandDecoder()
  private readonly transport: ConnectionTransport
  private readonly session: ClientSession
  private readonly logger?: Pick<Logger, 'error'>
  private readonly encoder?: RespEncodeOptions

  constructor(options: Resp2SessionAdapterOptions) {
    this.transport = options.transport
    this.session = options.session
    this.logger = options.logger
    this.encoder = options.encoder
  }

  async run(): Promise<void> {
    try {
      for await (const chunk of this.transport.read()) {
        const frames = this.decoder.push(chunk)
        for (const frame of frames) {
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
    }
  }

  private async handleFrame(frame: Resp2CommandFrame): Promise<void> {
    const result = await this.session.execute(frame.command, frame.args)
    await this.writeExecutorResult(result)
  }

  private async writeExecutorResult(result: ExecutorResult): Promise<void> {
    if (isResponseStream(result)) {
      for await (const frame of result.frames(this.transport.signal)) {
        await this.writeRedisResult(frame)
        if (this.transport.signal.aborted) {
          result.close('transport closed')
          return
        }
      }
      return
    }

    await this.writeRedisResult(result)
  }

  private async writeRedisResult(result: RedisResult): Promise<void> {
    await this.transport.write(encodeRedisResult(result, this.encoder))

    if (result.options?.close || result.options?.disconnect) {
      this.transport.close('command requested close')
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
      await this.writeRedisResult(RedisResult.error(err.message, 'ERR'))
      return
    }

    this.logger?.error(err)
    await this.writeRedisResult(
      RedisResult.error('internal server error', 'ERR'),
    )
  }
}
