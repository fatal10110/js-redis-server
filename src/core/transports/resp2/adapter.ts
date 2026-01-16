import { Socket } from 'net'
import Resp from 'respjs'
import { Logger, Transport } from '../../../types'
import { UserFacedError } from '../../errors'
import { Session } from '../session'

/**
 * RespTransport handles encoding responses to RESP format.
 */
class RespTransport implements Transport {
  constructor(
    private readonly logger: Logger,
    private readonly socket: Socket,
  ) {}

  write(responseData: unknown, close?: boolean) {
    if (
      responseData instanceof Error &&
      !(responseData instanceof UserFacedError)
    ) {
      close = true
    }

    this.socket.write(this.prepareResponse(responseData), err => {
      if (err) {
        this.logger.error(err)
        this.socket.destroySoon()
      }
    })

    if (close) {
      this.socket.destroySoon()
    }
  }

  private prepareResponse(jsResponse: unknown): Buffer {
    if (jsResponse === null) {
      return Resp.encodeNull()
    } else if (jsResponse instanceof Error) {
      return Resp.encodeError(jsResponse)
    } else if (typeof jsResponse === 'bigint') {
      // TODO fix respjs
      return Buffer.from(`:${jsResponse.toString()}\r\n`)
    } else if (Number.isInteger(jsResponse)) {
      return Resp.encodeInteger(jsResponse as number)
    } else if (Array.isArray(jsResponse)) {
      return Resp.encodeArray(jsResponse.map(this.prepareResponse.bind(this)))
    } else if (Buffer.isBuffer(jsResponse)) {
      return Resp.encodeBufBulk(jsResponse)
    } else if (typeof jsResponse === 'string') {
      return Resp.encodeString(jsResponse)
    } else if (typeof jsResponse === 'object') {
      const keys = Object.keys(jsResponse)

      if (keys.length === 0) {
        return Resp.encodeNullArray()
      } else {
        const arr = []

        for (const k of keys) {
          arr.push(Resp.encodeString(k))
          arr.push(
            this.prepareResponse((jsResponse as Record<string, unknown>)[k]),
          )
        }

        return Resp.encodeArray(arr)
      }
    } else {
      throw new Error(`Unknown response of type ${typeof jsResponse}`)
    }
  }
}

/**
 * RespAdapter manages a single client connection using the RESP protocol.
 * It uses a Session to track execution state and delegates command handling.
 *
 * Commands from the same connection are processed sequentially using a queue.
 * This ensures that blocking commands (like BLPOP) don't allow subsequent
 * commands to be processed out of order.
 */
export class RespAdapter {
  private controller: AbortController
  private transport: RespTransport
  /** Queue of pending commands for this connection */
  private commandQueue: Array<{ cmdName: Buffer; args: Buffer[] }> = []
  /** Whether we're currently processing commands from the queue */
  private isProcessing = false
  /** Whether processing is paused due to backpressure */
  private isPaused = false

  constructor(
    private readonly logger: Logger,
    private readonly socket: Socket,
    private readonly session: Session,
  ) {
    this.controller = new AbortController()
    this.transport = new RespTransport(this.logger, socket)

    // Set up socket lifecycle handlers
    socket
      .on('close', () => {
        this.controller.abort()
      })
      .on('error', () => {
        this.controller.abort()
      })
      .on('timeout', () => {
        this.controller.abort()
      })
      .on('end', () => {
        this.controller.abort()
      })

    // Handle backpressure
    socket.on('drain', () => {
      this.isPaused = false
      this.processQueue()
    })

    // Set up RESP parser
    const parser = new Resp({ bufBulk: true })
      .on('error', (err: unknown) => {
        this.transport.write(err)
      })
      .on('data', (data: Buffer[]) => {
        const [cmdName, ...args] = data

        // Queue the command and process sequentially
        this.commandQueue.push({ cmdName, args })
        this.processQueue()
      })

    // Pipe socket through parser
    socket.pipe(parser)
  }

  /**
   * Process commands from the queue sequentially.
   * Ensures commands from this connection complete in order,
   * even when some commands are suspended (e.g., BLPOP).
   */
  private async processQueue(): Promise<void> {
    if (this.isProcessing || this.isPaused) return
    this.isProcessing = true

    while (this.commandQueue.length > 0) {
      // Check for backpressure
      if (this.socket.writableNeedDrain) {
        this.isPaused = true
        this.isProcessing = false
        return
      }

      const cmd = this.commandQueue.shift()!

      try {
        // Delegate to Session, which handles MULTI/EXEC buffering internally
        // This await blocks until the command completes, including suspended commands
        await this.session.handle(
          this.transport,
          cmd.cmdName,
          cmd.args,
          this.controller.signal,
        )
      } catch (err) {
        this.transport.write(err)
      }
    }

    this.isProcessing = false
  }
}
