import { AddressInfo, Server, Socket, createServer } from 'net'
import Resp from 'respjs'
import { DBCommandExecutor, Logger, Transport } from '../../../types'
import { UserFacedError } from '../../errors'

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

export class Resp2Transport {
  public readonly server: Server

  constructor(
    private readonly logger: Logger,
    private readonly commandExecutor: DBCommandExecutor,
  ) {
    this.server = createServer({ keepAlive: true })
      .on('error', err => {
        // Handle errors here.
        logger.error(err)
      })
      .on('close', () => {
        //logger.info('Connection closed')
      })
      .on('connection', socket => {
        socket.pipe(this.handleConnection(socket))
      })
  }

  private handleConnection(socket: Socket) {
    const controller = new AbortController()

    socket
      .on('close', () => {
        controller.abort()
      })
      .on('error', () => {
        controller.abort()
      })
      .on('timeout', () => {
        controller.abort()
      })
      .on('end', () => {
        console.log('end')
        controller.abort()
      })

    const transport = new RespTransport(this.logger, socket)

    return new Resp({ bufBulk: true })
      .on('error', (err: unknown) => {
        transport.write(err)
      })
      .on('data', (data: Buffer[]) => {
        const [cmdName, ...args] = data

        console.log('cmdName', cmdName.toString())
        console.log(
          'args',
          args.map(arg => arg.toString()),
        )

        this.commandExecutor
          .execute(transport, cmdName, args, controller.signal)
          .catch(transport.write)
      })
  }

  listen(port?: number): Promise<void> {
    const promise = new Promise<void>((resolve, reject) => {
      this.server.once('error', reject)
      this.server.once('listening', () => {
        this.server.removeListener('error', reject)
        resolve()
      })
    })

    this.server.listen(port)

    return promise
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) =>
      this.server.close(err => {
        err ? reject(err) : resolve()
      }),
    )
  }

  getAddress(): string {
    const address = this.server.address()

    if (address instanceof String) {
      throw new Error(`Could not fetch address from ${address}`)
    }

    // TODO
    return `127.0.0.1:${(address as AddressInfo).port}`
  }
}
