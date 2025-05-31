import { AddressInfo, Server, Socket, createServer } from 'net'
import Resp from 'respjs'
import { DBCommandExecutor, Logger } from '../../../types'
import { UserFacedError } from '../../errors'

export class Resp2Transport {
  public readonly server: Server

  constructor(
    private readonly logger: Logger,
    private readonly dbProvider: () => DBCommandExecutor,
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

  write(socket: Socket, responseData: unknown, close?: boolean) {
    socket.write(this.prepareResponse(responseData), err => {
      if (err) {
        this.logger.error(err)
        socket.destroySoon()
      }
    })

    if (close) {
      socket.destroySoon()
    }
  }

  private handleError(socket: Socket, err: unknown) {
    if (err instanceof UserFacedError) {
      this.write(socket, err, false)
    } else {
      this.logger.error(`Error on processing incoming data`, { err })
      this.write(socket, new UserFacedError(`ERR Error!`), true)
    }
  }

  private handleConnection(socket: Socket) {
    const commandExecutor = this.dbProvider()

    socket
      .on('close', () => {
        commandExecutor.shutdown().catch(err => {
          this.logger.error(`Error on shutting down command executor`, { err })
        })
      })
      .on('error', () => {
        commandExecutor.shutdown().catch(err => {
          this.logger.error(`Error on shutting down command executor`, { err })
        })
      })
      .on('timeout', () => {
        commandExecutor.shutdown().catch(err => {
          this.logger.error(`Error on shutting down command executor`, { err })
        })
      })

    return new Resp({ bufBulk: true })
      .on('error', (err: unknown) => {
        this.handleError(socket, err)
      })
      .on('data', (data: Buffer[]) => {
        const [cmdName, ...args] = data

        try {
          socket.pause()

          commandExecutor
            .execute(cmdName, args)
            .then(({ response, close }) => {
              this.write(socket, response, close)
            })
            .catch(err => {
              this.handleError(socket, err)
            })
            .finally(() => {
              socket.resume()
            })
        } catch (err) {
          this.handleError(socket, err)
          socket.resume()
        }
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

    return `127.0.0.1:${(address as AddressInfo).port}`
  }
}
