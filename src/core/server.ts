import net, { Server, Socket } from 'net'
import Resp from 'respjs'
import { Node } from './node'
import { UserFacedError } from './errors'

// TODO move to shared types
export interface Logger {
  info(msg: unknown, metadata?: Record<string, unknown>): void
  error(msg: unknown, metadata?: Record<string, unknown>): void
}

export interface NetworkInterface {
  createInterface(node: Node): Server
}

export class ServerNetwork implements NetworkInterface {
  constructor(private readonly logger: Logger) {}

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

  private handleResponse(
    socket: Socket,
    responseData: unknown,
    close: boolean,
  ) {
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

  private createResp(socket: Socket, node: Node) {
    return new Resp({ bufBulk: true })
      .on('error', function (error: unknown) {
        socket.write(Resp.encodeError(error as Error))
      })
      .on('data', (data: Buffer[]) => {
        const [cmd, ...args] = data

        node
          .request(cmd, args)
          .then(result => {
            this.handleResponse(socket, result.response, !!result.close)
          })
          .catch((err: unknown) => {
            if (err instanceof UserFacedError) {
              this.handleResponse(socket, err, false)
            } else {
              this.logger.error(`Error on processing incoming data`, { err })
              this.handleResponse(
                socket,
                new UserFacedError(`ERR Error!`),
                true,
              )
            }
          })
      })
  }

  createInterface(node: Node): Server {
    return net
      .createServer({ keepAlive: true })
      .on('error', err => {
        // Handle errors here.
        this.logger.error(err)
      })
      .on('close', () => {
        this.logger.info('Connection closed')
      })
      .on('connection', socket => {
        socket.pipe(this.createResp(socket, node))
      })
  }
}
