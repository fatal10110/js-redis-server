import { Server, Socket, createServer } from 'net'
import type { CommandExecutor } from '../../command-executor'
import type { Logger } from '../../../logger'
import type { RedisClusterNodeRole, RedisServerState } from '../../../state'
import type { RespEncodeOptions } from '../../resp-encoder'
import { formatHostPort, formatSocketAddressParts } from '../../network-address'
import { attachSession } from '../attach-session'
import { SocketConnectionTransport } from '../socket-connection-transport'

export type Resp2ServerOptions = {
  server: RedisServerState
  executor: CommandExecutor
  logger?: Pick<Logger, 'error'>
  encoder?: RespEncodeOptions
  nodeRole?: RedisClusterNodeRole
}

export class Resp2Server {
  readonly server: Server

  private readonly state: RedisServerState
  private readonly executor: CommandExecutor
  private readonly logger?: Pick<Logger, 'error'>
  private readonly encoder?: RespEncodeOptions
  private readonly nodeRole?: RedisClusterNodeRole

  constructor(options: Resp2ServerOptions) {
    this.state = options.server
    this.executor = options.executor
    this.logger = options.logger
    this.encoder = options.encoder
    this.nodeRole = options.nodeRole

    this.server = createServer({ keepAlive: true })
      .on('error', err => this.logger?.error(err))
      .on('connection', socket => this.handleConnection(socket))
  }

  listen(port?: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const onError = (err: Error) => {
        this.server.removeListener('listening', onListening)
        reject(err)
      }
      const onListening = () => {
        this.server.removeListener('error', onError)
        resolve()
      }

      this.server.once('error', onError)
      this.server.once('listening', onListening)
      this.server.listen(port)
    })
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) =>
      this.server.close(err => {
        this.state.close()
        if (err) {
          reject(err)
          return
        }
        resolve()
      }),
    )
  }

  getAddress(): string {
    return formatHostPort('127.0.0.1', this.getPort())
  }

  getPort(): number {
    const address = this.server.address()
    if (!address || typeof address === 'string') {
      throw new Error('Server not listening')
    }
    return address.port
  }

  private handleConnection(socket: Socket) {
    const transport = new SocketConnectionTransport(socket)
    // Fire-and-forget: the returned `done` promise already swallows and logs
    // its own errors, and the session tears itself down when the socket closes.
    attachSession(transport, {
      state: this.state,
      executor: this.executor,
      nodeRole: this.nodeRole,
      logger: this.logger,
      encoder: this.encoder,
      clientAddress: formatSocketAddressParts(
        socket.remoteAddress,
        socket.remotePort,
      ),
    })
  }
}
