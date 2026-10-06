import { Server, Socket, createServer } from 'net'
import type { CommandExecutor } from '../../command-executor'
import type { Logger } from '../../../logger'
import type { RedisClusterNodeRole, RedisServerState } from '../../../state'
import { formatHostPort, formatSocketAddressParts } from '../../network-address'
import { attachSession } from '../attach-session'
import { SocketConnectionTransport } from '../socket-connection-transport'

export type Resp2ServerOptions = {
  server: RedisServerState
  executor: CommandExecutor
  logger?: Pick<Logger, 'error'>
  nodeRole?: RedisClusterNodeRole
}

export class Resp2Server {
  readonly server: Server

  private readonly state: RedisServerState
  private readonly executor: CommandExecutor
  private readonly logger?: Pick<Logger, 'error'>
  private readonly nodeRole?: RedisClusterNodeRole

  constructor(options: Resp2ServerOptions) {
    this.state = options.server
    this.executor = options.executor
    this.logger = options.logger
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
      clientAddress: formatSocketAddressParts(
        socket.remoteAddress,
        socket.remotePort,
      ),
      localAddress: formatSocketAddressParts(
        socket.localAddress,
        socket.localPort,
      ),
      fd: socketFd(socket),
    })
  }
}

/**
 * The OS file descriptor behind an accepted socket, which real Redis reports
 * as CLIENT LIST's `fd=`. Node keeps it on the libuv handle (`TCPWrap.fd`,
 * not public API); it is -1 where the platform has none to expose (Windows).
 */
function socketFd(socket: Socket): number | undefined {
  const fd = (socket as unknown as { _handle?: { fd?: unknown } })._handle?.fd
  return typeof fd === 'number' && fd >= 0 ? fd : undefined
}
