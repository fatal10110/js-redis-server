import { AddressInfo, Server, Socket, createServer } from 'net'
import { DBCommandExecutor, Logger } from '../../../types'

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
        this.handleConnection(socket)
      })
  }

  private handleConnection(socket: Socket) {
    // Phase 4: Delegate to adapter pattern
    if ('createAdapter' in this.commandExecutor) {
      const createAdapter = (this.commandExecutor as any).createAdapter.bind(
        this.commandExecutor,
      )
      createAdapter(this.logger, socket)
    } else {
      throw new Error(
        'CommandExecutor must implement createAdapter method for Phase 4 architecture',
      )
    }
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
