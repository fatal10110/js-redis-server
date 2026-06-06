import { Socket } from 'net'
import { Command, Logger } from '../../types'
import { RedisKernel } from './redis-kernel'
import { RespAdapter } from '../../core/transports/resp2/adapter'
import { Session } from '../../core/transports/session'
import { RegistryCommandValidator } from '../../core/transports/command-validator'
import { NormalState } from '../../core/transports/session-state'
type InitialStateFactory = (validator: RegistryCommandValidator) => NormalState

export class BaseCommander {
  private readonly kernel: RedisKernel
  private readonly sessions = new Map<string, Session>()
  private readonly validator: RegistryCommandValidator

  constructor(
    private readonly commands: Record<string, Command>,
    private readonly createInitialState: InitialStateFactory,
    private readonly luaCommands?: Record<string, Command>,
  ) {
    this.validator = new RegistryCommandValidator(this.commands)
    this.kernel = new RedisKernel()
  }

  async shutdown(): Promise<void> {
    const shutdownPromises = Array.from(this.sessions.values()).map(session =>
      session.shutdown(),
    )
    await Promise.all(shutdownPromises)
    this.sessions.clear()
  }

  createAdapter(logger: Logger, socket: Socket): RespAdapter {
    const session = new Session(
      this.commands,
      this.kernel,
      this.createInitialState(this.validator),
    )
    if (this.luaCommands) {
      session.setLuaCommands(this.luaCommands)
    }
    const connectionId = session.getConnectionId()
    this.sessions.set(connectionId, session)

    socket.on('close', () => {
      this.sessions.delete(connectionId)
      session.shutdown().catch(err => logger.error(err))
    })

    return new RespAdapter(logger, socket, session)
  }

  // Session execution is now driven by awaiting kernel turns directly.
}
