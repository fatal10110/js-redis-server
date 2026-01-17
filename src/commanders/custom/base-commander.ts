import { Socket } from 'net'
import { Command, ExecutionContext, Logger } from '../../types'
import { CommandJob, RedisKernel } from './redis-kernel'
import { RespAdapter } from '../../core/transports/resp2/adapter'
import { Session } from '../../core/transports/session'
import { RegistryCommandValidator } from '../../core/transports/command-validator'
import { NormalState } from '../../core/transports/session-state'
import { CommandExecutionContext } from './execution-context'

type InitialStateFactory = (validator: RegistryCommandValidator) => NormalState
type ExecutionContextFactory = (
  commands: Record<string, Command>,
) => ExecutionContext

export class BaseCommander {
  private readonly kernel: RedisKernel
  private readonly sessions = new Map<string, Session>()
  private readonly validator: RegistryCommandValidator
  private readonly createExecutionContext: ExecutionContextFactory

  constructor(
    private readonly commands: Record<string, Command>,
    private readonly createInitialState: InitialStateFactory,
    createExecutionContext?: ExecutionContextFactory,
  ) {
    this.validator = new RegistryCommandValidator(this.commands)
    this.createExecutionContext =
      createExecutionContext ??
      (commands => new CommandExecutionContext(commands))
    this.kernel = new RedisKernel(this.handleJob.bind(this))
  }

  async shutdown(): Promise<void> {
    const shutdownPromises = Array.from(this.sessions.values()).map(session =>
      session.shutdown(),
    )
    await Promise.all(shutdownPromises)
    this.sessions.clear()
  }

  createAdapter(logger: Logger, socket: Socket): RespAdapter {
    const context = this.createExecutionContext(this.commands)
    const session = new Session(
      context,
      this.kernel,
      this.createInitialState(this.validator),
    )
    const connectionId = session.getConnectionId()
    this.sessions.set(connectionId, session)

    socket.on('close', () => {
      this.sessions.delete(connectionId)
      session.shutdown().catch(err => logger.error(err))
    })

    return new RespAdapter(logger, socket, session)
  }

  private async handleJob(job: CommandJob): Promise<void> {
    const session = this.sessions.get(job.connectionId)

    if (!session) {
      throw new Error(`Session not found for connection ${job.connectionId}`)
    }

    await session.executeJob(job)
  }
}
