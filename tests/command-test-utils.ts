import { Command } from '../src/types'
import { DB } from '../src/commanders/custom/db'
import { createCommands } from '../src/commanders/custom/commands/redis'
import { Session } from '../src/core/transports/session'
import { RedisKernel } from '../src/commanders/custom/redis-kernel'
import { RegistryCommandValidator } from '../src/core/transports/command-validator'
import { NormalState } from '../src/core/transports/session-state'
import { createMockTransport } from './mock-transport'

export function runCommand(
  command: Command,
  rawCmd: string | Buffer,
  args: Buffer[],
) {
  const transport = createMockTransport()
  const raw = typeof rawCmd === 'string' ? Buffer.from(rawCmd) : rawCmd
  command.run(raw, args, new AbortController().signal, transport)
  return {
    response: transport.getLastResponse(),
    transport,
  }
}

export function createTestSession(db: DB) {
  const commands = createCommands(db)
  const validator = new RegistryCommandValidator(commands)
  let session: Session
  const kernel = new RedisKernel(async job => session.executeJob(job))
  session = new Session(commands, kernel, new NormalState(validator, db))

  return {
    execute(
      transport: ReturnType<typeof createMockTransport>,
      rawCmd: string | Buffer,
      args: Buffer[],
      signal: AbortSignal = new AbortController().signal,
    ) {
      const raw = typeof rawCmd === 'string' ? Buffer.from(rawCmd) : rawCmd
      return session.handle(transport, raw, args, signal)
    },
  }
}
