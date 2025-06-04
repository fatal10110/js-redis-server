import { Command, CommandResult } from '../../../../../types'

export class ScriptKillCommand implements Command {
  getKeys(): Buffer[] {
    return []
  }

  run(_rawCmd: Buffer, _args: Buffer[]): Promise<CommandResult> {
    // In a real implementation, this would kill currently running scripts
    // For now, we'll just return OK since we don't have script execution tracking
    return Promise.resolve({ response: 'OK' })
  }
}
