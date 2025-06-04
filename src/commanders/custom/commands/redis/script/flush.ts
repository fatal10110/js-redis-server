import { Command, CommandResult } from '../../../../../types'

export class ScriptFlushCommand implements Command {
  constructor(private readonly scriptStore: Record<string, Buffer>) {}

  getKeys(): Buffer[] {
    return []
  }

  run(_rawCmd: Buffer, _args: Buffer[]): Promise<CommandResult> {
    // Clear all scripts from cache
    for (const key of Object.keys(this.scriptStore)) {
      delete this.scriptStore[key]
    }

    return Promise.resolve({ response: 'OK' })
  }
}
