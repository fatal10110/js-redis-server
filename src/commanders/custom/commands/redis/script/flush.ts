import { Command, CommandResult } from '../../../../../types'
import { DB } from '../../../db'

export class ScriptFlushCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(): Buffer[] {
    return []
  }

  run(): Promise<CommandResult> {
    this.db.flushScripts()

    return Promise.resolve({ response: 'OK' })
  }
}
