import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'

export class FlushdbCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(): Buffer[] {
    return []
  }

  run(): Promise<CommandResult> {
    this.db.flushdb()
    return Promise.resolve({ response: 'OK' })
  }
}

export default function (db: DB): Command {
  return new FlushdbCommand(db)
}
