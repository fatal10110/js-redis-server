import { Command, CommandResult } from '../../../../../../types'
import { DB } from '../../../../db'

export class FlushallCommand implements Command {
  constructor(private readonly db: DB) {}

  getKeys(): Buffer[] {
    return []
  }

  run(): Promise<CommandResult> {
    this.db.flushall()
    return Promise.resolve({ response: 'OK' })
  }
}

export default function (db: DB): Command {
  return new FlushallCommand(db)
}
