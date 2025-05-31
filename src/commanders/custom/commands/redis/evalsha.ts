import { Command, CommandResult } from '../../../../types'

export class EvalShaCommand implements Command {
  constructor(
    private readonly scriptsStore: Record<string, Buffer>,
    private readonly evalCommand: Command,
  ) {}

  getKeys(): Buffer[] {
    throw new Error('Method not implemented.')
  }
  run(): Promise<CommandResult> {
    throw new Error('Method not implemented.')
  }
}
