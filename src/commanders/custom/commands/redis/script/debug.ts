import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'

export class ScriptDebugCommand implements Command {
  getKeys(): Buffer[] {
    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments('script|debug')
    }

    const mode = args[0].toString().toLowerCase()

    // Validate the debug mode
    if (!['yes', 'sync', 'no'].includes(mode)) {
      throw new Error('ERR debug mode must be one of: YES, SYNC, NO')
    }

    // In a real implementation, this would set the debug mode for script execution
    // For now, we'll just validate the argument and return OK
    return Promise.resolve({ response: 'OK' })
  }
}
