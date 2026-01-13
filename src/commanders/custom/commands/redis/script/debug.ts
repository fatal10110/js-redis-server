import { WrongNumberOfArguments } from '../../../../../core/errors'
import { Command, CommandResult } from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const ScriptDebugCommandDefinition: CommandDefinition = {
  metadata: defineCommand('script|debug', {
    arity: 2, // SCRIPT DEBUG <YES|SYNC|NO>
    flags: {
      admin: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SCRIPT],
  }),
  factory: () => new ScriptDebugCommand(),
}

export class ScriptDebugCommand implements Command {
  readonly metadata = ScriptDebugCommandDefinition.metadata

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length !== 1) {
      throw new WrongNumberOfArguments(this.metadata.name)
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
