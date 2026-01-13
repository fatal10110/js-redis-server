import { WrongNumberOfArguments } from '../../../../core/errors'
import { Command, CommandResult } from '../../../../types'
import { defineCommand, CommandCategory } from '../metadata'
import type { CommandDefinition } from '../registry'

export const MonitorCommandDefinition: CommandDefinition = {
  metadata: defineCommand('monitor', {
    arity: 1, // MONITOR
    flags: {
      admin: true,
      blocking: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.SERVER],
  }),
  factory: () => new MonitorCommand(),
}

class MonitorCommand implements Command {
  readonly metadata = MonitorCommandDefinition.metadata

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }
  async run(_rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    await new Promise(resolve => {
      setTimeout(resolve, 10000)
    })

    return {
      response: 'OK',
    }
  }
}

export default function () {
  return new MonitorCommand()
}
