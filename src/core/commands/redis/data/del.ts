import { Command, CommandBuilder, CommandResult, Node } from '../../../../types'
import { WrongNumberOfArguments } from '../../../errors'

export class DelCommand implements Command {
  constructor(private readonly node: Node) {}
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    if (!args.length) {
      throw new WrongNumberOfArguments('del')
    }

    let counter = 0

    for (const key of args) {
      if (this.node.db.del(key)) {
        counter++
      }
    }

    return { response: counter }
  }
}

export default function (node: Node): CommandBuilder {
  return function (): Command {
    return new DelCommand(node)
  }
}
