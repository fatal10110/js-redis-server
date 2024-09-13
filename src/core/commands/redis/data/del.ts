import { DataCommand } from '..'
import { WrongNumberOfArguments } from '../../../errors'
import { Node } from '../../../node'

export class DelCommand implements DataCommand {
  getKeys(args: Buffer[]): Buffer[] {
    return args
  }

  run(node: Node, args: Buffer[]): number {
    if (!args.length) {
      throw new WrongNumberOfArguments('del')
    }

    let counter = 0

    for (const key of args) {
      if (node.db.del(key)) {
        counter++
      }
    }

    return counter
  }
}

export default new DelCommand()
