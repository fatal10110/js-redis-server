import { Command, CommandResult, Node } from '../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { WrongNumberOfArguments } from '../../../errors'

export class MgetCommand implements Command {
  constructor(private readonly node: Node) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return args
  }
  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    if (args.length === 0) {
      throw new WrongNumberOfArguments('mget')
    }

    const res: (Buffer | null)[] = []

    for (const key of args) {
      const val = this.node.db.get(key)

      if (val instanceof StringDataType) {
        res.push(val.data)
      } else {
        res.push(null)
      }
    }

    return { response: res }
  }
}

export default function (node: Node) {
  return function () {
    return new MgetCommand(node)
  }
}
