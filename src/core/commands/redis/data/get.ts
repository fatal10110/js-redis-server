import { Command, CommandResult, Node } from '../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { WrongNumberOfArguments, WrongType } from '../../../errors'

export class GetCommand implements Command {
  constructor(private readonly node: Node) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments('get')
    }

    return args
  }

  run(rawCmd: Buffer, args: Buffer[]): CommandResult {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments('get')
    }

    const val = this.node.db.get(args[0])

    if (val === null) return { response: null }

    if (!(val instanceof StringDataType)) {
      throw new WrongType(args[0].toString())
    }

    return { response: val.data }
  }
}

export default function (node: Node) {
  return function () {
    return new GetCommand(node)
  }
}
