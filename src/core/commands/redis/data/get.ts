import { DataCommand } from '..'
import { StringDataType } from '../../../../data-structures/string'
import { WrongNumberOfArguments, WrongType } from '../../../errors'
import { Node } from '../../../node'

export class GetCommand implements DataCommand {
  getKeys(args: Buffer[]): Buffer[] {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments('get')
    }

    return args
  }

  run(node: Node, args: Buffer[]): unknown {
    if (!args.length || args.length > 1) {
      throw new WrongNumberOfArguments('get')
    }

    const val = node.db.get(args[0])

    if (val === null) return null

    if (!(val instanceof StringDataType)) {
      throw new WrongType(args[0].toString())
    }

    return val.data
  }
}

export default new GetCommand()
