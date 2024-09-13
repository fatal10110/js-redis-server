import { DataCommand } from '..'
import { StringDataType } from '../../../../data-structures/string'
import { WrongNumberOfArguments } from '../../../errors'
import { Node } from '../../../node'

export class MgetCommand implements DataCommand {
  getKeys(args: Buffer[]): Buffer[] {
    return args
  }
  run(node: Node, args: Buffer[]): unknown {
    if (args.length === 0) {
      throw new WrongNumberOfArguments('mget')
    }

    const res: (Buffer | null)[] = []

    for (const key of args) {
      const val = node.db.get(key)

      if (val instanceof StringDataType) {
        res.push(val.data)
      } else {
        res.push(null)
      }
    }

    return res
  }
}

export default new MgetCommand()
