import { NodeClientCommand } from '..'
import { HandlingResult, Node } from '../../../node'

export class ClientSetName implements NodeClientCommand {
  handle(node: Node, args: unknown[]): HandlingResult {
    return {
      response: 'OK',
    }
  }
}

export default new ClientSetName()
