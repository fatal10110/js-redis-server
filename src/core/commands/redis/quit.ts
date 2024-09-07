import { NodeClientCommand } from '.'
import { HandlingResult, Node } from '../../node'

export class QuitCommand implements NodeClientCommand {
  handle(node: Node, args: unknown[]): HandlingResult {
    return {
      close: true,
      response: 'OK',
    }
  }
}

export default new QuitCommand()
