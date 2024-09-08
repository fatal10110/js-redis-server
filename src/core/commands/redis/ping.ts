import { NodeClientCommand } from '.'
import { HandlingResult, Node } from '../../node'

export class Ping implements NodeClientCommand {
  handle(node: Node, args: unknown[]): HandlingResult {
    return { response: 'PONG' }
  }
}

export default new Ping()
