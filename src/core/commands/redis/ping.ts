import { NodeCommand } from '.'
import { DiscoveryService } from '../../cluster/network'
import { Node } from '../../node'

export class Ping implements NodeCommand {
  handle(
    discoveryService: DiscoveryService,
    node: Node,
    args: unknown[],
  ): string | Buffer {
    return 'PONG'
  }
}

export default new Ping()
