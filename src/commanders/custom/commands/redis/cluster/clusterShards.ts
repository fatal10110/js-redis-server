import { WrongNumberOfArguments } from '../../../../../core/errors'
import {
  Command,
  CommandResult,
  DiscoveryNode,
  DiscoveryService,
} from '../../../../../types'

export const commandName = 'shards'

export class ClusterShardsCommand implements Command {
  constructor(
    private readonly me: DiscoveryNode,
    private readonly discoveryService: DiscoveryService,
  ) {}

  getKeys(): Buffer[] {
    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): Prmise<CommandResult> {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(`cluster|${commandName}`)
    }

    const mapping: Record<number, DiscoveryNode[]> = {}

    for (const clusterNode of this.discoveryService.getAll()) {
      const arr = (mapping[clusterNode.slots[0][0]] ??= [])

      if (this.discoveryService.isMaster(this.me.id)) arr.unshift(clusterNode)
      else arr.push(clusterNode)
    }

    const shards: [
      string,
      number[],
      string,
      [
        string,
        string,
        string,
        number,
        string,
        string,
        string,
        string,
        string,
        string,
        string,
        number,
        string,
        string,
      ][],
    ][] = []

    for (const clusterNodes of Object.values(mapping)) {
      const master = clusterNodes[0]
      const slots = master.slots.reduce<number[]>((acc, range) => {
        acc.push(...range)

        return acc
      }, [])

      shards.push([
        'slots',
        slots,
        'nodes',
        clusterNodes.map(clusterNode => {
          const [host, port] = clusterNode.host.split(':')

          return [
            'id',
            clusterNode.id,
            'port',
            Number(port),
            'ip',
            host,
            'endpoint',
            host,
            'role',
            master.id === clusterNode.id ? 'master' : 'replica',
            'replication-offset',
            1,
            'health',
            'online',
          ]
        }),
      ])
    }

    return Promise.resolve({ response: shards })
  }
}
