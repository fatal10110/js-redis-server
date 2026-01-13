import { WrongNumberOfArguments } from '../../../../../core/errors'
import {
  Command,
  CommandResult,
  DiscoveryNode,
  DiscoveryService,
} from '../../../../../types'
import { defineCommand, CommandCategory } from '../../metadata'
import type { CommandDefinition } from '../../registry'

export const commandName = 'shards'

export const ClusterShardsCommandDefinition: CommandDefinition = {
  metadata: defineCommand(`cluster|${commandName}`, {
    arity: 1, // CLUSTER SHARDS
    flags: {
      admin: true,
      readonly: true,
    },
    firstKey: -1,
    lastKey: -1,
    keyStep: 1,
    categories: [CommandCategory.CLUSTER],
  }),
  factory: deps => {
    if (!deps.discoveryService || !deps.mySelfId) {
      throw new Error('Cluster shards requires discoveryService and mySelfId')
    }

    const me = deps.discoveryService.getById(deps.mySelfId)
    return new ClusterShardsCommand(me, deps.discoveryService)
  },
}

export class ClusterShardsCommand implements Command {
  readonly metadata = ClusterShardsCommandDefinition.metadata

  constructor(
    private readonly me: DiscoveryNode,
    private readonly discoveryService: DiscoveryService,
  ) {}

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return []
  }

  run(rawCommand: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length > 0) {
      throw new WrongNumberOfArguments(this.metadata.name)
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
          return [
            'id',
            clusterNode.id,
            'port',
            clusterNode.port,
            'ip',
            clusterNode.host,
            'endpoint',
            clusterNode.host,
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
