import { LuaFactory } from 'wasmoon'
import { ClusterNode } from '../../cluster/clusterNode'
import { CommandsInput } from '../../../types'
import createEval from './eval'
import createMulti from './multi'
import pingCommand from './ping'
import quitCommand from './quit'
import createClient from './client'
import createCluster from './cluster'
import createGet from './data/get'
import createSet from './data/set'
import createMget from './data/mget'
import createDel from './data/del'
import createCommandInfo from './command'
import createInfo from './info'

export async function createClusterCommandsInputBuilder(): Promise<
  [() => void, (node: ClusterNode) => CommandsInput]
> {
  const factory = new LuaFactory()
  const lua = await factory.createEngine({ injectObjects: true })

  // TODO use better solution, maybe convert to class
  return [
    lua.global.close.bind(lua.global),
    function createClusterCommands(node: ClusterNode): CommandsInput {
      return {
        eval: createEval(node, lua),
        multi: createMulti(node),
        ping: pingCommand,
        quit: quitCommand,
        client: createClient(node),
        cluster: createCluster(node),
        get: createGet(node),
        set: createSet(node),
        mget: createMget(node),
        del: createDel(node),
        command: createCommandInfo,
        info: createInfo,
      }
    },
  ]
}
