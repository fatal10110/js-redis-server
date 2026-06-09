import type { CommandDefinition } from '../core/command-definition'
import { CommandExecutor } from '../core/command-executor'
import { CommandRegistry } from '../core/command-registry'
import type { ExecutionPolicy } from '../core/execution-policies'
import { createTransactionPolicy } from '../core/execution-policies'
import { commandCommand } from './command'
import { connectionCommands } from './connection'
import { hashesCommands } from './hashes'
import { keysCommands } from './keys'
import { listsCommands } from './lists'
import { scanCommands } from './scan'
import { scriptsCommands } from './scripts'
import { setsCommands } from './sets'
import { streamsCommands } from './streams'
import { stringsCommands } from './strings'
import { transactionCommands } from './transactions'
import { zsetsCommands } from './zsets'

export const redisCommandDefinitions: readonly CommandDefinition[] = [
  ...connectionCommands,
  commandCommand,
  ...transactionCommands,
  ...stringsCommands,
  ...keysCommands,
  ...scanCommands,
  ...hashesCommands,
  ...listsCommands,
  ...setsCommands,
  ...zsetsCommands,
  ...streamsCommands,
  ...scriptsCommands,
]

export function createRedisCommandRegistry(
  extraCommands: readonly CommandDefinition[] = [],
): CommandRegistry {
  const registry = new CommandRegistry()
  registry.registerAll(redisCommandDefinitions)
  registry.registerAll(extraCommands)
  return registry
}

export function createRedisCommandExecutor(options?: {
  extraCommands?: readonly CommandDefinition[]
  policies?: readonly ExecutionPolicy[]
}): CommandExecutor {
  return new CommandExecutor({
    registry: createRedisCommandRegistry(options?.extraCommands),
    policies: [...(options?.policies ?? []), createTransactionPolicy()],
  })
}

export {
  pingCommand,
  quitCommand,
  selectCommand,
  connectionCommands,
} from './connection'
export {
  createClusterCommand,
  createClusterCommands,
  readonlyCommand,
  readwriteCommand,
} from './cluster'
export { commandCommand } from './command'
export {
  getCommand,
  setCommand,
  mgetCommand,
  appendCommand,
  strlenCommand,
  incrCommand,
  decrCommand,
  incrbyCommand,
  decrbyCommand,
  incrbyfloatCommand,
  getsetCommand,
  getdelCommand,
  setnxCommand,
  setexCommand,
  psetexCommand,
  msetCommand,
  msetnxCommand,
  getrangeCommand,
  setrangeCommand,
  getexCommand,
  stringsCommands,
} from './strings'
export {
  discardCommand,
  execCommand,
  multiCommand,
  transactionCommands,
  unwatchCommand,
  watchCommand,
} from './transactions'
export {
  dbsizeCommand,
  delCommand,
  existsCommand,
  expireCommand,
  expireatCommand,
  flushallCommand,
  flushdbCommand,
  keysCommands,
  persistCommand,
  pexpireCommand,
  pexpireatCommand,
  pttlCommand,
  renameCommand,
  renamenxCommand,
  ttlCommand,
  typeCommand,
} from './keys'
export {
  hscanCommand,
  keysCommand,
  scanCommand,
  scanCommands,
  sscanCommand,
  zscanCommand,
} from './scan'
export {
  listsCommands,
  blpopCommand,
  brpopCommand,
  lindexCommand,
  llenCommand,
  lpopCommand,
  lpushCommand,
  lpushxCommand,
  lrangeCommand,
  lremCommand,
  lsetCommand,
  ltrimCommand,
  rpopCommand,
  rpoplpushCommand,
  rpushCommand,
  rpushxCommand,
} from './lists'
export {
  hashesCommands,
  hexistsCommand,
  hgetallCommand,
  hgetCommand,
  hdelCommand,
  hincrbyfloatCommand,
  hincrbyCommand,
  hkeysCommand,
  hlenCommand,
  hmgetCommand,
  hmsetCommand,
  hsetnxCommand,
  hsetCommand,
  hstrlenCommand,
  hvalsCommand,
} from './hashes'
export {
  setsCommands,
  saddCommand,
  scardCommand,
  sdiffCommand,
  sdiffstoreCommand,
  sinterCommand,
  sinterstoreCommand,
  sismemberCommand,
  smembersCommand,
  smoveCommand,
  spopCommand,
  srandmemberCommand,
  sremCommand,
  sunionCommand,
  sunionstoreCommand,
} from './sets'
export {
  evalCommand,
  evalshaCommand,
  scriptCommand,
  scriptsCommands,
} from './scripts'
export {
  zsetsCommands,
  zaddCommand,
  zremCommand,
  zcardCommand,
  zrankCommand,
  zrevrankCommand,
  zscoreCommand,
  zincrbyCommand,
  zrangeCommand,
  zrevrangeCommand,
  zrangebyscoreCommand,
  zremrangebyscoreCommand,
  zcountCommand,
  zpopminCommand,
  zpopmaxCommand,
} from './zsets'
