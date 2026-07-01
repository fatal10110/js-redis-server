import type { CommandDefinition } from '../core/command-definition'
import { CommandExecutor } from '../core/command-executor'
import { CommandRegistry } from '../core/command-registry'
import {
  gateSatisfied,
  resolveCompatibilityProfile,
  type CompatibilityProfile,
  type CompatibilitySpec,
} from '../core/compatibility'
import type { ExecutionPolicy } from '../core/execution-policies'
import {
  createAuthPolicy,
  createSubscribedModePolicy,
  createTransactionPolicy,
} from '../core/execution-policies'
import { bitmapsCommands } from './bitmaps'
import { commandCommand } from './command'
import { configCommands } from './config'
import { connectionCommands } from './connection'
import { geoCommands } from './geo'
import { hashesCommands } from './hashes'
import { keysCommands } from './keys'
import { listsCommands } from './lists'
import { monitorCommands } from './monitor'
import { pubsubCommands } from './pubsub'
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
  ...configCommands,
  ...transactionCommands,
  ...monitorCommands,
  ...stringsCommands,
  ...bitmapsCommands,
  ...keysCommands,
  ...scanCommands,
  ...hashesCommands,
  ...listsCommands,
  ...setsCommands,
  ...zsetsCommands,
  ...geoCommands,
  ...pubsubCommands,
  ...streamsCommands,
  ...scriptsCommands,
]

export function createRedisCommandRegistry(
  extraCommands: readonly CommandDefinition[] = [],
  profile: CompatibilityProfile = resolveCompatibilityProfile(),
): CommandRegistry {
  const registry = new CommandRegistry()
  registry.registerAll(
    filterCompatibleCommands(redisCommandDefinitions, profile),
  )
  registry.registerAll(filterCompatibleCommands(extraCommands, profile))
  return registry
}

export function createRedisCommandExecutor(options?: {
  extraCommands?: readonly CommandDefinition[]
  policies?: readonly ExecutionPolicy[]
  compatibility?: CompatibilitySpec
}): CommandExecutor {
  const profile = resolveCompatibilityProfile(options?.compatibility)
  return new CommandExecutor({
    registry: createRedisCommandRegistry(options?.extraCommands, profile),
    profile,
    policies: [
      createAuthPolicy(),
      createSubscribedModePolicy(),
      ...(options?.policies ?? []),
      createTransactionPolicy(),
    ],
  })
}

function filterCompatibleCommands(
  definitions: readonly CommandDefinition[],
  profile: CompatibilityProfile,
): CommandDefinition[] {
  return definitions.filter(
    definition =>
      definition.since === undefined ||
      gateSatisfied(definition.since, profile),
  )
}

export {
  aclCommand,
  clientCommand,
  pingCommand,
  quitCommand,
  selectCommand,
  shutdownCommand,
  slowlogCommand,
  connectionCommands,
} from './connection'
export { monitorCommand, monitorCommands } from './monitor'
export {
  createClusterCommands,
  readonlyCommand,
  readwriteCommand,
} from './cluster'
export { commandCommand } from './command'
export { configCommand, configCommands } from './config'
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
  substrCommand,
  setrangeCommand,
  getexCommand,
  stringsCommands,
} from './strings'
export {
  bitmapsCommands,
  setbitCommand,
  getbitCommand,
  bitcountCommand,
  bitposCommand,
  bitopCommand,
  bitfieldCommand,
  bitfieldRoCommand,
} from './bitmaps'
export {
  discardCommand,
  execCommand,
  multiCommand,
  transactionCommands,
  unwatchCommand,
  watchCommand,
} from './transactions'
export {
  copyCommand,
  dbsizeCommand,
  delCommand,
  existsCommand,
  expireCommand,
  expireatCommand,
  expiretimeCommand,
  pexpiretimeCommand,
  flushallCommand,
  flushdbCommand,
  keysCommands,
  moveCommand,
  persistCommand,
  pexpireCommand,
  pexpireatCommand,
  pttlCommand,
  randomkeyCommand,
  renameCommand,
  renamenxCommand,
  sortCommand,
  sortRoCommand,
  touchCommand,
  ttlCommand,
  typeCommand,
  unlinkCommand,
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
  blmpopCommand,
  blmoveCommand,
  blpopCommand,
  brpopCommand,
  lindexCommand,
  linsertCommand,
  llenCommand,
  lmoveCommand,
  lmpopCommand,
  lpopCommand,
  lposCommand,
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
  publishCommand,
  psubscribeCommand,
  pubsubCommand,
  pubsubCommands,
  punsubscribeCommand,
  spublishCommand,
  ssubscribeCommand,
  subscribeCommand,
  sunsubscribeCommand,
  unsubscribeCommand,
} from './pubsub'
export {
  hashesCommands,
  hexistsCommand,
  hgetallCommand,
  hgetdelCommand,
  hgetexCommand,
  hgetCommand,
  hdelCommand,
  hincrbyfloatCommand,
  hincrbyCommand,
  hpersistCommand,
  hpttlCommand,
  httlCommand,
  hkeysCommand,
  hlenCommand,
  hmgetCommand,
  hmsetCommand,
  hrandfieldCommand,
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
  sintercardCommand,
  sinterstoreCommand,
  sismemberCommand,
  smismemberCommand,
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
  evalRoCommand,
  evalshaCommand,
  evalshaRoCommand,
  fcallCommand,
  fcallRoCommand,
  functionCommand,
  scriptCommand,
  scriptsCommands,
} from './scripts'
export {
  xackCommand,
  xaddCommand,
  xautoclaimCommand,
  xclaimCommand,
  xdelCommand,
  xgroupCommand,
  xinfoCommand,
  xlenCommand,
  xpendingCommand,
  xreadCommand,
  xreadgroupCommand,
  xrevrangeCommand,
  xrangeCommand,
  streamsCommands,
  xtrimCommand,
} from './streams'
export {
  geoCommands,
  geoaddCommand,
  geoposCommand,
  geodistCommand,
  geohashCommand,
} from './geo'
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
  bzpopminCommand,
  bzpopmaxCommand,
  zmpopCommand,
  bzmpopCommand,
} from './zsets'
