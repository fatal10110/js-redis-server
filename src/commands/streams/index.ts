import { xackCommand } from './xack'
import { xaddCommand } from './xadd'
import { xautoclaimCommand } from './xautoclaim'
import { xclaimCommand } from './xclaim'
import { xdelCommand } from './xdel'
import { xgroupCommand } from './xgroup'
import { xinfoCommand } from './xinfo'
import { xlenCommand } from './xlen'
import { xpendingCommand } from './xpending'
import { xrangeCommand, xrevrangeCommand } from './xrange'
import { xreadCommand } from './xread'
import { xreadgroupCommand } from './xreadgroup'
import { xsetidCommand } from './xsetid'
import { xtrimCommand } from './xtrim'

export const streamsCommands = [
  xaddCommand,
  xlenCommand,
  xrangeCommand,
  xrevrangeCommand,
  xdelCommand,
  xtrimCommand,
  xreadCommand,
  xgroupCommand,
  xreadgroupCommand,
  xackCommand,
  xpendingCommand,
  xclaimCommand,
  xautoclaimCommand,
  xinfoCommand,
  xsetidCommand,
]

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
  xrangeCommand,
  xreadCommand,
  xreadgroupCommand,
  xrevrangeCommand,
  xsetidCommand,
  xtrimCommand,
}
