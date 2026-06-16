import { blmoveCommand } from './blmove'
import { blmpopCommand, lmpopCommand } from './lmpop'
import { blpopCommand, brpopCommand } from './blpop'
import {
  lindexCommand,
  llenCommand,
  lrangeCommand,
  lremCommand,
  lsetCommand,
  ltrimCommand,
} from './access'
import { lposCommand } from './lpos'
import { lpopCommand, rpopCommand } from './pop'
import {
  lpushCommand,
  lpushxCommand,
  rpushCommand,
  rpushxCommand,
} from './push'
import { lmoveCommand, rpoplpushCommand } from './move'

export const listsCommands = [
  lpushCommand,
  rpushCommand,
  lpopCommand,
  rpopCommand,
  llenCommand,
  lrangeCommand,
  lindexCommand,
  lsetCommand,
  lremCommand,
  ltrimCommand,
  lpushxCommand,
  rpushxCommand,
  rpoplpushCommand,
  lposCommand,
  lmoveCommand,
  lmpopCommand,
  blmoveCommand,
  blmpopCommand,
  blpopCommand,
  brpopCommand,
]

export {
  blmoveCommand,
  blmpopCommand,
  blpopCommand,
  brpopCommand,
  lindexCommand,
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
}
