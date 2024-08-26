import { DB } from '../db'
import del from './del'
import get from './get'
import set from './set'

export type CommandResult = number | null | Buffer | Iterable<unknown> | string
export type Command = (db: DB, args: Buffer[]) => CommandResult

const hashCommands = {}

const stringCommands = {
  get,
}

const commands: Record<string, Command> = {
  del,
  set,
  ...stringCommands,
}

export default commands
