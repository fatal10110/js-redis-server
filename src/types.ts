import { Socket } from 'net'
import { DB } from './core/db'

export interface CommandProvider {
  getOrCreateCommand(socket: Socket, rawCmd: Buffer, args: Buffer[]): Command
}

export interface Logger {
  info(msg: unknown, metadata?: Record<string, unknown>): void
  error(msg: unknown, metadata?: Record<string, unknown>): void
}

export interface Node {
  readonly db: DB
  commandExecutor: CommandProvider
  write(socket: Socket, responseData: unknown, close?: boolean): void
}

export type CommandsInput = Record<string, CommandBuilder>
export type CommandBuilder = (socket: Socket) => Command

export type CommandResult = {
  close?: boolean
  response: unknown
}

export interface Command {
  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[]
  run(rawCmd: Buffer, args: Buffer[]): CommandResult
}
