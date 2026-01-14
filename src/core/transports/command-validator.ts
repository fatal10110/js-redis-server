import { Command } from '../../types'
import {
  UnknownCommand,
  WrongNumberOfArguments,
  ExpectedInteger,
  WrongNumberOfKeys,
} from '../errors'
import { CommandValidator } from './session-state'

/**
 * Validates commands using the command registry.
 * Used by TransactionState to validate commands before buffering.
 */
export class RegistryCommandValidator implements CommandValidator {
  constructor(private readonly commands: Record<string, Command>) {}

  validate(command: string, args: Buffer[]): void {
    const cmd = this.commands[command]

    if (!cmd) {
      throw new UnknownCommand(command, args)
    }

    // Validate command syntax using getKeys (which throws on syntax errors)
    try {
      cmd.getKeys(Buffer.from(command), args)
    } catch (err) {
      // Re-throw syntax/validation errors that should abort the transaction
      if (
        err instanceof WrongNumberOfArguments ||
        err instanceof UnknownCommand ||
        err instanceof ExpectedInteger ||
        err instanceof WrongNumberOfKeys
      ) {
        throw err
      }
      // Ignore other errors from getKeys (they'll be caught during execution)
    }
  }
}
