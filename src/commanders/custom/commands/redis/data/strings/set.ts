import {
  WrongNumberOfArguments,
  RedisSyntaxError,
  WrongType,
  ExpectedInteger,
  InvalidExpireTime,
} from '../../../../../../core/errors'
import { Command, CommandResult } from '../../../../../../types'
import { StringDataType } from '../../../../data-structures/string'
import { DB } from '../../../../db'
import { defineCommand, CommandCategory } from '../../../metadata'
import type { CommandDefinition } from '../../../registry'

interface SetOptions {
  expiration?: number
  nx?: boolean
  xx?: boolean
  keepTtl?: boolean
  get?: boolean
}

// Command definition with metadata
export const SetCommandDefinition: CommandDefinition = {
  metadata: defineCommand('set', {
    arity: -3, // SET key value [options...]
    flags: {
      write: true,
      denyoom: true,
    },
    firstKey: 0,
    lastKey: 0,
    keyStep: 1,
    categories: [CommandCategory.STRING],
  }),
  factory: deps => new SetCommand(deps.db),
}

export class SetCommand implements Command {
  readonly metadata = SetCommandDefinition.metadata

  constructor(private readonly db: DB) {}

  getKeys(rawCmd: Buffer, args: Buffer[]): Buffer[] {
    if (args.length < 2) {
      // SET command requires at least 2 arguments: key and value
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    return [args[0]]
  }

  run(rawCmd: Buffer, args: Buffer[]): Promise<CommandResult> {
    if (args.length < 2) {
      // SET command requires at least 2 arguments: key and value
      throw new WrongNumberOfArguments(this.metadata.name)
    }

    const [key, value] = args

    // Parse options from remaining arguments
    const options = this.parseOptions(args.slice(2))

    // Get existing value and type
    const existingData = this.db.get(key)
    let oldValue: Buffer | null = null

    // Handle GET option - return old value if it exists and is a string
    if (options.get) {
      if (existingData instanceof StringDataType) {
        oldValue = existingData.data
      } else if (existingData !== null) {
        // Cannot use GET option on non-string data types (Redis compatibility)
        throw new WrongType()
      }
    }

    // Handle NX/XX conditions
    if (options.nx && existingData !== null) {
      // NX: only set if key doesn't exist, but key exists
      if (options.get) {
        return Promise.resolve({ response: oldValue })
      }
      return Promise.resolve({ response: null })
    }

    if (options.xx && existingData === null) {
      // XX: only set if key exists, but key doesn't exist
      if (options.get) {
        return Promise.resolve({ response: null })
      }
      return Promise.resolve({ response: null })
    }

    // If key exists and is not a string type, delete it first
    if (existingData !== null && !(existingData instanceof StringDataType)) {
      this.db.del(key)
    }

    // Calculate expiration
    let expiration: number | undefined

    if (options.keepTtl && existingData instanceof StringDataType) {
      // Keep existing TTL - we need to get the current TTL
      // This is a limitation of the current DB implementation
      // In a real Redis implementation, we would preserve the exact TTL
      expiration = undefined // Current DB doesn't expose TTL getter
    } else if (options.expiration !== undefined) {
      expiration = options.expiration
    }

    // Set the new value
    this.db.set(key, new StringDataType(value), expiration)

    // Return appropriate response
    if (options.get) {
      return Promise.resolve({ response: oldValue })
    }

    return Promise.resolve({ response: 'OK' })
  }

  private parseOptions(args: Buffer[]): SetOptions {
    const options: SetOptions = {}
    let i = 0

    while (i < args.length) {
      const option = args[i].toString().toUpperCase()

      switch (option) {
        // EX seconds - Set expiration time in seconds (relative to current time)
        case 'EX': {
          if (i + 1 >= args.length) {
            // EX option requires a value argument (number of seconds)
            throw new RedisSyntaxError()
          }
          if (options.expiration !== undefined) {
            // Cannot specify multiple expiration options (EX, PX, EXAT, PXAT)
            throw new RedisSyntaxError()
          }
          const seconds = parseInt(args[i + 1].toString())
          if (isNaN(seconds)) {
            // EX value must be an integer
            throw new ExpectedInteger()
          }
          if (seconds <= 0) {
            // EX value must be positive (cannot set negative expire time)
            throw new InvalidExpireTime('set')
          }
          options.expiration = Date.now() + seconds * 1000
          i += 2
          break
        }

        // PX milliseconds - Set expiration time in milliseconds (relative to current time)
        case 'PX': {
          if (i + 1 >= args.length) {
            // PX option requires a value argument (number of milliseconds)
            throw new RedisSyntaxError()
          }
          if (options.expiration !== undefined) {
            // Cannot specify multiple expiration options (EX, PX, EXAT, PXAT)
            throw new RedisSyntaxError()
          }
          const milliseconds = parseInt(args[i + 1].toString())
          if (isNaN(milliseconds)) {
            // PX value must be an integer
            throw new ExpectedInteger()
          }
          if (milliseconds <= 0) {
            // PX value must be positive (cannot set negative expire time)
            throw new InvalidExpireTime('set')
          }
          options.expiration = Date.now() + milliseconds
          i += 2
          break
        }

        // EXAT timestamp-seconds - Set expiration time as Unix timestamp in seconds
        case 'EXAT': {
          if (i + 1 >= args.length) {
            // EXAT option requires a value argument (Unix timestamp in seconds)
            throw new RedisSyntaxError()
          }
          if (options.expiration !== undefined) {
            // Cannot specify multiple expiration options (EX, PX, EXAT, PXAT)
            throw new RedisSyntaxError()
          }
          const timestampSeconds = parseInt(args[i + 1].toString())
          if (isNaN(timestampSeconds)) {
            // EXAT value must be an integer
            throw new ExpectedInteger()
          }
          if (timestampSeconds <= 0) {
            // EXAT value must be positive (cannot set negative expire time)
            throw new InvalidExpireTime('set')
          }
          options.expiration = timestampSeconds * 1000
          i += 2
          break
        }

        // PXAT timestamp-milliseconds - Set expiration time as Unix timestamp in milliseconds
        case 'PXAT': {
          if (i + 1 >= args.length) {
            // PXAT option requires a value argument (Unix timestamp in milliseconds)
            throw new RedisSyntaxError()
          }
          if (options.expiration !== undefined) {
            // Cannot specify multiple expiration options (EX, PX, EXAT, PXAT)
            throw new RedisSyntaxError()
          }
          const timestampMilliseconds = parseInt(args[i + 1].toString())
          if (isNaN(timestampMilliseconds)) {
            // PXAT value must be an integer
            throw new ExpectedInteger()
          }
          if (timestampMilliseconds <= 0) {
            // PXAT value must be positive (cannot set negative expire time)
            throw new InvalidExpireTime('set')
          }
          options.expiration = timestampMilliseconds
          i += 2
          break
        }

        // NX - Only set the key if it does NOT exist (create new key only)
        case 'NX':
          if (options.xx) {
            // Cannot use both NX and XX options together (they are mutually exclusive)
            throw new RedisSyntaxError()
          }
          options.nx = true
          i += 1
          break

        // XX - Only set the key if it already EXISTS (update existing key only)
        case 'XX':
          if (options.nx) {
            // Cannot use both NX and XX options together (they are mutually exclusive)
            throw new RedisSyntaxError()
          }
          options.xx = true
          i += 1
          break

        // KEEPTTL - Retain the existing time to live (TTL) of the key
        case 'KEEPTTL':
          if (options.expiration !== undefined) {
            // Cannot use KEEPTTL with expiration options (EX, PX, EXAT, PXAT)
            throw new RedisSyntaxError()
          }
          options.keepTtl = true
          i += 1
          break

        // GET - Return the previous value of the key before setting the new value
        case 'GET':
          options.get = true
          i += 1
          break

        // Unknown option - throw syntax error for any unrecognized option
        default:
          // Unrecognized option provided - Redis only supports specific SET options
          throw new RedisSyntaxError()
      }
    }

    // Validate incompatible options
    if (options.keepTtl && options.expiration !== undefined) {
      // KEEPTTL and expiration options are mutually exclusive (double-check validation)
      throw new RedisSyntaxError()
    }

    return options
  }
}

export default function (db: DB) {
  return new SetCommand(db)
}
