import Redis from 'ioredis-mock'
import { ChainableCommander, Redis as RedisType } from 'ioredis'
import {
  Command,
  ClusterCommanderFactory,
  DBCommandExecutor,
  DiscoveryService,
  Logger,
  CommandResult,
  DiscoveryNode,
  Transport,
} from '../../types'
import createCluster from '../custom/commands/redis/cluster'
import createClient from '../custom/commands/redis/client'
import createQuit from '../custom/commands/redis/quit'
import clusterKeySlot from 'cluster-key-slot'
import {
  CorssSlot,
  MovedError,
  TransactionDiscardedWithError,
  WrongNumberOfArguments,
} from '../../core/errors'

/**
 * Transforms replies back to their original Redis format by reversing ioredis built-in transformers.
 *
 * ioredis has built-in reply transformers for some commands:
 * - HGETALL: Converts ['k1', 'v1', 'k2', 'v2'] to { k1: 'v1', k2: 'v2' }
 * - And argument transformers for HMSET, MSET that convert objects to flat arrays
 *
 * This function reverses these transformations to return the original Redis format.
 *
 * @param cmdStr - The command name in lowercase
 * @param response - The response from ioredis-mock
 * @returns The transformed response in original Redis format
 */
function transformReplyToOriginalFormat(
  cmdStr: string,
  response: unknown,
): unknown {
  switch (cmdStr) {
    case 'exec':
      // Convert ioredis format [[err?, res], ...] to [err|res, ...]
      // Each element in the array is either the error (if present) or the result
      if (Array.isArray(response)) {
        return response.map(([err, result]) => err || result)
      }
      return response

    case 'hgetall':
      // Convert object {key: value, key2: value2} back to array [key, value, key2, value2]
      // This reverses ioredis's built-in HGETALL reply transformer
      if (
        response &&
        typeof response === 'object' &&
        !Array.isArray(response) &&
        response !== null
      ) {
        const result: string[] = []
        for (const [key, value] of Object.entries(
          response as Record<string, unknown>,
        )) {
          result.push(key, String(value))
        }
        return result
      }
      return response

    case 'mget':
      // MGET returns array as-is, no transformation needed in ioredis
      return response

    case 'zrange':
    case 'zrevrange':
      // These commands with WITHSCORES return flat arrays in original Redis format
      // ioredis doesn't transform these by default, so we return as-is
      return response

    // Note: HMSET and MSET have argument transformers (not reply transformers)
    // so their responses don't need transformation back

    default:
      // No transformation needed for other commands
      return response
  }
}

class IORredisMockCommander implements DBCommandExecutor {
  protected transaction: undefined | ChainableCommander
  protected transactionWithErrors: boolean = false

  constructor(
    private readonly logger: Logger,
    private readonly redis: RedisType,
    private readonly commandOverrides?: Record<string, Command>,
  ) {}

  async execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    const cmdStr = rawCmd.toString().toLowerCase()
    const commander: RedisType | ChainableCommander =
      this.transaction || this.redis

    if (!(cmdStr in commander) && !this.commandOverrides?.[cmdStr]) {
      throw new Error(`Command ${rawCmd.toString().toLowerCase()} not found`)
    }

    if (cmdStr === 'multi' && this.transaction) {
      throw new Error('Transaction already started')
    } else if (cmdStr === 'multi') {
      this.transaction = (commander as unknown as RedisType).multi()
      transport.write(Buffer.from('OK'))
      return
    }

    if (cmdStr === 'exec' && !this.transaction) {
      throw new Error('Transaction not started')
    } else if (cmdStr === 'discard') {
      this.transaction = undefined
      this.transactionWithErrors = false
    } else if (cmdStr === 'exec') {
      this.transaction = undefined
      const hadErrors = this.transactionWithErrors
      this.transactionWithErrors = false

      if (hadErrors) {
        throw new TransactionDiscardedWithError()
      }
    }

    try {
      let command = this.commandOverrides?.[cmdStr]

      if (command) {
        const res = await command.run(rawCmd, args, signal)
        transport.write(res.response)
        return
      }

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      command = (commander as any)[cmdStr].bind(commander)

      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      let response = await command(...args.map(arg => arg.toString()))

      if (cmdStr === 'multi') {
        response = 'OK'
      } else if (response?.constructor.name === 'Pipeline') {
        response = 'QUEUED'
      }

      transport.write(transformReplyToOriginalFormat(cmdStr, response))

      return
    } catch (err) {
      if (this.transaction) {
        this.transactionWithErrors = true
      }

      transport.write(err)
      return
    }
  }

  shutdown(): Promise<void> {
    //this.logger.info('Shutting down IORredisMockCommander')
    return Promise.resolve()
  }
}

class IORredisMockClusterCommander extends IORredisMockCommander {
  private readonly me: DiscoveryNode
  private readonly clusterCommandsOverrides: Record<string, Command>
  private readonly discoveryService: DiscoveryService

  constructor(
    logger: Logger,
    redis: RedisType,
    private readonly mySelfId: string,
    discoveryService: DiscoveryService,
  ) {
    const clusterCommandsOverrides = {
      ...commadsOverrides,
      client: createClient(),
      cluster: createCluster(discoveryService, mySelfId),
      quit: createQuit(),
    }

    super(logger, redis, clusterCommandsOverrides)

    this.discoveryService = discoveryService
    this.me = this.discoveryService.getById(this.mySelfId)
    this.clusterCommandsOverrides = clusterCommandsOverrides
  }

  private getKeys(cmdStr: string, args: Buffer[]): Buffer[] {
    switch (cmdStr) {
      case 'mget':
      case 'exists':
      case 'touch':
      case 'unlink':
      case 'del':
        return args // All args are keys
      case 'mset':
      case 'msetnx':
        return args.filter((_, i) => i % 2 === 0) // Even indexed args are keys
      case 'zdiff':
      case 'zinter':
      case 'zunion':
      case 'zdiffstore':
      case 'zinterstore':
      case 'zunionstore':
        return args.slice(1, parseInt(args[0].toString()) + 1) // Number of keys specified in first arg
      case 'scan':
      case 'keys':
      case 'randomkey':
      case 'flushdb':
      case 'flushall':
      case 'multi':
      case 'exec':
      case 'discard':
        return [] // These commands operate on key patterns or entire DB, not specific keys
      case 'eval':
      case 'evalsha': {
        if (args.length < 2) {
          throw new WrongNumberOfArguments('evalsha')
        }

        const numKeys = parseInt(args[1].toString())
        return args.slice(2, 2 + numKeys) // Keys start after script and numkeys
      }
      case 'xread':
      case 'xreadgroup': {
        const streams = args.indexOf(Buffer.from('STREAMS'))
        if (streams === -1) return [args[0]]
        return args.slice(
          streams + 1,
          streams + 1 + Math.floor((args.length - streams - 1) / 2),
        )
      }
      default:
        return [args[0]] // Default to first arg as key
    }
  }

  execute(
    transport: Transport,
    rawCmd: Buffer,
    args: Buffer[],
    signal: AbortSignal,
  ): Promise<void> {
    const cmdName = rawCmd.toString().toLowerCase()
    const cmd = this.clusterCommandsOverrides?.[cmdName]

    let keys: Buffer[] = []

    try {
      if (cmd) {
        keys = cmd.getKeys(rawCmd, args)
      } else {
        keys = this.getKeys(cmdName, args)
      }
    } catch (err) {
      if (this.transaction) {
        this.transactionWithErrors = true
      }

      throw err
    }

    if (!keys.length) {
      return super.execute(transport, rawCmd, args, signal)
    }

    const slot = clusterKeySlot.generateMulti(keys)

    if (slot === -1) {
      throw new CorssSlot()
    }

    for (const [min, max] of this.me.slots) {
      if (slot >= min && slot <= max) {
        return super.execute(transport, rawCmd, args, signal)
      }
    }

    const clusterNode = this.discoveryService.getBySlot(slot)

    throw new MovedError(clusterNode.host, clusterNode.port, slot)
  }

  shutdown(): Promise<void> {
    return super.shutdown()
  }
}

const commadsOverrides: Record<string, Command> = {
  command: {
    getKeys: () => [],
    run: () => Promise.resolve({ close: false, response: 'mock response' }),
  },
  info: {
    getKeys(): Buffer[] {
      return []
    },
    run(): Promise<CommandResult> {
      return Promise.resolve({ response: 'mock info' })
    },
  },
}

export class IORedisMockCommanderFactory {
  private readonly db = new Redis()

  constructor(private readonly logger: Logger) {}

  shutdown(): Promise<void> {
    //this.logger.info('Shutting down IORedisMockCommanderFactory')
    return Promise.resolve()
  }

  createCommander(): DBCommandExecutor {
    return new IORredisMockCommander(this.logger, this.db, commadsOverrides)
  }
}

export class IORedisMockClusterCommanderFactory
  implements ClusterCommanderFactory
{
  private readonly dbs: Record<string, RedisType> = {}

  constructor(
    private readonly logger: Logger,
    private readonly discoveryService: DiscoveryService,
  ) {}

  createCommander(mySelfId: string): DBCommandExecutor {
    this.dbs[mySelfId] = this.dbs[mySelfId] || new Redis()

    return new IORredisMockClusterCommander(
      this.logger,
      this.dbs[mySelfId],
      mySelfId,
      this.discoveryService,
    )
  }

  createReadOnlyCommander(mySelfId: string): DBCommandExecutor {
    const { id } = this.discoveryService.getMaster(mySelfId)
    return this.createCommander(id) // TODO readonly commander
  }

  shutdown(): Promise<void> {
    //this.logger.info('Shutting down IORedisMockClusterCommanderFactory')
    return Promise.resolve()
  }
}
