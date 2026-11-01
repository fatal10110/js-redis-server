import { createRequire } from 'node:module'

/**
 * node-redis' own command definitions, borrowed so the facade's typed command
 * methods build exactly the arguments and reply shapes the real client does
 * (#490). Each entry of `@redis/client`'s `COMMANDS` map (node-redis 5 and 6)
 * is `{ parseCommand(parser, ...args), transformReply }`: the real client runs
 * `parseCommand` on a fresh `BasicCommandParser`, sends `parser.redisArgs`,
 * then hands the decoded reply to the `transformReply` of its configured
 * protocol (`client/index.js` `#createCommand` / `_executeCommand`). The
 * facade does the same, with its in-memory pipeline in place of the socket.
 */

/** The slice of node-redis' `BasicCommandParser` the facade reads. */
export type NodeRedisCommandParser = {
  readonly redisArgs: (string | Buffer)[]
  readonly firstKey: string | Buffer | undefined
  readonly preserve: unknown
}

/** node-redis' `transformReply(reply, preserve, typeMapping)`. */
export type NodeRedisTransformReply = (
  reply: unknown,
  preserve: unknown,
  typeMapping: unknown,
) => unknown

/** One `COMMANDS` entry, as far as the facade uses it. */
export type NodeRedisCommandDefinition = {
  parseCommand(parser: NodeRedisCommandParser, ...args: unknown[]): void
  transformReply?:
    NodeRedisTransformReply | Partial<Record<2 | 3, NodeRedisTransformReply>>
  IS_READ_ONLY?: boolean
}

export type NodeRedisCommandSet = {
  /** `COMMANDS`, keyed by both spellings (`GET` and `get`, `HSET` and `hSet`). */
  commands: Readonly<Record<string, NodeRedisCommandDefinition>>
  /** `BasicCommandParser`. */
  Parser: new () => NodeRedisCommandParser
}

let resolved: { set: NodeRedisCommandSet | undefined } | undefined

/**
 * The installed node-redis' command set, loaded once, or `undefined` when there
 * is none the facade can drive: `redis` absent, or node-redis 4, whose commands
 * are `transformArguments` functions in another layout (it has no
 * `RESP_TYPES` either, which is how it is told apart). `redis` is an optional
 * peer and `@redis/client` its own dependency, so both are resolved from where
 * `redis` itself is installed.
 */
export function nodeRedisCommandSet(
  redis: Record<string, unknown> | undefined,
): NodeRedisCommandSet | undefined {
  resolved ??= { set: redis ? loadCommandSet(redis) : undefined }
  return resolved.set
}

function loadCommandSet(
  redis: Record<string, unknown>,
): NodeRedisCommandSet | undefined {
  if (redis.RESP_TYPES === undefined) {
    return undefined
  }
  try {
    const fromRedis = createRequire(createRequire(__filename).resolve('redis'))
    const commandsModule = fromRedis('@redis/client/dist/lib/commands') as {
      default?: Record<string, NodeRedisCommandDefinition>
    }
    const parserModule = fromRedis('@redis/client/dist/lib/client/parser') as {
      BasicCommandParser?: new () => NodeRedisCommandParser
    }
    const commands = commandsModule.default
    const Parser = parserModule.BasicCommandParser
    if (
      typeof Parser !== 'function' ||
      typeof commands?.GET?.parseCommand !== 'function'
    ) {
      return undefined
    }
    return { commands, Parser }
  } catch {
    return undefined
  }
}

/**
 * node-redis' `getTransformReply(command, resp)`: one function for both
 * protocols, or one per protocol (`undefined` there: the reply as decoded).
 */
export function transformReplyFor(
  command: NodeRedisCommandDefinition,
  resp: 2 | 3,
): NodeRedisTransformReply | undefined {
  const { transformReply } = command
  return typeof transformReply === 'function'
    ? transformReply
    : transformReply?.[resp]
}

/** Run `command.parseCommand` on a fresh parser, as node-redis does. */
export function parseNodeRedisCommand(
  set: NodeRedisCommandSet,
  command: NodeRedisCommandDefinition,
  args: unknown[],
): NodeRedisCommandParser {
  const parser = new set.Parser()
  command.parseCommand(parser, ...args)
  return parser
}
