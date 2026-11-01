import { EventEmitter } from 'node:events'
import { createRequire } from 'node:module'
import { createRedisCommandExecutor } from '../commands'
import { buildClusterNodes, type ClusterNodePipeline } from '../cluster'
import { ClientSession } from '../core/client-session'
import type { CommandExecutor } from '../core/command-executor'
import type { CompatibilityProfile } from '../core/compatibility'
import {
  decodeRedisMapEntries,
  decodeRedisValue,
  redisErrorText,
  type ClientDecodeOptions,
  type NativeRedisReply,
  type ReturnNestedErrors,
} from '../core/decode-redis-value'
import { RedisCommandError } from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import type { RedisValue } from '../core/redis-value'
import type { RespVersion } from '../core/resp-encoder'
import {
  REDIS_CLUSTER_SLOT_COUNT,
  RedisServerState,
  RedisClusterTopology,
} from '../state'
import type { NodeRedisCommandName } from './node-redis-command-names'
import {
  nodeRedisCommandSet,
  parseNodeRedisCommand,
  transformReplyFor,
  type NodeRedisCommandDefinition,
  type NodeRedisCommandSet,
  type NodeRedisTransformReply,
} from './node-redis-commands'

/**
 * In-memory drop-in for the `node-redis` (`redis` package) client.
 *
 * node-redis exposes no socket/connector hook (its `#socketFactory` is private),
 * so unlike the ioredis path we cannot drive the *real* client over a virtual
 * socket. Instead this is a **hand-written facade** (the plan's "Tier 2") that
 * replicates node-redis' public surface and routes every command through the
 * shared in-memory command pipeline via a per-connection {@link ClientSession} —
 * the exact same pipeline used by the networked server, MULTI/EXEC and Lua.
 *
 * Scope is deliberately honest: a curated set of camelCase methods carries
 * node-redis-correct return types, and EVERYTHING ELSE falls through to the
 * generic {@link NodeRedisMockClient.sendCommand} escape hatch, which decodes
 * replies to native JS via the shared {@link decodeReply}. This avoids the
 * `ioredis-mock` whack-a-mole trap of hand-shaping every command's reply.
 */

const DEFAULT_DATABASE_COUNT = 16

// node-redis throws its own error types (`instanceof WatchError` / `ErrorReply`
// is the documented user idiom), so when the `redis` package is present — it
// always is, since it's the thing being mocked — surface the real classes, and
// follow the installed version where their behavior differs. Resolved once, on
// first need, and cached. Local stand-ins cover what `redis` does not export.
type ErrorReplyConstructor = new (message: string) => Error
type RedisErrorConstructors = {
  WatchError: new (message?: string) => Error
  ErrorReply: ErrorReplyConstructor
  /**
   * The class a server `-ERR` reply decodes to. node-redis v5+ decodes it into
   * `SimpleError` (a subclass of `ErrorReply`); v4 has no `SimpleError` and
   * decodes it into `ErrorReply` itself, so that is the fallback.
   */
  SimpleError: ErrorReplyConstructor
  /**
   * What `exec()` throws when any queued command failed. `undefined` on a
   * `redis` that predates it (< 4.6.12): that `exec()` never throws for a
   * failed command — it resolves, with the error inline in the reply array.
   */
  MultiErrorReply:
    (new (replies: unknown[], errorIndexes: number[]) => Error) | undefined
  ClientClosedError: new () => Error
  DisconnectsClientError: new () => Error
}

// Local stand-ins, used when `redis` cannot be required at all (they then model
// v6, the version this facade tracks) or lacks one of the classes. They mirror
// node-redis' own classes: same message, same `name` ('Error' — none of the
// real classes assigns one), and the same `constructor.name`, which is what
// `util.inspect` and assertion libraries print. None is `instanceof` a class a
// user can import, so code that must work without `redis` matches `message`.
class WatchError extends Error {
  constructor(message = 'One (or more) of the watched keys has been changed') {
    super(message)
  }
}
class ErrorReply extends Error {}
class SimpleError extends ErrorReply {}
class MultiErrorReply extends ErrorReply {
  constructor(
    readonly replies: unknown[],
    readonly errorIndexes: number[],
  ) {
    super(
      `${errorIndexes.length} commands failed, see .replies and .errorIndexes for more information`,
    )
  }
  *errors(): IterableIterator<unknown> {
    for (const index of this.errorIndexes) {
      yield this.replies[index]
    }
  }
}
class ClientClosedError extends Error {
  constructor() {
    super('The client is closed')
  }
}
class DisconnectsClientError extends Error {
  constructor() {
    super('Disconnects client')
  }
}

const STAND_IN_REDIS_ERRORS: RedisErrorConstructors = {
  WatchError,
  ErrorReply,
  SimpleError,
  MultiErrorReply,
  ClientClosedError,
  DisconnectsClientError,
}

let resolvedRedisErrors: RedisErrorConstructors | undefined

/**
 * node-redis' own error classes, resolved on first need and cached.
 *
 * - **Lazy.** Importing this module must do no work (`"sideEffects": false`),
 *   and ioredis-only consumers import it too, so nothing here may load the
 *   optional `redis` peer at import time — only the first facade client
 *   constructed ({@link CommandRunner}'s constructor) or the first error
 *   decoded does.
 * - **Synchronous.** `destroy()` throws synchronously, so the close path cannot
 *   await an import. `redis` and `@redis/client` are CommonJS-only packages (no
 *   `exports` map), so `require` returns the very module instance an ESM
 *   `import … from 'redis'` sees — the classes are identical and `instanceof`
 *   holds for ESM and CJS consumers alike.
 * - **Resolved from this module's own location**, as a static import would be.
 * - **Per class.** Older `redis` releases lack some classes — `ErrorReply`
 *   arrived in redis 4.1.0, `MultiErrorReply` in 4.6.12 (@redis/client 1.5.13),
 *   `SimpleError` in v5 — and a missing one must not cost the ones that do
 *   exist: `instanceof ClientClosedError` has to keep holding on every version
 *   that exports it. A missing `SimpleError` becomes `ErrorReply` and a missing
 *   `MultiErrorReply` stays `undefined`, as that version behaves; any other
 *   missing class falls back to its stand-in alone.
 */
function redisErrors(): RedisErrorConstructors {
  resolvedRedisErrors ??= loadRedisErrors()
  return resolvedRedisErrors
}

function requireRedis(): Record<string, unknown> | undefined {
  try {
    return createRequire(__filename)('redis') as Record<string, unknown>
  } catch {
    return undefined
  }
}

function loadRedisErrors(): RedisErrorConstructors {
  const redis = requireRedis()
  if (!redis) {
    return STAND_IN_REDIS_ERRORS
  }
  const exported = <K extends keyof RedisErrorConstructors>(
    name: K,
  ): RedisErrorConstructors[K] | undefined => {
    const value = redis[name]
    return typeof value === 'function'
      ? (value as RedisErrorConstructors[K])
      : undefined
  }

  const errorReply = exported('ErrorReply') ?? ErrorReply
  return {
    WatchError: exported('WatchError') ?? WatchError,
    ErrorReply: errorReply,
    SimpleError: exported('SimpleError') ?? errorReply,
    MultiErrorReply: exported('MultiErrorReply'),
    ClientClosedError: exported('ClientClosedError') ?? ClientClosedError,
    DisconnectsClientError:
      exported('DisconnectsClientError') ?? DisconnectsClientError,
  }
}

/** A close-path error, synchronously — `destroy()` cannot await. */
function closeError(which: 'ClientClosedError' | 'DisconnectsClientError') {
  return new (redisErrors()[which])()
}

/** Thrown by any call on a closed client, a redundant close included. */
function clientClosedError(): Error {
  return closeError('ClientClosedError')
}

/**
 * Real node-redis' `destroy()` flushes the in-flight command queue with this
 * (`client/index.js`: `#queue.flushAll(new DisconnectsClientError())`), and
 * `disconnect()` is an alias for `destroy()`.
 */
function disconnectsClientError(): Error {
  return closeError('DisconnectsClientError')
}

let resolvedDefaultResp: NodeRedisRespVersion | undefined

/**
 * The protocol a node-redis client negotiates when given no `RESP` option,
 * resolved like the error classes above (lazily, from the installed `redis`,
 * cached). node-redis v6 exports `DEFAULT_RESP` (3) and opens every connection
 * with `HELLO 3`; v4 and v5 export no such constant and stay on RESP2 unless
 * asked (`@redis/client` 5.x: `this.#options.RESP ?? 2`). Without `redis`
 * at all the facade models v6, as its error stand-ins do.
 */
function nodeRedisDefaultResp(): NodeRedisRespVersion {
  resolvedDefaultResp ??= loadDefaultResp()
  return resolvedDefaultResp
}

function loadDefaultResp(): NodeRedisRespVersion {
  const redis = requireRedis()
  if (!redis) {
    return 3
  }
  const declared = redis.DEFAULT_RESP
  return declared === 2 || declared === 3 ? declared : 2
}

/** A command argument node-redis accepts on the wire. */
export type NodeRedisCommandArgument = string | Buffer

/**
 * Native JS value a reply decodes to. An error nested in an array or map is an
 * `Error` (node-redis' `SimpleError`) in place; a top-level one is thrown.
 */
export type NodeRedisReply = NativeRedisReply<Error>

export type NodeRedisMockClusterOptions = {
  masters: number
  replicas?: number
}

/** A protocol version node-redis' `RESP` client option accepts. */
export type NodeRedisRespVersion = 2 | 3

/** Client options shared by the standalone and the cluster facade. */
export type NodeRedisMockClientOptions = {
  /**
   * The protocol the client negotiates on connect, like node-redis' own `RESP`
   * option. Defaults to the installed node-redis' default: RESP3 on v6 (and
   * when `redis` is not installed), RESP2 on v4 and v5. `HELLO` still switches
   * it mid-connection.
   */
  RESP?: NodeRedisRespVersion
}

export type CreateNodeRedisMockOptions = NodeRedisMockClientOptions &
  (
    | { cluster?: undefined; databaseCount?: number }
    | { cluster: NodeRedisMockClusterOptions }
  )

/** node-redis' `ZRANGE` options (its `ZRangeOptions`). */
export type NodeRedisZRangeOptions = {
  BY?: 'SCORE' | 'LEX'
  REV?: boolean
  LIMIT?: { offset: number; count: number }
}

/** A single `{ score, value }` member for ZADD-style methods. */
export type NodeRedisZMember = { score: number; value: string }

/**
 * A cluster node as node-redis' `cluster.masters` / `cluster.slots` list it.
 * Real node-redis also hangs its connection (`client`, `connectPromise`) on
 * it; the facade has none to expose.
 */
export type NodeRedisMockClusterNode = {
  id: string
  host: string
  port: number
  /** `host:port`. */
  address: string
  readonly: boolean
}

/** One entry of node-redis' `cluster.slots`: the shard owning the slot. */
export type NodeRedisMockClusterShard = { master: NodeRedisMockClusterNode }

/** Listener invoked for each delivered pub/sub message: `(message, channel)`. */
export type NodeRedisPubSubListener = (message: string, channel: string) => void

/**
 * node-redis' variadic argument (`RedisVariadicArgument`): one value or an
 * array of them, sent in order. The facade's variadic methods also take them
 * as rest arguments (`sAdd(key, 'a', 'b')`), which real node-redis does not.
 */
export type NodeRedisVariadicArgument =
  NodeRedisCommandArgument | readonly NodeRedisCommandArgument[]

/** `set()` options, in node-redis' shape and emitted in its order. */
export type NodeRedisSetOptions = {
  expiration?:
    | 'KEEPTTL'
    | { type: 'KEEPTTL' }
    | { type: 'EX' | 'PX' | 'EXAT' | 'PXAT'; value: number }
  EX?: number
  PX?: number
  EXAT?: number
  PXAT?: number
  KEEPTTL?: boolean
  /** `'NX'`, `'XX'`, or a compare condition followed by {@link matchValue}. */
  condition?: string
  matchValue?: NodeRedisCommandArgument
  NX?: boolean
  XX?: boolean
  GET?: boolean
}

/** A hash field or value; numbers are sent as their decimal string. */
export type NodeRedisHashValue = NodeRedisCommandArgument | number

/** `hSet(key, fields)`'s multi-field forms: an object, a `Map`, or tuples / a flat list. */
export type NodeRedisHashFields =
  | Record<string, NodeRedisHashValue>
  | Map<NodeRedisHashValue, NodeRedisHashValue>
  | readonly (NodeRedisHashValue | readonly NodeRedisHashValue[])[]

/**
 * `mSet()`'s forms: `[key, value]` tuples, a flat `[k, v, k, v]` list, or an
 * object. Strings and Buffers only, as node-redis types them: a number is
 * passed through and rejected by the encoder, as node-redis does.
 */
export type NodeRedisMSetArguments =
  | readonly (readonly [
      key: NodeRedisCommandArgument,
      value: NodeRedisCommandArgument,
    ])[]
  | readonly NodeRedisCommandArgument[]
  | Record<string, NodeRedisCommandArgument>

/** `expire()`'s optional mode. */
export type NodeRedisExpireMode = 'NX' | 'XX' | 'GT' | 'LT'

/** `zAdd()` options, emitted as node-redis emits them. */
export type NodeRedisZAddOptions = {
  condition?: 'NX' | 'XX'
  NX?: boolean
  XX?: boolean
  comparison?: 'LT' | 'GT'
  LT?: boolean
  GT?: boolean
  CH?: boolean
}

/** A weighted source key for `zUnionStore()`. */
export type NodeRedisZKeyAndWeight = {
  key: NodeRedisCommandArgument
  weight: number
}

/** `zUnionStore()`'s source keys: plain or weighted, one or an array. */
export type NodeRedisZKeys =
  | NodeRedisVariadicArgument
  | NodeRedisZKeyAndWeight
  | readonly NodeRedisZKeyAndWeight[]

/**
 * Creates an in-memory node-redis-shaped client (standalone) or cluster client.
 * Pass `{ cluster: { masters } }` for a {@link NodeRedisMockCluster}; otherwise
 * a {@link NodeRedisMockClient} backed by a single in-memory pipeline.
 */
export async function createNodeRedisMock(
  options: CreateNodeRedisMockOptions = {},
): Promise<NodeRedisMockClient | NodeRedisMockCluster> {
  // node-redis' error classes are resolved by each client's constructor (see
  // redisErrors()), so every construction path — this factory or a direct
  // `new`/`create()` — gets the real WatchError/ErrorReply/ClientClosedError.
  if ('cluster' in options && options.cluster) {
    return NodeRedisMockCluster.create(options.cluster, { RESP: options.RESP })
  }
  const databaseCount =
    ('databaseCount' in options ? options.databaseCount : undefined) ??
    DEFAULT_DATABASE_COUNT
  const state = new RedisServerState({ databaseCount })
  const executor = createRedisCommandExecutor()
  return new NodeRedisMockClient({
    state,
    executor,
    ownsState: true,
    RESP: options.RESP,
  })
}

type FacadeBackend = {
  state: RedisServerState
  executor: CommandExecutor
}

/**
 * Shared command-method surface implemented by both the standalone client and
 * the cluster client. Standalone runs every command on its own session; the
 * cluster routes by slot to the owning node's session — but the curated method
 * bodies are identical, so they live in this base and dispatch through the
 * abstract {@link CommandRunner.run}.
 *
 * Extends `EventEmitter` because a real node-redis client is one: `on`/`once`/
 * `off` (and the rest of the emitter surface) come for free on both clients.
 */
abstract class CommandRunner extends EventEmitter {
  /**
   * Whether this client has been torn down. Shared by both clients, but they
   * act on it differently: the standalone one refuses every later call, while
   * a real `RedisCluster` closes idempotently (see
   * {@link NodeRedisMockCluster.quit}).
   */
  protected closed = false

  constructor() {
    super()
    // First need of node-redis' error classes: resolve them before this client
    // can run, decode or close anything. Not at import — see redisErrors().
    redisErrors()
    CommandRunner.installNodeRedisCommands()
  }

  private static nodeRedisCommandsInstalled = false

  /**
   * Give both facades and their `multi()` node-redis' own command methods —
   * every `COMMANDS` entry of the installed node-redis (5 or 6) a curated or
   * client method does not already define, in both spellings (`hDel`,
   * `HDEL`). Each runs `parseCommand` on node-redis' parser and the reply
   * through that command's `transformReply` for the client's configured
   * `RESP`, as `RedisClient#createCommand` does, so arguments and reply
   * shapes are node-redis' own.
   */
  private static installNodeRedisCommands(): void {
    if (CommandRunner.nodeRedisCommandsInstalled) {
      return
    }
    CommandRunner.nodeRedisCommandsInstalled = true
    const set = nodeRedisCommandSet(requireRedis())
    if (!set) {
      return
    }
    for (const [name, command] of Object.entries(set.commands)) {
      if (name in CommandRunner.prototype) {
        continue
      }
      Object.defineProperty(CommandRunner.prototype, name, {
        configurable: true,
        writable: true,
        value: async function (this: CommandRunner, ...args: unknown[]) {
          return this.runNodeRedisCommand(set, command, args)
        },
      })
    }
  }

  private async runNodeRedisCommand(
    set: NodeRedisCommandSet,
    command: NodeRedisCommandDefinition,
    args: unknown[],
  ): Promise<unknown> {
    const parser = parseNodeRedisCommand(set, command, args)
    const transform = transformReplyFor(command, this.nodeRedisResp)
    const reply = this.decode(await this.run(parser.redisArgs, parser.firstKey))
    return transform ? transform(reply, parser.preserve, undefined) : reply
  }

  /**
   * Execute one already-tokenised command and return its decoded reply.
   * Implementations pick the session (standalone: the only one; cluster: the
   * slot owner for the command's keys).
   */
  protected abstract run(
    args: NodeRedisCommandArgument[],
    firstKey?: NodeRedisCommandArgument,
  ): Promise<RedisValue>

  /**
   * The `RESP` the client was configured with, which picks each command's
   * `transformReply` — node-redis binds it when it builds the client, so a
   * later `HELLO` changes how replies decode, not which transform runs.
   */
  protected abstract get nodeRedisResp(): NodeRedisRespVersion

  /**
   * RESP version this client's replies decode under — what every
   * protocol-dependent shape keys off (map, map-pairs, flat-pairs, double,
   * big-number, boolean). `HELLO` switches it mid-connection, so it is read per
   * reply rather than fixed at construction.
   */
  protected abstract get respVersion(): RespVersion

  /** The served profile, which spells a RESP2 `double` (#451). */
  protected abstract get profile(): CompatibilityProfile

  /** Decode a reply the way the generic `sendCommand()` escape hatch does. */
  protected decode(value: RedisValue): NodeRedisReply {
    return decodeReply(value, this.respVersion, this.profile)
  }

  /** Whether the client is open: node-redis' `isOpen` getter. */
  get isOpen(): boolean {
    return !this.closed
  }

  // --- strings -------------------------------------------------------------

  async get(key: string): Promise<string | null> {
    return asStringOrNull(await this.run(['GET', key]))
  }

  /**
   * SET with node-redis' `options` object (`{ expiration: { type: 'EX',
   * value } }`, the legacy `EX` / `PX` / … / `KEEPTTL` keys, `condition` /
   * `NX` / `XX`, `GET`), emitted in node-redis' order. Raw trailing arguments
   * (`set(key, value, 'EX', '10')`) are still appended as given.
   */
  set(
    key: NodeRedisCommandArgument,
    value: NodeRedisCommandArgument | number,
    options?: NodeRedisSetOptions,
  ): Promise<string | null>
  set(
    key: NodeRedisCommandArgument,
    value: NodeRedisCommandArgument | number,
    ...rest: NodeRedisCommandArgument[]
  ): Promise<string | null>
  async set(
    key: NodeRedisCommandArgument,
    value: NodeRedisCommandArgument | number,
    ...rest: (NodeRedisCommandArgument | NodeRedisSetOptions | undefined)[]
  ): Promise<string | null> {
    return asStringOrNull(await this.run(setArguments(key, value, rest)))
  }

  async del(...keys: NodeRedisVariadicArgument[]): Promise<number> {
    return asNumber(await this.run(['DEL', ...variadicArguments(keys)]))
  }

  /**
   * MSET from `[key, value]` tuples, a flat `[k, v, k, v]` list or an object,
   * as node-redis takes it (an empty list throws `empty toSet Argument`).
   */
  async mSet(toSet: NodeRedisMSetArguments): Promise<string> {
    return asString(await this.run(['MSET', ...mSetArguments(toSet)]))
  }

  async exists(...keys: NodeRedisVariadicArgument[]): Promise<number> {
    return asNumber(await this.run(['EXISTS', ...variadicArguments(keys)]))
  }

  async incr(key: string): Promise<number> {
    return asNumber(await this.run(['INCR', key]))
  }

  async expire(
    key: NodeRedisCommandArgument,
    seconds: number,
    mode?: NodeRedisExpireMode,
  ): Promise<number> {
    // node-redis' EXPIRE has no transformReply — it passes the raw 0/1 integer
    // through as a number (not a boolean).
    const args = ['EXPIRE', key, String(seconds)]
    if (mode) {
      args.push(mode)
    }
    return asNumber(await this.run(args))
  }

  async ttl(key: string): Promise<number> {
    return asNumber(await this.run(['TTL', key]))
  }

  // --- hashes --------------------------------------------------------------

  /**
   * HSET of one `field, value` pair, or of many as node-redis takes them: an
   * object, a `Map`, or `[field, value]` tuples / a flat list.
   */
  hSet(
    key: NodeRedisCommandArgument,
    field: NodeRedisHashValue,
    value: NodeRedisHashValue,
  ): Promise<number>
  hSet(
    key: NodeRedisCommandArgument,
    fields: NodeRedisHashFields,
  ): Promise<number>
  async hSet(
    key: NodeRedisCommandArgument,
    fields: NodeRedisHashValue | NodeRedisHashFields,
    value?: NodeRedisHashValue,
  ): Promise<number> {
    return asNumber(
      await this.run(['HSET', key, ...hashFieldArguments(fields, value)]),
    )
  }

  async hGet(key: string, field: string): Promise<string | null> {
    return asStringOrNull(await this.run(['HGET', key, field]))
  }

  async hGetAll(key: string): Promise<{ [field: string]: string }> {
    const value = await this.run(['HGETALL', key])
    // Deliberately *not* `decodeReply`: node-redis' own `hGetAll`
    // transformReply assembles the object itself, from the RESP2 flat array as
    // readily as from the RESP3 map, so the curated method is an object on both
    // protocols where the raw `sendCommand` path follows the protocol (#414).
    return decodeMapReply(value, this.respVersion) as {
      [field: string]: string
    }
  }

  // --- lists ---------------------------------------------------------------

  async lPush(
    key: NodeRedisCommandArgument,
    ...elements: NodeRedisVariadicArgument[]
  ): Promise<number> {
    return asNumber(
      await this.run(['LPUSH', key, ...variadicArguments(elements)]),
    )
  }

  async rPush(
    key: NodeRedisCommandArgument,
    ...elements: NodeRedisVariadicArgument[]
  ): Promise<number> {
    return asNumber(
      await this.run(['RPUSH', key, ...variadicArguments(elements)]),
    )
  }

  async lRange(key: string, start: number, stop: number): Promise<string[]> {
    return asStringArray(
      await this.run(['LRANGE', key, String(start), String(stop)]),
    )
  }

  // --- sets ----------------------------------------------------------------

  async sAdd(
    key: NodeRedisCommandArgument,
    ...members: NodeRedisVariadicArgument[]
  ): Promise<number> {
    return asNumber(
      await this.run(['SADD', key, ...variadicArguments(members)]),
    )
  }

  async sMembers(key: string): Promise<string[]> {
    return asStringArray(await this.run(['SMEMBERS', key]))
  }

  // --- sorted sets ---------------------------------------------------------

  /**
   * ZADD with node-redis' options (`condition` / `NX` / `XX`, `comparison` /
   * `LT` / `GT`, `CH`) ahead of the members; an infinite score goes out as
   * `+inf` / `-inf`.
   */
  async zAdd(
    key: NodeRedisCommandArgument,
    members: NodeRedisZMember | readonly NodeRedisZMember[],
    options?: NodeRedisZAddOptions,
  ): Promise<number> {
    const list: readonly NodeRedisZMember[] = Array.isArray(members)
      ? members
      : [members as NodeRedisZMember]
    const args: NodeRedisCommandArgument[] = [
      'ZADD',
      key,
      ...zAddOptionArguments(options),
    ]
    for (const { score, value } of list) {
      args.push(doubleArgument(score), value)
    }
    return asNumber(await this.run(args))
  }

  /**
   * ZRANGE with node-redis' signature: `min` / `max` are indexes, scores or
   * lex bounds depending on `BY`, and the options are appended exactly as
   * node-redis' `zRangeArgument` appends them — `BY` (only `'SCORE'` / `'LEX'`
   * add anything), then `REV`, then `LIMIT offset count` — so the server sees
   * the argument list a real client sends and answers, or rejects, it the
   * same way.
   */
  async zRange(
    key: NodeRedisCommandArgument,
    min: NodeRedisCommandArgument | number,
    max: NodeRedisCommandArgument | number,
    options?: NodeRedisZRangeOptions,
  ): Promise<string[]> {
    return asStringArray(
      await this.run(['ZRANGE', key, ...zRangeArguments(min, max, options)]),
    )
  }

  /**
   * {@link zRange} plus `WITHSCORES`, as `{ value, score }` members. node-redis'
   * `transformSortedSetReply` builds the same objects at both protocols — it
   * parses the RESP2 score string like `transformDoubleReply` (`inf` →
   * `Infinity`) and takes the RESP3 double as is — so the method is
   * protocol-independent.
   */
  async zRangeWithScores(
    key: NodeRedisCommandArgument,
    min: NodeRedisCommandArgument | number,
    max: NodeRedisCommandArgument | number,
    options?: NodeRedisZRangeOptions,
  ): Promise<NodeRedisZMember[]> {
    return decodeSortedSetReply(
      await this.run([
        'ZRANGE',
        key,
        ...zRangeArguments(min, max, options),
        'WITHSCORES',
      ]),
    )
  }

  /**
   * ZUNIONSTORE with node-redis' key forms — one key or an array, plain or
   * `{ key, weight }` (which adds `WEIGHTS`) — and `{ AGGREGATE }`.
   */
  async zUnionStore(
    destination: NodeRedisCommandArgument,
    keys: NodeRedisZKeys,
    options?: { AGGREGATE?: 'SUM' | 'MIN' | 'MAX' },
  ): Promise<number> {
    const args = ['ZUNIONSTORE', destination, ...zKeysArguments(keys)]
    if (options?.AGGREGATE) {
      args.push('AGGREGATE', options.AGGREGATE)
    }
    return asNumber(await this.run(args))
  }

  // --- scripting -----------------------------------------------------------

  /**
   * EVAL with node-redis' options shape. The declared `keys` — not the script
   * text — are the command's routing keys, which is exactly what the executor's
   * plan reports.
   */
  async eval(
    script: string,
    options: { keys?: string[]; arguments?: string[] } = {},
  ): Promise<NodeRedisReply> {
    const keys = options.keys ?? []
    const args = options.arguments ?? []
    const value = await this.run([
      'EVAL',
      script,
      String(keys.length),
      ...keys,
      ...args,
    ])
    return this.decode(value)
  }
}

/**
 * The node-redis command methods a facade takes from the installed node-redis
 * (see `CommandRunner.installNodeRedisCommands`), minus the `Own` names it
 * defines itself. Typed loosely, since node-redis' own per-command signatures
 * live in `redis`, an optional peer: cast the facade to `RedisClientType` /
 * `RedisClusterType` for those, as the integration tests do. At runtime a
 * method exists only when the installed node-redis (5 or 6) defines it.
 */
export type NodeRedisCommandMethods<Own extends PropertyKey, R> = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- node-redis' signatures, see above
  [K in Exclude<NodeRedisCommandName, Own>]: (...args: any[]) => R
}

/** The curated command methods both facades define themselves. */
type CuratedCommandName = keyof CommandRunner

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- node-redis' replies, see NodeRedisCommandMethods
type NodeRedisReplyPromise = Promise<any>

// The generated methods, merged into each facade class (declaration merging
// is how a class types members it installs at runtime).
/* eslint-disable @typescript-eslint/no-unsafe-declaration-merging, @typescript-eslint/no-empty-object-type */
export interface NodeRedisMockClient extends NodeRedisCommandMethods<
  CuratedCommandName | 'publish',
  NodeRedisReplyPromise
> {}
export interface NodeRedisMockCluster extends NodeRedisCommandMethods<
  CuratedCommandName,
  NodeRedisReplyPromise
> {}
export interface NodeRedisMockMulti extends NodeRedisCommandMethods<
  'get' | 'set' | 'del' | 'incr' | 'hSet' | 'hGet',
  NodeRedisMockMulti
> {}
/* eslint-enable @typescript-eslint/no-unsafe-declaration-merging, @typescript-eslint/no-empty-object-type */

export type NodeRedisMockClientInit = FacadeBackend &
  NodeRedisMockClientOptions & {
    database?: number
    /** True only for the client that created the state — it owns its teardown. */
    ownsState?: boolean
  }

/**
 * Standalone in-memory node-redis facade. Drives one {@link ClientSession}
 * against a single keyspace; pub/sub uses a *dedicated* session that drains
 * pushes off {@link ClientSession.readPushes}, mirroring node-redis' rule that a
 * subscribed connection is reserved for pub/sub.
 */
// eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging -- see NodeRedisCommandMethods
export class NodeRedisMockClient extends CommandRunner {
  private readonly backend: FacadeBackend
  /** The configured `database` — what `duplicate()` copies. */
  private readonly database?: number
  /**
   * node-redis' `#selectedDB`: the configured database until `select()` (or a
   * transaction's `select()`) moves it, and what a re-opened connection
   * selects again.
   */
  private selectedDatabase?: number
  private readonly ownsState: boolean
  /** The `RESP` this client was configured with — what `duplicate()` copies. */
  private readonly resp: NodeRedisRespVersion
  /** Replaced by a fresh one each time {@link connect} re-opens the client. */
  private session: ClientSession
  /**
   * Serializes commands on this client the way a real single node-redis
   * connection does, so a concurrent (un-awaited) call cannot interleave between
   * the MULTI and EXEC of a transaction sharing {@link session}.
   */
  private commandLock: Promise<unknown> = Promise.resolve()
  /** Dedicated session + push-reader loop, created lazily on first subscribe. */
  private pubsub?: {
    session: ClientSession
    abort: AbortController
    /** The push-draining loop; awaited on teardown so nothing dangles. */
    drained: Promise<void>
  }
  /**
   * The registered pub/sub listeners, per channel and per pattern. They belong
   * to the client, not to a connection: like node-redis' `PubSub.listeners`
   * they survive `quit()` / `destroy()` / `disconnect()`, and {@link connect}
   * re-subscribes them on the new connection before `'ready'` (#600).
   */
  private readonly channelListeners = new Map<
    string,
    Set<NodeRedisPubSubListener>
  >()
  private readonly patternListeners = new Map<
    string,
    Set<NodeRedisPubSubListener>
  >()
  /**
   * Set synchronously by `destroy()` (and so `disconnect()`), which — unlike
   * `quit()` — flush instead of draining. Every unit of work not yet answered,
   * queued or already executing, rejects with this (see {@link flushable}).
   */
  private flushPendingWith?: () => Error
  /**
   * Present exactly while a graceful `quit()` is draining. This is the
   * "closing" state, as distinct from closed: new work is refused, but a
   * forced close may still escalate — it calls this to reject the quit().
   */
  private rejectPendingQuit?: (err: Error) => void
  /** Guards {@link teardown}; see its note on why {@link closed} cannot. */
  private tornDown = false
  /** Settles once the last {@link teardown} has released everything. Never rejects. */
  private teardownDone: Promise<void> = Promise.resolve()
  /** Settles once the last graceful {@link quit} (`'end'` included) is over. Never rejects. */
  private quitDone: Promise<void> = Promise.resolve()
  /** Present while a {@link connect} is re-opening the client. */
  private reopening?: Promise<this>
  /** Pub/sub calls made while closed wait on it; see {@link untilReopened}. */
  private reopened?: { promise: Promise<void>; resolve: () => void }
  /**
   * Bumped by every `quit()` / `destroy()`, so a re-open that a close
   * overtook (closed again before its turn came) knows to give up.
   */
  private closeCount = 0
  /**
   * node-redis' `socketEpoch`: bumped on each (re)connect. {@link watch}
   * records the epoch it ran in, and a transaction executed after a reconnect
   * throws `WatchError('Client reconnected after WATCH')`, since the server
   * forgot the WATCH with the old connection.
   */
  private socketEpoch = 1
  private watchEpoch?: number

  constructor(init: NodeRedisMockClientInit) {
    super()
    this.backend = { state: init.state, executor: init.executor }
    this.database = init.database
    this.selectedDatabase = init.database
    this.ownsState = init.ownsState ?? false
    this.resp = init.RESP ?? nodeRedisDefaultResp()
    this.session = this.openSession()
    // node-redis emits 'connect' then 'ready' once the handshake completes.
    queueMicrotask(() => {
      this.emit('connect')
      this.emit('ready')
    })
  }

  /**
   * node-redis clients require an explicit connect(); on an open client it is a
   * no-op here (real node-redis rejects `Socket already opened`).
   *
   * On a closed client it re-opens, as real node-redis does (#440): a fresh
   * connection to the same server — the keyspace is untouched, the selected
   * database and protocol are the configured ones — then `'connect'` and
   * `'ready'`, and the next close emits `'end'` again. Like the real client the
   * client counts as open as soon as connect() is called, so commands issued
   * right after it (without awaiting) run on the new connection. Called while a
   * `quit()` is still draining, it waits for that close to finish first.
   *
   * Like node-redis' `RedisCommandsQueue.resubscribe()`, the channels and
   * patterns that still have listeners are subscribed again on the new
   * connection — one `SUBSCRIBE` and one `PSUBSCRIBE` — before `'ready'` (#600).
   */
  async connect(): Promise<this> {
    if (!this.closed) {
      return this.reopening ?? this
    }
    this.closed = false
    const closeCount = this.closeCount
    const closing = Promise.all([this.quitDone, this.teardownDone])
    const reopening = this.runExclusive(async () => {
      await closing
      if (this.closeCount !== closeCount) {
        // Closed again before the re-open got its turn.
        throw clientClosedError()
      }
      await this.reopen()
      if (this.closeCount !== closeCount) {
        // Closed while the re-subscribe ran.
        throw clientClosedError()
      }
      return this
    })
    this.reopening = reopening
    try {
      return await reopening
    } finally {
      if (this.reopening === reopening) {
        this.reopening = undefined
      }
    }
  }

  /**
   * The connect-time handshake: a fresh session, `'connect'`, the re-subscribe
   * of every channel and pattern that still has a listener, then `'ready'`.
   */
  private async reopen(): Promise<void> {
    if (this.ownsState) {
      this.backend.state.reopen()
    }
    this.session = this.openSession()
    this.tornDown = false
    this.flushPendingWith = undefined
    this.socketEpoch += 1
    this.emit('connect')
    await this.resubscribe()
    if (!this.closed) {
      this.emit('ready')
      const waiting = this.reopened
      this.reopened = undefined
      waiting?.resolve()
    }
  }

  /**
   * Settles once the next {@link connect} has re-opened the client. node-redis
   * queues a pub/sub command issued on a closed client (no `ClientClosedError`,
   * unlike every other command) and writes it after the re-subscribe, so
   * {@link subscribeTo} / {@link unsubscribeFrom} wait on this, then run (#490).
   * Nothing holds the event loop: never re-opened, they just stay pending.
   */
  private untilReopened(): Promise<void> {
    if (!this.reopened) {
      let resolve!: () => void
      const promise = new Promise<void>(done => {
        resolve = done
      })
      this.reopened = { promise, resolve }
    }
    return this.reopened.promise
  }

  /** node-redis' `PubSub.resubscribe()`: one command per listener kind. */
  private async resubscribe(): Promise<void> {
    const kinds = [
      ['SUBSCRIBE', this.channelListeners],
      ['PSUBSCRIBE', this.patternListeners],
    ] as const
    for (const [command, listeners] of kinds) {
      if (listeners.size === 0) {
        continue
      }
      const pubsub = this.ensurePubSub()
      await runOnSession(
        pubsub.session,
        [command, ...listeners.keys()],
        this.closed,
      )
    }
  }

  private openSession(): ClientSession {
    const session = new ClientSession({
      server: this.backend.state,
      executor: this.backend.executor,
      database: this.selectedDatabase,
    })
    // node-redis opens a RESP3 client with `HELLO 3` before anything else, and
    // a RESP2 one with no HELLO at all.
    session.setProtocolVersion(this.resp)
    return session
  }

  /**
   * A fresh, independent client over the **same** shared keyspace. Like
   * node-redis' `duplicate(overrides)`, it is returned synchronously and takes
   * this client's options — so its configured `RESP`, not whatever a `HELLO`
   * has since switched this connection to — unless `overrides` says otherwise.
   *
   * Real node-redis returns it unconnected (`isOpen` false until `connect()`);
   * the facade's is already open, and its `connect()` resolves at once, so
   * both `duplicate()` + `await connect()` and using it directly work.
   */
  duplicate(overrides: NodeRedisMockClientOptions = {}): NodeRedisMockClient {
    return new NodeRedisMockClient({
      ...this.backend,
      database: this.database,
      // node-redis spreads `overrides` over its options, so an explicit
      // `RESP: undefined` drops the option and falls back to DEFAULT_RESP.
      RESP: 'RESP' in overrides ? overrides.RESP : this.resp,
    })
  }

  /** Generic escape hatch for any command, decoded to a native JS reply. */
  async sendCommand(args: NodeRedisCommandArgument[]): Promise<NodeRedisReply> {
    return this.decode(await this.run(args))
  }

  /** node-redis' `isReady`: open, with the connect handshake done. */
  get isReady(): boolean {
    return !this.closed && this.reopening === undefined
  }

  protected get respVersion(): RespVersion {
    return this.session.protocolVersion
  }

  protected get nodeRedisResp(): NodeRedisRespVersion {
    return this.resp
  }

  /**
   * node-redis' `select(db)`, a client method rather than a `COMMANDS` entry:
   * it resolves `undefined` (not `'OK'`) and remembers the database, so a
   * re-opened connection selects it again (`client/index.js` `SELECT`). A
   * `duplicate()` still takes the configured `database`, as node-redis'.
   */
  async select(db: number): Promise<void> {
    await this.run(['SELECT', String(db)])
    this.selectedDatabase = db
  }

  protected get profile(): CompatibilityProfile {
    return this.backend.state.profile
  }

  protected run(args: NodeRedisCommandArgument[]): Promise<RedisValue> {
    // Open/closed is decided at *issue* time, the way a real client decides it
    // when it writes the command to the socket. A command issued before a close
    // is already in the queue and must not be retroactively killed by a close
    // that happens while it waits its turn — `quit()` drains those, and
    // `disconnect()`/`destroy()` flush them via `flushPendingWith`.
    this.assertOpen()
    return this.runExclusive(() =>
      this.flushable(() =>
        this.followingProtocol(() => runOnSession(this.session, args, false)),
      ),
    )
  }

  /**
   * Run `work` on the command session and, if it moved that session's
   * protocol (a `HELLO` or a `RESET`), carry the new version over to a live
   * pub/sub session at once. A RESP3 node-redis client subscribes on its one
   * connection, so a `HELLO 2` sent while subscribed re-shapes that
   * connection too: real Redis then lists the subscriber at `resp=2`.
   */
  private async followingProtocol<T>(work: () => Promise<T>): Promise<T> {
    const before = this.session.protocolVersion
    try {
      return await work()
    } finally {
      if (this.pubsub && this.session.protocolVersion !== before) {
        this.syncPubSubProtocol(this.pubsub)
      }
    }
  }

  /**
   * Run one queued unit of work subject to a hard close. Real node-redis'
   * `#queue.flushAll(new DisconnectsClientError())` rejects every command it
   * has not yet answered — still queued, *or already on the wire* — so a unit
   * that has not started, or is still running when the flush lands, rejects
   * with the flush error whatever it would otherwise have produced. That also
   * covers a parked `BLPOP 0`: teardown's `session.close()` unblocks it, and
   * the internal abort must not leak out as a bare `AbortError`.
   */
  private async flushable<T>(work: () => Promise<T>): Promise<T> {
    this.throwIfFlushed()
    let result: T
    try {
      result = await work()
    } catch (err) {
      this.throwIfFlushed()
      throw err
    }
    this.throwIfFlushed()
    return result
  }

  private throwIfFlushed(): void {
    const flush = this.flushPendingWith
    if (flush) {
      throw flush()
    }
  }

  /** Run `fn` after any in-flight command/transaction on this client settles. */
  private runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.commandLock.then(fn, fn)
    this.commandLock = result.then(
      () => undefined,
      () => undefined,
    )
    return result
  }

  // --- pub/sub -------------------------------------------------------------

  async publish(channel: string, message: string): Promise<number> {
    return asNumber(await this.run(['PUBLISH', channel, message]))
  }

  /**
   * Subscribe `listener` to one channel or an array of them. As in node-redis,
   * one `SUBSCRIBE` carries the channels not yet subscribed, and a listener
   * added to already-subscribed channels sends no command.
   */
  subscribe(
    channels: NodeRedisVariadicArgument,
    listener: NodeRedisPubSubListener,
  ): Promise<void> {
    return this.subscribeTo(
      'SUBSCRIBE',
      this.channelListeners,
      channels,
      listener,
    )
  }

  pSubscribe(
    patterns: NodeRedisVariadicArgument,
    listener: NodeRedisPubSubListener,
  ): Promise<void> {
    return this.subscribeTo(
      'PSUBSCRIBE',
      this.patternListeners,
      patterns,
      listener,
    )
  }

  /**
   * node-redis' `unsubscribe(channels?, listener?)`: no channels drops every
   * channel; channels without a listener drop all of their listeners; with a
   * listener only that one goes, and only channels left without listeners are
   * unsubscribed (no command at all when every channel keeps one).
   */
  unsubscribe(
    channels?: NodeRedisVariadicArgument,
    listener?: NodeRedisPubSubListener,
  ): Promise<void> {
    return this.unsubscribeFrom(
      'UNSUBSCRIBE',
      this.channelListeners,
      channels,
      listener,
    )
  }

  pUnsubscribe(
    patterns?: NodeRedisVariadicArgument,
    listener?: NodeRedisPubSubListener,
  ): Promise<void> {
    return this.unsubscribeFrom(
      'PUNSUBSCRIBE',
      this.patternListeners,
      patterns,
      listener,
    )
  }

  private async subscribeTo(
    command: 'SUBSCRIBE' | 'PSUBSCRIBE',
    listeners: Map<string, Set<NodeRedisPubSubListener>>,
    channels: NodeRedisVariadicArgument,
    listener: NodeRedisPubSubListener,
  ): Promise<void> {
    const names = variadicArguments([channels]).map(pubSubName)
    const fresh = names.filter(name => !listeners.has(name))
    if (fresh.length > 0 && this.closed) {
      await this.untilReopened()
      return this.subscribeTo(command, listeners, channels, listener)
    }
    // Registered before the command runs, so a message published right after
    // the subscribe lands reaches it; undone if the subscribe fails.
    const added = names.filter(name => addListener(listeners, name, listener))
    if (fresh.length === 0) {
      return
    }
    try {
      const pubsub = this.ensurePubSub()
      this.syncPubSubProtocol(pubsub)
      await runOnSession(pubsub.session, [command, ...fresh], this.closed)
    } catch (err) {
      for (const name of added) {
        removeListener(listeners, name, listener)
      }
      throw err
    }
  }

  private async unsubscribeFrom(
    command: 'UNSUBSCRIBE' | 'PUNSUBSCRIBE',
    listeners: Map<string, Set<NodeRedisPubSubListener>>,
    channels: NodeRedisVariadicArgument | undefined,
    listener: NodeRedisPubSubListener | undefined,
  ): Promise<void> {
    const names =
      channels === undefined
        ? undefined
        : variadicArguments([channels]).map(pubSubName)
    let args: string[]
    let release: () => void
    if (names === undefined) {
      args = [command]
      release = () => listeners.clear()
    } else if (listener === undefined) {
      args = [command, ...names]
      release = () => names.forEach(name => listeners.delete(name))
    } else {
      args = [command]
      for (const name of names) {
        const set = listeners.get(name)
        if (set && [...set].some(other => other !== listener)) {
          continue
        }
        args.push(name)
      }
      release = () =>
        names.forEach(name => removeListener(listeners, name, listener))
      if (args.length === 1) {
        // Every channel keeps another listener: no command, as node-redis.
        release()
        return
      }
    }
    if (this.closed) {
      await this.untilReopened()
      return this.unsubscribeFrom(command, listeners, channels, listener)
    }
    if (!this.pubsub) {
      // Never subscribed on this connection: nothing to tell the server.
      release()
      return
    }
    this.syncPubSubProtocol(this.pubsub)
    await runOnSession(this.pubsub.session, args, this.closed)
    release()
  }

  // --- transactions --------------------------------------------------------

  async watch(...keys: NodeRedisVariadicArgument[]): Promise<string> {
    const reply = asString(
      await this.run(['WATCH', ...variadicArguments(keys)]),
    )
    this.watchEpoch ??= this.socketEpoch
    return reply
  }

  async unwatch(): Promise<string> {
    const reply = asString(await this.run(['UNWATCH']))
    this.watchEpoch = undefined
    return reply
  }

  /** Begin a MULTI transaction. Commands are queued, then replayed on exec(). */
  multi(): NodeRedisMockMulti {
    return new NodeRedisMockMulti((queued, selectedDatabase) => {
      // node-redis' `_executeMulti`: a transaction consumes the WATCH, and one
      // set before a reconnect cannot hold — the server dropped it.
      const watchEpoch = this.watchEpoch
      this.watchEpoch = undefined
      // Same issue-time gate as run(): a transaction handed to exec() before a
      // close drains with the rest of the queue.
      this.assertOpen()
      if (watchEpoch !== undefined && watchEpoch !== this.socketEpoch) {
        throw new (redisErrors().WatchError)('Client reconnected after WATCH')
      }
      return this.runExclusive(() =>
        this.flushable(() =>
          this.followingProtocol(async () => {
            const span = await runTransactionSpan(this.session, queued, false)
            // node-redis' `_executeMulti` adopts a queued `select()` once
            // EXEC has answered with the replies.
            if (
              selectedDatabase !== undefined &&
              span.encodeError === undefined &&
              (span.value.kind === 'array' || span.value.kind === 'set')
            ) {
              this.selectedDatabase = selectedDatabase
            }
            return span
          }),
        ),
      )
    }, this.resp)
  }

  // --- lifecycle -----------------------------------------------------------

  /**
   * Real node-redis rejects new work on a client that is closed *or closing*
   * (a `quit()` still draining) with `ClientClosedError` — so defensive
   * teardown that closes twice sees the same error here, and 'end' stays a
   * once-per-close signal.
   */
  private assertOpen(): void {
    if (this.closed) {
      throw clientClosedError()
    }
  }

  /**
   * node-redis 5+'s graceful close: `quit()`'s draining, resolving
   * `undefined` rather than `'OK'`.
   */
  async close(): Promise<void> {
    await this.quit()
  }

  /**
   * Graceful close. Real node-redis' `quit()` appends `QUIT` to the command
   * queue, so every command already issued runs to completion first — 2000
   * pending commands all resolve. Queueing the teardown through the same
   * {@link runExclusive} lock reproduces that: the lock is FIFO, so by the time
   * teardown runs, nothing issued before the `quit()` is left.
   *
   * While it drains the client is *closing*: new work is refused, but a forced
   * {@link destroy}/{@link disconnect} can still escalate and reject this.
   */
  async quit(): Promise<string> {
    this.assertOpen()
    // Claim the closed flag synchronously so commands issued *after* this point
    // are refused (assertOpen in run()) while the ones before it still drain.
    this.closed = true
    this.closeCount += 1
    const forced = new Promise<never>((_, reject) => {
      this.rejectPendingQuit = reject
    })
    const closing = (async () => {
      try {
        // A drain can block indefinitely (a BLPOP 0 ahead of it), so it races
        // the escalation rather than waiting on it.
        await Promise.race([this.runExclusive(async () => undefined), forced])
      } finally {
        this.rejectPendingQuit = undefined
      }
      await this.teardown()
      this.emit('end')
    })()
    // A connect() issued meanwhile re-opens only once this close — its 'end'
    // included — is over.
    this.quitDone = closing.then(noop, noop)
    await closing
    return 'OK'
  }

  /**
   * Hard close. Unlike {@link quit} this does *not* drain. Real node-redis'
   * `disconnect()` is literally `Promise.resolve(this.destroy())`, so all of
   * it — including the throw on an already-closed client — happens
   * synchronously inside destroy(), before any promise exists:
   * `client.disconnect().catch(...)` crashes against the real client, and so
   * it does here.
   */
  // Deliberately NOT `async`: that would turn destroy()'s synchronous throw into
  // a rejection, which is exactly the divergence this mirrors away.
  disconnect(): Promise<void> {
    this.destroy()
    return Promise.resolve()
  }

  /**
   * node-redis exposes a synchronous destroy(). It flushes — every unit of
   * work not yet answered, queued or executing, rejects with
   * {@link disconnectsClientError} — then tears down and emits 'end' in-line.
   *
   * Escalating a pending {@link quit} (the "graceful, then force" teardown
   * pattern) follows real node-redis v6 exactly as observed against a live
   * server: the flush also rejects the quit() itself, and then destroy() STILL
   * throws `ClientClosedError`, and no 'end' is emitted. That is because real
   * destroy() flushes its queue first and only then reaches its socket, which
   * the pending quit() had already marked closed. (Real node-redis also leaks
   * the socket on this path — the server keeps the connection and the process
   * never exits. That is deliberately NOT reproduced: the facade still tears
   * everything down, so nothing hangs.)
   */
  destroy(): void {
    const rejectQuit = this.rejectPendingQuit
    if (this.closed && !rejectQuit) {
      throw clientClosedError()
    }
    // Flush first. Both flags go up synchronously, so nothing queued slips
    // past, and anything executing is rejected when it settles.
    this.closed = true
    this.closeCount += 1
    this.flushPendingWith = disconnectsClientError
    this.rejectPendingQuit = undefined
    rejectQuit?.(disconnectsClientError())
    // teardown() is async (it awaits the push-drain loop). Re-emit a rejection
    // as an 'error' event, which is what node-redis does with a teardown
    // failure — and, like any EventEmitter, that itself throws when nothing is
    // listening. teardown() releases everything it owns before this point, so
    // the throw leaks nothing. Its synchronous prefix closes the session, which
    // is what unblocks an executing BLPOP so the flush can reject it.
    void this.teardown().catch(err => this.emit('error', err))
    if (rejectQuit) {
      throw clientClosedError()
    }
    this.emit('end')
  }

  /**
   * Releases the session, pub/sub and (when owned) the state. Guarded by its
   * own flag rather than {@link closed}, which the close methods claim earlier
   * — `quit()` sets it before draining, so it is already true by the time
   * teardown runs. It never sets {@link closed} itself: a `connect()` issued
   * while a `quit()` drained has already flipped it back to open, and its
   * re-open waits for this release ({@link teardownDone}) before it runs.
   */
  private teardown(): Promise<void> {
    if (this.tornDown) {
      return this.teardownDone
    }
    this.tornDown = true
    const released = this.releaseResources()
    this.teardownDone = released.then(noop, noop)
    return released
  }

  private async releaseResources(): Promise<void> {
    try {
      this.session.close()
      if (this.pubsub) {
        const pubsub = this.pubsub
        this.pubsub = undefined
        pubsub.abort.abort()
        pubsub.session.close()
        // Wait for the drain loop to observe the abort and finish, so no async
        // iteration dangles past teardown.
        // The listener maps stay: they are the client's, and a later
        // connect() re-subscribes them (#600).
        await pubsub.drained
      }
    } finally {
      // In a `finally` because a push-drain loop that rejects must not strand
      // the state: closing it is what clears the self-rescheduling
      // active-expiry timer, and a live timer keeps the process alive.
      // Only the creating client owns the state graph — duplicates share it and
      // must not close it out from under their siblings.
      if (this.ownsState) {
        this.backend.state.close()
      }
    }
  }

  /**
   * Keep the pub/sub session on the client's protocol. A RESP3 node-redis
   * client subscribes on its one connection (only RESP2 needs a connection
   * reserved for pub/sub), so its subscriptions run at whatever that
   * connection negotiated: its `RESP` option, or a later `HELLO`. The facade
   * keeps a separate session for them, so it carries the command session's
   * version over before each (un)subscribe, and after any command that moves
   * it while subscribed (see {@link followingProtocol}). `CLIENT LIST` then
   * reports the subscriber at the client's `resp=`, as it does against a real
   * server.
   */
  private syncPubSubProtocol(
    pubsub: NonNullable<NodeRedisMockClient['pubsub']>,
  ): void {
    pubsub.session.setProtocolVersion(this.session.protocolVersion)
  }

  private ensurePubSub(): NonNullable<NodeRedisMockClient['pubsub']> {
    if (this.pubsub) {
      return this.pubsub
    }

    const abort = new AbortController()
    const session = new ClientSession({
      server: this.backend.state,
      executor: this.backend.executor,
      database: this.selectedDatabase,
    })
    session.setProtocolVersion(this.session.protocolVersion)
    // Drain pushes for the lifetime of the subscription; route each delivered
    // message/pmessage frame to the registered listeners. The promise is held
    // so teardown can await its completion.
    const pubsub: NonNullable<NodeRedisMockClient['pubsub']> = {
      session,
      abort,
      drained: Promise.resolve(),
    }
    pubsub.drained = this.drainPushes(pubsub, abort.signal)
    this.pubsub = pubsub
    return pubsub
  }

  private async drainPushes(
    pubsub: NonNullable<NodeRedisMockClient['pubsub']>,
    signal: AbortSignal,
  ): Promise<void> {
    try {
      for await (const frame of pubsub.session.readPushes(signal)) {
        this.dispatchPush(pubsub, frame)
      }
    } catch (err) {
      if (!signal.aborted) {
        this.emit('error', err)
      }
    }
  }

  private dispatchPush(
    pubsub: NonNullable<NodeRedisMockClient['pubsub']>,
    frame: RedisResult,
  ): void {
    const value = frame.value
    if (value.kind !== 'push') {
      return
    }
    // A push frame's type is its `name` ('message' / 'pmessage' / 'subscribe' /
    // …); `items` is just the payload. We only deliver actual messages —
    // subscribe/unsubscribe confirmations are consumed elsewhere.
    const items = value.items.map(item =>
      String(
        decodeReply(
          item,
          pubsub.session.protocolVersion,
          pubsub.session.server.profile,
        ),
      ),
    )

    if (value.name === 'message') {
      const [channel, message] = items
      notify(this.channelListeners.get(channel), message, channel)
      return
    }

    if (value.name === 'pmessage') {
      const [pattern, channel, message] = items
      notify(this.patternListeners.get(pattern), message, channel)
    }
  }
}

type RedisErrorValue = Extract<RedisValue, { kind: 'error' }>

/** One replayed MULTI → … → EXEC span, with what each reply must decode under. */
type TransactionSpan = {
  /** EXEC's reply. */
  value: RedisValue
  /** The first command that failed to queue (EXEC then answered `EXECABORT`). */
  queueError?: RedisErrorValue
  /** The encoder's `TypeError` for the first command never written, if any. */
  encodeError?: unknown
  /** The protocol in force once the transaction finished. */
  respVersion: RespVersion
  /** The served profile, which spells a RESP2 `double`. */
  profile: CompatibilityProfile
  /** Protocol each queued command's reply was produced under, by index. */
  replyVersions: readonly RespVersion[]
}

/**
 * The RESP version each queued reply was produced under. A `HELLO` inside a
 * MULTI switches the protocol partway through EXEC, and real Redis encodes the
 * items before it under the old version and the rest — including `HELLO`'s own
 * reply — under the new one. Verified against Redis 8.0.6 with node-redis@6, in
 * both directions:
 *
 *   MULTI; ZRANGE z 0 -1 WITHSCORES; HELLO 3; ZRANGE …; EXEC   (on RESP2)
 *     → ["a","1","b","2"], <RESP3 map>, [["a",1],["b",2]]
 *   the same with HELLO 2 on a RESP3 connection
 *     → [["a",1],["b",2]], <RESP2 array>, ["a","1","b","2"]
 *
 * `ClientSession` gets this right for the wire path by pre-encoding each item
 * as it goes (its `sawProtocolSwitch`), but hands the facade only the decoded
 * array, so the switch is replayed here from the queue the facade owns.
 *
 * Only `HELLO <n>` is modelled — `RESET` is not queued by real Redis, it runs
 * immediately and aborts the transaction (verified: `MULTI; RESET; EXEC` →
 * `ERR EXEC without MULTI`). A queued `HELLO` that *fails* leaves the protocol
 * where it was, so the replay skips any whose slot in EXEC's array came back
 * an error; that is what makes it exact rather than a guess, and it is why
 * there is no "prediction missed, use the final version everywhere" fallback:
 * such a fallback is wrong for every item before the divergence (it decodes a
 * reply that ran at RESP2 as RESP3), and with failed HELLOs accounted for
 * there is nothing left for it to catch but a protocol-moving command this
 * facade does not know about — where a replay that is right up to the
 * divergence still beats a uniform value that is wrong before it.
 */
function replayProtocolSwitches(
  queued: readonly NodeRedisCommandArgument[][],
  before: RespVersion,
  result: RedisValue,
): RespVersion[] {
  const items =
    result.kind === 'array' || result.kind === 'set' ? result.items : []
  const versions: RespVersion[] = []
  let current = before
  queued.forEach((args, index) => {
    if (items[index]?.kind !== 'error') {
      current = protocolSwitchedBy(args) ?? current
    }
    versions.push(current)
  })
  return versions
}

/** The version a queued `HELLO <2|3>` switches to, or undefined for anything else. */
function protocolSwitchedBy(
  args: readonly NodeRedisCommandArgument[],
): RespVersion | undefined {
  if (args.length < 2 || toText(args[0]).toLowerCase() !== 'hello') {
    return undefined
  }
  const version = Number(toText(args[1]))
  return version === 2 || version === 3 ? version : undefined
}

function toText(arg: NodeRedisCommandArgument): string {
  return Buffer.isBuffer(arg) ? arg.toString('utf8') : arg
}

/**
 * node-redis' `zRangeArgument`: the bounds, then `BY` (only `'SCORE'` /
 * `'LEX'` add anything), `REV` and `LIMIT offset count`, in that order.
 */
function zRangeArguments(
  min: NodeRedisCommandArgument | number,
  max: NodeRedisCommandArgument | number,
  options: NodeRedisZRangeOptions | undefined,
): NodeRedisCommandArgument[] {
  const args = [doubleArgument(min), doubleArgument(max)]
  switch (options?.BY) {
    case 'SCORE':
      args.push('BYSCORE')
      break
    case 'LEX':
      args.push('BYLEX')
      break
  }
  if (options?.REV) {
    args.push('REV')
  }
  if (options?.LIMIT) {
    args.push(
      'LIMIT',
      options.LIMIT.offset.toString(),
      options.LIMIT.count.toString(),
    )
  }
  return args
}

/**
 * node-redis' `transformStringDoubleArgument`: a number is spelled the way
 * Redis parses a score bound (`Infinity` → `+inf`), anything else passes
 * through untouched.
 */
function doubleArgument(
  value: NodeRedisCommandArgument | number,
): NodeRedisCommandArgument {
  if (typeof value !== 'number') {
    return value
  }
  if (value === Infinity) {
    return '+inf'
  }
  if (value === -Infinity) {
    return '-inf'
  }
  return value.toString()
}

/**
 * MULTI builder mirroring node-redis' chainable transaction API. Queues curated
 * commands and replays them with a real MULTI/EXEC on the owning session, so the
 * server's transaction + WATCH semantics drive the result (EXEC returns an array
 * of replies, or `null` when a watched key changed).
 */
// eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging -- see NodeRedisCommandMethods
export class NodeRedisMockMulti {
  private readonly queued: NodeRedisCommandArgument[][] = []
  /** Per queued command: its `transformReply` and the parser's `preserve`. */
  private readonly transforms: (
    { transform: NodeRedisTransformReply; preserve: unknown } | undefined
  )[] = []
  /** Per queued command: node-redis' `parser.firstKey`, for cluster routing. */
  private readonly firstKeys: (NodeRedisCommandArgument | undefined)[] = []
  /** A queued `select()`'s database, adopted by the client once EXEC answers. */
  private selectedDatabase?: number
  private settled = false

  /**
   * @param runTransaction replays the queue as MULTI … EXEC.
   * @param resp the client's configured `RESP`, which picks each node-redis
   *   command's `transformReply`.
   */
  constructor(
    private readonly runTransaction: (
      queued: NodeRedisCommandArgument[][],
      selectedDatabase: number | undefined,
      firstKeys: readonly (NodeRedisCommandArgument | undefined)[],
    ) => Promise<TransactionSpan>,
    private readonly resp: NodeRedisRespVersion = nodeRedisDefaultResp(),
  ) {
    NodeRedisMockMulti.installNodeRedisCommands()
  }

  private static nodeRedisCommandsInstalled = false

  /**
   * node-redis' command methods on `multi()`, as `RedisClientMultiCommand`
   * builds them: parse with node-redis' parser, queue `parser.redisArgs`, and
   * keep the command's `transformReply` for {@link exec}. See
   * `CommandRunner.installNodeRedisCommands`.
   */
  private static installNodeRedisCommands(): void {
    if (NodeRedisMockMulti.nodeRedisCommandsInstalled) {
      return
    }
    NodeRedisMockMulti.nodeRedisCommandsInstalled = true
    const set = nodeRedisCommandSet(requireRedis())
    if (!set) {
      return
    }
    for (const [name, command] of Object.entries(set.commands)) {
      if (name in NodeRedisMockMulti.prototype) {
        continue
      }
      Object.defineProperty(NodeRedisMockMulti.prototype, name, {
        configurable: true,
        writable: true,
        value: function (this: NodeRedisMockMulti, ...args: unknown[]) {
          const parser = parseNodeRedisCommand(set, command, args)
          const transform = transformReplyFor(command, this.resp)
          return this.queue(
            parser.redisArgs,
            transform && { transform, preserve: parser.preserve },
            parser.firstKey,
          )
        },
      })
    }
  }

  /**
   * node-redis' `multi().select(db)`: queues SELECT, and the client keeps
   * `db` as its selected database once the transaction ran.
   */
  select(db: number): this {
    this.selectedDatabase = db
    return this.queue(['SELECT', String(db)])
  }

  set(
    key: NodeRedisCommandArgument,
    value: NodeRedisCommandArgument | number,
    options?: NodeRedisSetOptions,
  ): this {
    return this.queue(setArguments(key, value, [options]))
  }

  get(key: string): this {
    return this.queue(['GET', key])
  }

  del(...keys: NodeRedisVariadicArgument[]): this {
    return this.queue(['DEL', ...variadicArguments(keys)])
  }

  incr(key: string): this {
    return this.queue(['INCR', key])
  }

  hSet(
    key: NodeRedisCommandArgument,
    field: NodeRedisHashValue,
    value: NodeRedisHashValue,
  ): this
  hSet(key: NodeRedisCommandArgument, fields: NodeRedisHashFields): this
  hSet(
    key: NodeRedisCommandArgument,
    fields: NodeRedisHashValue | NodeRedisHashFields,
    value?: NodeRedisHashValue,
  ): this {
    return this.queue(['HSET', key, ...hashFieldArguments(fields, value)])
  }

  hGet(key: string, field: string): this {
    return this.queue(['HGET', key, field])
  }

  /** Generic escape hatch: queue any raw command. */
  addCommand(args: NodeRedisCommandArgument[]): this {
    return this.queue(args)
  }

  private queue(
    args: NodeRedisCommandArgument[],
    transform?: { transform: NodeRedisTransformReply; preserve: unknown },
    firstKey?: NodeRedisCommandArgument,
  ): this {
    this.queued.push(args)
    this.transforms.push(transform)
    this.firstKeys.push(firstKey)
    return this
  }

  /**
   * Replay the queued commands inside a real MULTI/EXEC and return the array of
   * decoded replies. Matching node-redis, a watch-aborted transaction throws a
   * `WatchError` (never returns null), and per-command errors are aggregated
   * into a single `MultiErrorReply` carrying every reply + the error indexes
   * (on a `redis` that has that class — older ones resolve, errors inline).
   */
  async exec(): Promise<NodeRedisReply[]> {
    this.assertOpen()
    this.settled = true

    const {
      value: result,
      queueError,
      encodeError,
      respVersion,
      profile,
      replyVersions,
    } = await this.runTransaction(
      this.queued,
      this.selectedDatabase,
      this.firstKeys,
    )
    const errors = redisErrors()

    if (encodeError !== undefined) {
      // node-redis rejects the un-encodable command while writing, before any
      // reply is read, so its TypeError rejects exec() ahead of every reply
      // error; the other commands and EXEC still ran (#490).
      throw encodeError
    }
    if (queueError) {
      // node-redis pipelines MULTI, the queued commands and EXEC and awaits
      // them together, so the first command that failed to queue rejects the
      // whole exec() with its own error, ahead of EXEC's `EXECABORT` (#510).
      throw new errors.SimpleError(redisErrorText(queueError))
    }
    if (result.kind === 'null' || result.kind === 'null-array') {
      // RESP2 `*-1` (a watched key changed). node-redis throws, never null.
      throw new errors.WatchError()
    }
    if (result.kind !== 'array' && result.kind !== 'set') {
      // Any other EXEC reply — an error — rejects as decoded.
      return [decodeReply(result, respVersion, profile)]
    }

    const replies: unknown[] = []
    const errorIndexes: number[] = []
    result.items.forEach((item, index) => {
      if (item.kind === 'error') {
        replies.push(new errors.SimpleError(redisErrorText(item)))
        errorIndexes.push(index)
        return
      }
      const reply = decodeReply(
        item,
        replyVersions[index] ?? respVersion,
        profile,
      )
      // node-redis' `transformReplies`: each successful reply through its
      // command's `transformReply`; errors stay as they are.
      const transform = this.transforms[index]
      replies.push(
        transform
          ? transform.transform(reply, transform.preserve, undefined)
          : reply,
      )
    })

    // A `redis` older than 4.6.12 has no MultiErrorReply, and its `exec()`
    // resolves with each failed command's error inline instead of throwing.
    if (errorIndexes.length > 0 && errors.MultiErrorReply) {
      throw new errors.MultiErrorReply(replies, errorIndexes)
    }
    return replies as NodeRedisReply[]
  }

  /** Cancel the transaction without running the queued commands. */
  async discard(): Promise<void> {
    this.assertOpen()
    this.settled = true
    // No MULTI was opened on the session yet (we only open it in exec()), so
    // discarding is purely client-side: drop the queue.
    this.queued.length = 0
    this.transforms.length = 0
    this.firstKeys.length = 0
  }

  private assertOpen(): void {
    if (this.settled) {
      throw new Error('this multi has already been executed or discarded')
    }
  }
}

/** First synthetic node port of a {@link NodeRedisMockCluster}. */
const CLUSTER_BASE_PORT = 7000

/**
 * In-memory node-redis cluster facade. Reuses {@link buildClusterNodes} for a
 * TCP-free cluster, then routes each command to the slot owner's session
 * (computed via {@link RedisClusterTopology.calculateSlotForKeys}). The curated
 * method surface is inherited unchanged from {@link CommandRunner}.
 */
// eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging -- see NodeRedisCommandMethods
export class NodeRedisMockCluster extends CommandRunner {
  private readonly topology: RedisClusterTopology
  /** The master pipelines: what this facade routes to. */
  private readonly masterPipelines: ClusterNodePipeline[]
  /**
   * Whether this client built the nodes. Only the creator closes their state
   * and replication links; a {@link duplicate} shares them and closes just its
   * own sessions, as the standalone client's `ownsState` does.
   */
  private readonly ownsNodes: boolean
  /** node-redis' node objects, one per node id, kept so they compare by identity. */
  private readonly nodeObjects = new Map<string, NodeRedisMockClusterNode>()
  private readonly sessions = new Map<string, ClientSession>()
  /**
   * One FIFO lock per node session — {@link runOnNode}. Keyed by the session,
   * so the fresh sessions a later command opens never inherit a torn-down
   * one's queue.
   */
  private readonly nodeLocks = new WeakMap<ClientSession, Promise<void>>()
  private readonly replicationLinks: { close(): void }[]
  /**
   * RESP version for the client as a whole, not for one node. Real node-redis
   * hands its `RESP` setting to every node client when it builds the slot map
   * (`@redis/client`'s `cluster-slots`), so no key can come back in the other
   * protocol's shape. `HELLO` here is keyless, so it only ever reaches
   * `masters[0]`; the version it negotiates is held here and replayed onto each
   * other node session by {@link syncProtocol} before that session serves a
   * command. Reading *this* rather than the serving session's version is also
   * what keeps two concurrent commands on different nodes from decoding under
   * each other's protocol.
   *
   * What it does not remove — and cannot — is the ambiguity of a `HELLO` that
   * is itself in flight. Commands here run concurrently, so every command
   * already running when the switch lands reads this field only when its reply
   * is decoded: the replies that can come back in the other protocol's shape
   * are bounded by the set of commands in flight at that moment, not by one.
   * Real node-redis has the same ambiguity for the same reason — its parser
   * belongs to the connection, not to the individual reply — so the point of
   * the conditional write in {@link run} is to stop that window outliving the
   * switch, not to close it.
   */
  private clientRespVersion: RespVersion
  /** The `RESP` this client was configured with: its handshake protocol. */
  private readonly resp: NodeRedisRespVersion

  private constructor(
    topology: RedisClusterTopology,
    masters: ClusterNodePipeline[],
    replicationLinks: readonly { close(): void }[],
    options: NodeRedisMockClientOptions,
    ownsNodes = true,
  ) {
    super()
    this.topology = topology
    this.masterPipelines = masters
    this.replicationLinks = [...replicationLinks]
    this.ownsNodes = ownsNodes
    this.resp = options.RESP ?? nodeRedisDefaultResp()
    this.clientRespVersion = this.resp
    queueMicrotask(() => {
      this.emit('connect')
      this.emit('ready')
    })
  }

  static create(
    options: NodeRedisMockClusterOptions,
    clientOptions: NodeRedisMockClientOptions = {},
  ): NodeRedisMockCluster {
    const { topology, nodes, replicationLinks } = buildClusterNodes({
      masters: options.masters,
      replicasPerMaster: options.replicas ?? 0,
      // Synthetic ports, as the ioredis mock uses: nothing listens on them,
      // but each node gets its own `host:port` — the identity node-redis'
      // `masters` / `slots` nodes and CLUSTER SLOTS / NODES / MOVED carry.
      basePort: CLUSTER_BASE_PORT,
    })
    const masters = nodes.filter(node => node.role === 'master')
    return new NodeRedisMockCluster(
      topology,
      masters,
      replicationLinks,
      clientOptions,
    )
  }

  async connect(): Promise<this> {
    return this
  }

  /**
   * A second cluster client over the **same** nodes and keyspace, returned
   * synchronously like node-redis' `duplicate(overrides)`, with this client's
   * `RESP` unless `overrides` says otherwise. It has its own connection
   * (session) per node, so a blocked command on one client does not hold up
   * the other, and closing it leaves the nodes to the client that built them.
   * Like the standalone {@link NodeRedisMockClient.duplicate} it is already
   * open; `connect()` resolves at once.
   */
  duplicate(overrides: NodeRedisMockClientOptions = {}): NodeRedisMockCluster {
    return new NodeRedisMockCluster(
      this.topology,
      this.masterPipelines,
      this.replicationLinks,
      { RESP: 'RESP' in overrides ? overrides.RESP : this.resp },
      false,
    )
  }

  /**
   * node-redis' `cluster.slots`: one entry per hash slot (16384), each the
   * `{ master }` shard that owns it. Real node-redis adds `replicas` only with
   * its `useReplicas` option, which the facade does not have. Read from the
   * live topology on each access; the node objects are shared with
   * {@link masters}.
   */
  get slots(): NodeRedisMockClusterShard[] {
    const shards = new Map<string, NodeRedisMockClusterShard>()
    const slots: NodeRedisMockClusterShard[] = []
    for (let slot = 0; slot < REDIS_CLUSTER_SLOT_COUNT; slot++) {
      const owner = this.topology.getSlotOwner(slot)
      if (!owner) {
        continue
      }
      let shard = shards.get(owner.id)
      if (!shard) {
        shard = { master: this.nodeObject(owner) }
        shards.set(owner.id, shard)
      }
      slots[slot] = shard
    }
    return slots
  }

  /** node-redis' `cluster.masters`: `{ id, host, port, address, readonly }` per master. */
  get masters(): NodeRedisMockClusterNode[] {
    return this.topology.nodes
      .filter(node => node.role === 'master')
      .map(node => this.nodeObject(node))
  }

  private nodeObject(node: {
    id: string
    host: string
    port: number
  }): NodeRedisMockClusterNode {
    let object = this.nodeObjects.get(node.id)
    if (!object) {
      object = {
        id: node.id,
        host: node.host,
        port: node.port,
        address: `${node.host}:${node.port}`,
        readonly: false,
      }
      this.nodeObjects.set(node.id, object)
    }
    return object
  }

  /**
   * node-redis' cluster escape hatch, `sendCommand(firstKey, isReadonly,
   * args, options?)`. A real cluster client sends it to `firstKey`'s slot
   * owner (a random node without one) and follows `MOVED`, so a keyed command
   * ends up on the owner of its own keys whatever `firstKey` says: the facade
   * routes by those keys, by `firstKey` for a keyless command, and to the
   * first master without either. `isReadonly` and `options` change nothing
   * here (there are no replica reads without `useReplicas`).
   */
  async sendCommand(
    firstKey: NodeRedisCommandArgument | undefined,
    isReadonly: boolean | undefined,
    args: NodeRedisCommandArgument[],
    options?: unknown,
  ): Promise<NodeRedisReply> {
    void isReadonly
    void options
    if (!Array.isArray(args)) {
      throw new TypeError(
        'NodeRedisMockCluster.sendCommand(firstKey, isReadonly, args): args must be the command array, as in node-redis',
      )
    }
    return this.decode(await this.runRouted(args, firstKey))
  }

  protected get respVersion(): RespVersion {
    return this.clientRespVersion
  }

  protected get nodeRedisResp(): NodeRedisRespVersion {
    return this.resp
  }

  /**
   * node-redis' `cluster.multi(routing?)`: the transaction runs on one node,
   * the slot owner of `routing` or else of the first queued command with a
   * key (`RedisClusterMultiCommand`'s `firstKey`); a keyless one goes to the
   * first master. Keys of other slots answer `CROSSSLOT` (or `MOVED`) there,
   * as on a real cluster.
   */
  multi(routing?: NodeRedisCommandArgument): NodeRedisMockMulti {
    return new NodeRedisMockMulti(async (queued, _selected, firstKeys) => {
      let session: ClientSession
      if (routing !== undefined) {
        session = this.sessionForCommand([], routing)
      } else {
        const index = queued.findIndex(
          (args, i) =>
            this.routingKeys(args).length > 0 || firstKeys[i] !== undefined,
        )
        session =
          index === -1
            ? this.sessionForCommand([])
            : this.sessionForCommand(queued[index], firstKeys[index])
      }
      return this.runOnNode(session, async () => {
        await this.syncProtocol(session)
        return runTransactionSpan(session, queued, this.closed)
      })
    }, this.resp)
  }

  protected get profile(): CompatibilityProfile {
    // Every node shares one hoisted profile (see buildClusterNodes).
    return this.masterPipelines[0].state.profile
  }

  protected run(
    args: NodeRedisCommandArgument[],
    firstKey?: NodeRedisCommandArgument,
  ): Promise<RedisValue> {
    return this.runRouted(args, firstKey)
  }

  private async runRouted(
    args: NodeRedisCommandArgument[],
    firstKey: NodeRedisCommandArgument | undefined,
  ): Promise<RedisValue> {
    // DELIBERATE DEVIATION: a command on a closed cluster throws
    // ClientClosedError here, where real node-redis v6 throws an internal
    // `TypeError: Cannot read properties of undefined (reading 'replicas')` —
    // it dereferences the slot map its own close path just reset. That is an
    // upstream crash, not a contract, and reproducing it would only teach
    // callers to catch a TypeError. Everything else about the cluster close
    // path follows real node-redis exactly; see the note above quit().
    const session = this.sessionForCommand(args, firstKey)
    return this.runOnNode(session, () => this.runOnSessionSynced(session, args))
  }

  private async runOnSessionSynced(
    session: ClientSession,
    args: NodeRedisCommandArgument[],
  ): Promise<RedisValue> {
    await this.syncProtocol(session)
    // Captured *after* syncProtocol, which moves the session itself: reading
    // it earlier would make the first command on a newly-synced node look like
    // a protocol-moving one and re-adopt a version the client already holds.
    const before = session.protocolVersion
    const value = await runOnSession(session, args, this.closed)
    // Adopt the version only when *this* command moved *this* session — a
    // HELLO or a RESET. Writing it unconditionally would let an ordinary
    // command racing a HELLO put its own stale version back: commands here run
    // concurrently, so a command that started before the HELLO landed sees a
    // session still on the old protocol and would undo the switch for the
    // whole client. Comparing before/after instead of matching on the command
    // name keeps RESET (and anything future) working without a second list of
    // protocol-moving commands to maintain.
    if (session.protocolVersion !== before) {
      this.clientRespVersion = session.protocolVersion
    }
    return value
  }

  /**
   * Run `fn` on `session` after every unit of work issued earlier on that node
   * settles. Real node-redis has one connection per node and writes a whole
   * `MULTI … EXEC` as one batch on it, so nothing issued alongside a
   * transaction lands inside it; the facade's span is several awaited steps,
   * so the commands routed to that node (and the protocol sync before them)
   * queue behind it here instead. Like that connection, a parked blocking
   * command holds the node for this client until it is answered; a
   * {@link duplicate} has its own sessions, so it is not held.
   */
  private runOnNode<T>(
    session: ClientSession,
    fn: () => Promise<T>,
  ): Promise<T> {
    const previous = this.nodeLocks.get(session) ?? Promise.resolve()
    const result = previous.then(fn, fn)
    this.nodeLocks.set(
      session,
      result.then(
        () => undefined,
        () => undefined,
      ),
    )
    return result
  }

  /**
   * Bring `session` up to {@link clientRespVersion} before it serves a command
   * — the facade's stand-in for the RESP handshake node-redis performs on every
   * node connection it opens. Done lazily, on the node's next command, which is
   * indistinguishable from doing it eagerly: a session that has not run a
   * command has not produced a reply to mis-shape.
   */
  private async syncProtocol(session: ClientSession): Promise<void> {
    if (session.protocolVersion === this.clientRespVersion) {
      return
    }
    await runOnSession(
      session,
      ['HELLO', String(this.clientRespVersion)],
      this.closed,
    )
  }

  // A real `RedisCluster` closes on completely different terms from a single
  // client, so do NOT copy the standalone contract here. Ground-truthed against
  // node-redis v6 driving a live 3-node cluster, and matching
  // `cluster/cluster-slots.js`, where every close routes through `#destroy()`:
  //
  //  - it emits 'disconnect', and NEVER 'end' — no 'end' emit exists anywhere
  //    under `cluster/`;
  //  - a redundant close does NOT throw. `#destroy()` resets its slot/node maps
  //    first, so a second call simply finds nothing to close, awaits
  //    `Promise.allSettled([])` and emits 'disconnect' AGAIN. The event is
  //    therefore once per close *call*, not once per open→closed transition;
  //  - quit()/disconnect()/destroy() all resolve `undefined` — the cluster's
  //    quit() returns `#destroy()`'s promise, not the standalone client's 'OK'.
  //
  // quit()/disconnect() stay async to match node-redis' signatures, but this
  // teardown is fully synchronous (no push-drain loop to await) — unlike the
  // standalone client, whose teardown awaits its pub/sub drain.

  async quit(): Promise<void> {
    await this.closeAsync()
  }

  /** node-redis 5+'s `close()`: the same close as {@link quit}. */
  async close(): Promise<void> {
    await this.closeAsync()
  }

  async disconnect(): Promise<void> {
    await this.closeAsync()
  }

  /** Synchronous in real node-redis too, so it emits in-line. */
  destroy(): void {
    this.teardown()
    this.emit('disconnect')
  }

  /**
   * Real quit()/disconnect() run `#destroy()`, which tears down synchronously
   * but emits 'disconnect' only after `await Promise.allSettled(...)` — so the
   * caller never sees the event before the call returns, and always sees it
   * before the returned promise settles. The await reproduces that ordering.
   */
  private async closeAsync(): Promise<void> {
    this.teardown()
    await Promise.allSettled([])
    this.emit('disconnect')
  }

  private teardown(): void {
    if (this.closed) {
      return
    }
    this.closed = true
    for (const session of this.sessions.values()) {
      session.close()
    }
    this.sessions.clear()
    // Per-connection state goes back to its handshake default alongside the
    // sessions it belongs to.
    this.clientRespVersion = this.resp
    if (!this.ownsNodes) {
      // A duplicate: the nodes, their state and links are the creator's.
      return
    }
    for (const link of this.replicationLinks) {
      link.close()
    }
    // Close each master's state to clear its active-expiry timer. (Replicas are
    // built with expiry disabled, so only masters arm one.)
    for (const node of this.masterPipelines) {
      node.state.close()
    }
  }

  /**
   * Resolve (and cache) a session on the master that owns the slot for the
   * command's keys. A keyless command runs on `firstKey`'s slot owner when
   * {@link sendCommand} names one, else on the first master.
   *
   * There is deliberately no client-side rejection here. A key set spanning
   * slots gives `-1`, which owns no node and falls through to the first master;
   * that node's `ClusterPolicy` recomputes the same keys and raises the real
   * error, which `decodeReply` surfaces as the same `ErrorReply`. Refusing here
   * instead would mean a second copy of rules the policy already owns, and
   * would pre-empt the ones it does not share — `SORT` routes on its source
   * key alone (plus `STORE`), while `ClusterPolicy` separately refuses BY/GET
   * patterns that may resolve to another slot with `BY option of SORT denied
   * in Cluster mode …` rather than `CROSSSLOT`.
   *
   * KNOWN LIMITATION — pub/sub is not supported through this cluster facade.
   * It exposes no subscribe API, and a raw `sendCommand(['SUBSCRIBE', …])` puts
   * the cached session for its node into subscriber mode for good: every later
   * command on that node is then refused with `ERR Can't execute …`. Because
   * `SUBSCRIBE` has no keys it lands on `masters[0]`, which also serves every
   * keyless command and the first slot range, so the blast radius is wide.
   * Fixing it properly means what the standalone client already does — a
   * dedicated pub/sub session plus a push-drain loop ({@link
   * NodeRedisMockClient.ensurePubSub}) — which is a feature, not a routing
   * change. Use {@link NodeRedisMockClient} for pub/sub.
   */
  private sessionForCommand(
    args: NodeRedisCommandArgument[],
    firstKey?: NodeRedisCommandArgument,
  ): ClientSession {
    const keys = this.routingKeys(args)
    const slot = this.topology.calculateSlotForKeys(
      keys.length === 0 && firstKey !== undefined ? [toBuffer(firstKey)] : keys,
    )
    const owner =
      slot === null
        ? this.masterPipelines[0]
        : (this.topology.getSlotOwner(slot) ?? this.masterPipelines[0])

    return this.sessionFor(owner.id)
  }

  /**
   * The command's routing keys, taken from the executor's own
   * {@link CommandExecutor.plan} — the exact extraction `ClusterPolicy` routes
   * on. Guessing them from the argument list gets multi-key commands (`MSET`,
   * `RENAME`), numkeys-prefixed ones (`EVAL`, `ZDIFF`, `LMPOP`), subcommand-
   * prefixed ones (`BITOP`) and STORE targets (`GEORADIUS … STORE`) wrong,
   * which picks the wrong node.
   *
   * Two invariants this leans on, neither enforced by a type:
   *
   *  - Planning is registry + schema only — no policy runs — so any master's
   *    executor answers identically. True because `buildClusterNodes` hands
   *    every node one hoisted profile and the same registry, differing only in
   *    their `CLUSTER` definitions. Per-node profiles would break it.
   *  - Every `RedisCommandError` out of `plan()` means "no keys we can route
   *    on" — an unregistered name, or an argument list the schema rejects.
   *    True while no registered command declares keys it cannot extract. Such
   *    a command goes to the first master and the normal pipeline turns it
   *    into the canonical error reply, exactly as the standalone client does,
   *    rather than growing a second error path here.
   *
   * Planning is not free — `session.execute` plans the command again, so a
   * cluster command pays two schema parses. A `routingKeysFor()` seam on the
   * executor would be cheaper and tidier, but with one call site it is not yet
   * worth the API.
   */
  private routingKeys(args: NodeRedisCommandArgument[]): readonly Buffer[] {
    if (args.length === 0) {
      return []
    }

    let encoded: Buffer[]
    try {
      encoded = encodeArguments(args)
    } catch {
      // Un-encodable: no route. The command itself rejects with the
      // encoder's TypeError when it is written (inside exec(), it alone).
      return []
    }
    const [name, ...rest] = encoded
    try {
      return this.masterPipelines[0].executor.plan(name, rest).keys
    } catch (err) {
      if (err instanceof RedisCommandError) {
        return []
      }
      throw err
    }
  }

  private sessionFor(nodeId: string): ClientSession {
    const existing = this.sessions.get(nodeId)
    if (existing) {
      return existing
    }
    const node = this.masterPipelines.find(master => master.id === nodeId)
    if (!node) {
      throw new Error(`No master pipeline for cluster node ${nodeId}`)
    }
    const session = new ClientSession({
      server: node.state,
      executor: node.executor,
      nodeRole: node.role,
    })
    this.sessions.set(nodeId, session)
    return session
  }
}

// --- shared helpers --------------------------------------------------------

/**
 * Replay MULTI → queued commands → EXEC on `session` as one span. EXEC is sent
 * even after a command failed to queue, so the session leaves MULTI (it
 * answers `EXECABORT`), exactly as node-redis pipelines it.
 *
 * A command the encoder refuses is never written: node-redis'
 * `commandsToWrite()` rejects just that command and writes the rest, EXEC
 * included (#490). Its `TypeError` is kept in {@link TransactionSpan.encodeError}.
 */
async function runTransactionSpan(
  session: ClientSession,
  queued: readonly NodeRedisCommandArgument[][],
  closed: boolean,
): Promise<TransactionSpan> {
  await runOnSession(session, ['MULTI'], closed)
  let queueError: RedisErrorValue | undefined
  let encodeError: unknown
  const written: NodeRedisCommandArgument[][] = []
  for (const args of queued) {
    let encoded: Buffer[]
    try {
      encoded = encodeArguments(args)
    } catch (err) {
      encodeError ??= err
      continue
    }
    written.push(args)
    const reply = await runEncoded(session, encoded, closed)
    if (reply.kind === 'error') {
      queueError ??= reply
    }
  }
  const before = session.protocolVersion
  const value = await runOnSession(session, ['EXEC'], closed)
  return {
    value,
    queueError,
    encodeError,
    respVersion: session.protocolVersion,
    profile: session.server.profile,
    replyVersions: replayProtocolSwitches(written, before, value),
  }
}

/**
 * Execute a tokenised command on a session and return its raw {@link RedisValue}
 * (callers decode it to the node-redis-correct shape). Throws when the client is
 * closed. A multi-target (UN)SUBSCRIBE's value is its first confirmation;
 * messages never flow here (pub/sub uses the dedicated push-draining session).
 *
 * Runs the reply's `afterReply` step, as the wire does. Nothing reads the push
 * queue of a non-pub/sub session, so `sendCommand(['MONITOR'])` resolves `OK`
 * but its feed lines are never surfaced — the facade has no `monitor()`.
 */
async function runOnSession(
  session: ClientSession,
  args: NodeRedisCommandArgument[],
  closed: boolean,
): Promise<RedisValue> {
  assertSessionOpen(closed)
  return runEncoded(session, encodeArguments(args), closed)
}

/** {@link runOnSession} for a command already through {@link encodeArguments}. */
async function runEncoded(
  session: ClientSession,
  [name, ...rest]: Buffer[],
  closed: boolean,
): Promise<RedisValue> {
  assertSessionOpen(closed)
  const result = await session.execute(name, rest)
  result.options?.afterReply?.()
  return result.value
}

function assertSessionOpen(closed: boolean): void {
  if (closed) {
    // Real node-redis rejects a command on a closed client with this exact
    // error, same as it does a redundant quit()/disconnect()/destroy().
    throw clientClosedError()
  }
}

/** The command's arguments as node-redis' encoder takes them (name at 0). */
function encodeArguments(args: readonly NodeRedisCommandArgument[]): Buffer[] {
  if (args.length === 0) {
    throw new Error('command requires at least a name')
  }
  return args.map((arg, index) => toBuffer(arg, index))
}

/**
 * Deliberately narrower than the shared `toRedisArgument`, which also accepts
 * `number` for {@link InMemoryRedisClient}. `@redis/client`'s encoder takes
 * `string | Buffer` and nothing else, so a facade that coerced anything wider
 * would green-light test code that throws against the real client.
 *
 * `Buffer.from` alone is not narrow enough: it happily converts arrays and
 * TypedArrays that node-redis rejects. Hence the explicit check, whose message
 * mirrors the encoder's.
 */
function toBuffer(arg: NodeRedisCommandArgument, index = 0): Buffer {
  if (Buffer.isBuffer(arg)) {
    return arg
  }
  if (typeof arg !== 'string') {
    throw new TypeError(
      `"arguments[${index}]" must be of type "string | Buffer", got ${typeof arg} instead.`,
    )
  }
  return Buffer.from(arg)
}

/** Adds `listener` under `key`; false when it was already there. */
function addListener(
  map: Map<string, Set<NodeRedisPubSubListener>>,
  key: string,
  listener: NodeRedisPubSubListener,
): boolean {
  const set = map.get(key) ?? new Set<NodeRedisPubSubListener>()
  if (set.has(listener)) {
    return false
  }
  set.add(listener)
  map.set(key, set)
  return true
}

/** Removes `listener` under `key`, and `key` once it has no listener left. */
function removeListener(
  map: Map<string, Set<NodeRedisPubSubListener>>,
  key: string,
  listener: NodeRedisPubSubListener,
): void {
  const set = map.get(key)
  if (!set) {
    return
  }
  set.delete(listener)
  if (set.size === 0) {
    map.delete(key)
  }
}

/** A channel or pattern as the listener maps key it (pushes decode to text). */
function pubSubName(name: NodeRedisCommandArgument): string {
  return Buffer.isBuffer(name) ? name.toString() : name
}

// --- node-redis argument forms ----------------------------------------------
//
// Each mirrors the `parseCommand` of the node-redis v6 command it serves
// (`@redis/client/dist/lib/commands/*.js`), so a facade method sends the
// argument list the real client would. Values pass through untouched except
// numbers, which node-redis stringifies; anything else non-string reaches
// toBuffer(), which rejects it as node-redis' encoder does.

function isArgumentList(
  value: unknown,
): value is readonly NodeRedisCommandArgument[] {
  return Array.isArray(value)
}

/** node-redis' `pushVariadicArguments`, over each of the method's arguments. */
function variadicArguments(
  values: readonly NodeRedisVariadicArgument[],
): NodeRedisCommandArgument[] {
  const out: NodeRedisCommandArgument[] = []
  for (const value of values) {
    if (isArgumentList(value)) {
      out.push(...value)
    } else {
      out.push(value)
    }
  }
  return out
}

/** node-redis stringifies a number argument; everything else passes as is. */
function numberArgument<T>(value: T | number): T | string {
  return typeof value === 'number' ? value.toString() : value
}

function setArguments(
  key: NodeRedisCommandArgument,
  value: NodeRedisCommandArgument | number,
  rest: readonly (NodeRedisCommandArgument | NodeRedisSetOptions | undefined)[],
): NodeRedisCommandArgument[] {
  const args: NodeRedisCommandArgument[] = ['SET', key, numberArgument(value)]
  for (const item of rest) {
    if (item === undefined) {
      continue
    }
    if (typeof item === 'string' || Buffer.isBuffer(item)) {
      args.push(item)
    } else {
      args.push(...setOptionArguments(item))
    }
  }
  return args
}

function setOptionArguments(
  options: NodeRedisSetOptions,
): NodeRedisCommandArgument[] {
  const args: NodeRedisCommandArgument[] = []
  const { expiration } = options
  if (expiration) {
    if (typeof expiration === 'string') {
      args.push(expiration)
    } else if (expiration.type === 'KEEPTTL') {
      args.push('KEEPTTL')
    } else {
      args.push(expiration.type, expiration.value.toString())
    }
  } else if (options.EX !== undefined) {
    args.push('EX', options.EX.toString())
  } else if (options.PX !== undefined) {
    args.push('PX', options.PX.toString())
  } else if (options.EXAT !== undefined) {
    args.push('EXAT', options.EXAT.toString())
  } else if (options.PXAT !== undefined) {
    args.push('PXAT', options.PXAT.toString())
  } else if (options.KEEPTTL) {
    args.push('KEEPTTL')
  }
  if (options.condition) {
    args.push(options.condition)
    if (options.matchValue !== undefined) {
      args.push(options.matchValue)
    }
  } else if (options.NX) {
    args.push('NX')
  } else if (options.XX) {
    args.push('XX')
  }
  if (options.GET) {
    args.push('GET')
  }
  return args
}

/**
 * node-redis' `parseMSetArguments`. Unlike `set()` / `hSet()` it does not
 * stringify numbers: they reach the encoder, whose `TypeError` rejects the
 * call (#490).
 */
function mSetArguments(
  toSet: NodeRedisMSetArguments,
): NodeRedisCommandArgument[] {
  const args: NodeRedisCommandArgument[] = []
  if (Array.isArray(toSet)) {
    if (toSet.length === 0) {
      throw new Error('empty toSet Argument')
    }
    if (Array.isArray(toSet[0])) {
      for (const [key, value] of toSet as readonly (readonly [
        NodeRedisCommandArgument,
        NodeRedisCommandArgument,
      ])[]) {
        args.push(key, value)
      }
    } else {
      const flat = toSet as readonly NodeRedisCommandArgument[]
      for (let i = 0; i < flat.length; i += 2) {
        args.push(flat[i], flat[i + 1])
      }
    }
    return args
  }
  for (const [key, value] of Object.entries(
    toSet as Record<string, NodeRedisCommandArgument>,
  )) {
    args.push(key, value)
  }
  return args
}

/** HSET's field/value list: node-redis' `pushMap` / `pushTuples` / `pushObject`. */
function hashFieldArguments(
  fields: NodeRedisHashValue | NodeRedisHashFields,
  value: NodeRedisHashValue | undefined,
): NodeRedisCommandArgument[] {
  if (
    typeof fields === 'string' ||
    typeof fields === 'number' ||
    Buffer.isBuffer(fields)
  ) {
    return [numberArgument(fields), numberArgument(value as NodeRedisHashValue)]
  }
  const args: NodeRedisCommandArgument[] = []
  if (fields instanceof Map) {
    for (const [field, fieldValue] of fields) {
      args.push(numberArgument(field), numberArgument(fieldValue))
    }
    return args
  }
  if (Array.isArray(fields)) {
    const pushTuples = (tuples: readonly unknown[]): void => {
      for (const tuple of tuples) {
        if (Array.isArray(tuple)) {
          pushTuples(tuple)
        } else {
          args.push(numberArgument(tuple as NodeRedisHashValue))
        }
      }
    }
    pushTuples(fields)
    return args
  }
  const object = fields as Record<string, NodeRedisHashValue>
  for (const field of Object.keys(object)) {
    args.push(field, numberArgument(object[field]))
  }
  return args
}

function zAddOptionArguments(
  options: NodeRedisZAddOptions | undefined,
): NodeRedisCommandArgument[] {
  const args: NodeRedisCommandArgument[] = []
  if (options?.condition) {
    args.push(options.condition)
  } else if (options?.NX) {
    args.push('NX')
  } else if (options?.XX) {
    args.push('XX')
  }
  if (options?.comparison) {
    args.push(options.comparison)
  } else if (options?.LT) {
    args.push('LT')
  } else if (options?.GT) {
    args.push('GT')
  }
  if (options?.CH) {
    args.push('CH')
  }
  return args
}

function isPlainKey(key: unknown): key is NodeRedisCommandArgument {
  return typeof key === 'string' || Buffer.isBuffer(key)
}

/** node-redis' `parseZKeysArguments`: numkeys, the keys, then any `WEIGHTS`. */
function zKeysArguments(keys: NodeRedisZKeys): NodeRedisCommandArgument[] {
  if (Array.isArray(keys)) {
    const list = keys as readonly (
      NodeRedisCommandArgument | NodeRedisZKeyAndWeight
    )[]
    const args: NodeRedisCommandArgument[] = [list.length.toString()]
    if (list.length === 0) {
      return args
    }
    if (isPlainKey(list[0])) {
      args.push(...(list as readonly NodeRedisCommandArgument[]))
      return args
    }
    const weighted = list as readonly NodeRedisZKeyAndWeight[]
    args.push(...weighted.map(({ key }) => key), 'WEIGHTS')
    args.push(...weighted.map(({ weight }) => doubleArgument(weight)))
    return args
  }
  if (isPlainKey(keys)) {
    return ['1', keys]
  }
  const { key, weight } = keys as NodeRedisZKeyAndWeight
  return ['1', key, 'WEIGHTS', doubleArgument(weight)]
}

function notify(
  listeners: Set<NodeRedisPubSubListener> | undefined,
  message: string,
  channel: string,
): void {
  if (!listeners) {
    return
  }
  for (const listener of listeners) {
    listener(message, channel)
  }
}

// --- reply decoding --------------------------------------------------------
//
// node-redis leaves most replies untransformed, and the shared RedisValue →
// native JS decoder already matches what it hands back: bulk-string → utf8
// string, integer → number, array/set → array, plus the protocol-dependent
// kinds (map, map-pairs, flat-pairs, double, big-number, boolean) in whichever
// shape the negotiated RESP version puts on the wire. So the curated methods
// are thin coercions over that decoder rather than per-command reply tables —
// uncommon commands get the same native shapes via sendCommand. The exceptions
// are the few curated methods node-redis *does* transform, which decode through
// their own helper: `hGetAll` builds an object on both protocols (#414).

/**
 * How this facade reads a {@link RedisValue}, and therefore where it diverges
 * from `IN_MEMORY_DECODE_OPTIONS`. Exported so a test can assert the two apart —
 * the divergences are deliberate, and a silent re-convergence is the failure
 * mode worth catching.
 */
export const NODE_REDIS_DECODE_OPTIONS: ClientDecodeOptions &
  ReturnNestedErrors = {
  // node-redis decodes a RESP2 `:` integer with plain JS number arithmetic, so
  // it is always a `number` (precision loss past 2^53 included) — never a
  // bigint. Only the RESP3 `(` BIG_NUMBER type yields a bigint.
  narrowBigInt: 'always',
  // node-redis routes a push frame by its type tag and hands listeners only the
  // payload, so the tag is not part of the decoded reply.
  pushShape: 'items',
  // Surface node-redis' own server-error class — `SimpleError`, a subclass of
  // `ErrorReply`, on v5+ — so both `instanceof ErrorReply` (the documented
  // idiom) and the concrete class match real node-redis.
  error: text => new (redisErrors().SimpleError)(text),
  // node-redis decodes an error element of an array or map in place — so
  // `EVAL "return {1, redis.error_reply('x')}" 0` resolves
  // `[1, SimpleError('ERR x')]` — and only a top-level error rejects (#510).
  nestedErrors: 'return',
}

function noop(): void {}

/**
 * Decode a reply the caller hands back whole. `respVersion` is the version the
 * connection served it under — it decides the protocol-dependent shapes — so it
 * is never defaulted.
 */
function decodeReply(
  value: RedisValue,
  respVersion: RespVersion,
  profile: CompatibilityProfile,
): NodeRedisReply {
  return decodeRedisValue(value, {
    ...NODE_REDIS_DECODE_OPTIONS,
    version: respVersion,
    profile,
  })
}

/**
 * Decode a reply a curated method immediately narrows to a scalar (or a flat
 * string array). None of the commands behind those methods replies with a kind
 * whose shape the protocol decides — no `map`, `double`, pair, `boolean` or
 * `big-number` reaches here — so the pinned version is unobservable, and the
 * `asNumber` / `asString` coercions on top of it stay honest.
 *
 * Adding a curated method for a command that *does* reply with one of those:
 * do not reach for this, and do not reach for `decodeReply` either. Ask what
 * node-redis' own `transformReply` for that command produces. Usually it
 * normalises, so the method is protocol-*independent* — real `zScore()` is a
 * number at RESP2 as well as RESP3, and `hGetAll()` an object at both — which
 * means decoding the reply's own kind directly, as {@link decodeMapReply}
 * does, not passing the live version to a decoder that would then follow it.
 */
function decodeScalarReply(value: RedisValue): NodeRedisReply {
  return decodeRedisValue(value, { ...NODE_REDIS_DECODE_OPTIONS, version: 2 })
}

/**
 * Decode a map reply for a curated method that presents it as an object on
 * *both* protocols, the way node-redis' `transformReply` does — see #414.
 * Entries still decode under the connection's own version, so only the
 * container shape is pinned.
 *
 * Nothing else is coerced into an object. An `error` — what a wrong-type key
 * replies with — goes through the ordinary decoder and throws, as real
 * node-redis' `hGetAll()` does at both protocols; swallowed into `{}` it would
 * be indistinguishable from a missing key. Any other kind (a `+QUEUED` after a
 * raw `MULTI`, say) fails loudly the way `asNumber` does, rather than resolving
 * to a string typed as an object.
 */
function decodeMapReply(
  value: RedisValue,
  respVersion: RespVersion,
): { [key: string]: NodeRedisReply } {
  const options = { ...NODE_REDIS_DECODE_OPTIONS, version: respVersion }
  if (value.kind === 'map' || value.kind === 'map-pairs') {
    return decodeRedisMapEntries(value.entries, options)
  }
  if (value.kind === 'error') {
    decodeRedisValue(value, options) // throws the reply's own error
  }
  throw new RedisCommandError(`expected a map reply, got ${value.kind}`)
}

/**
 * Decode a `WITHSCORES` reply into node-redis' `{ value, score }` members, the
 * same objects on both protocols (its `transformSortedSetReply`). Decoding
 * the pairs' own kinds at RESP3 gives exactly that: the member a string and
 * the score a number, `±Infinity` included. An `error` throws the reply's own
 * error; an empty `array` (nothing in range) is no members; anything else
 * fails loudly, as {@link decodeMapReply} does.
 */
function decodeSortedSetReply(value: RedisValue): NodeRedisZMember[] {
  const options = { ...NODE_REDIS_DECODE_OPTIONS, version: 3 as const }
  if (value.kind === 'flat-pairs') {
    return value.entries.map(([member, score]) => ({
      value: String(decodeRedisValue(member, options)),
      score: Number(decodeRedisValue(score, options)),
    }))
  }
  if (value.kind === 'array' && value.items.length === 0) {
    return []
  }
  if (value.kind === 'error') {
    decodeRedisValue(value, options) // throws the reply's own error
  }
  throw new RedisCommandError(`expected a sorted-set reply, got ${value.kind}`)
}

function asNumber(value: RedisValue): number {
  const reply = decodeScalarReply(value)
  if (typeof reply === 'number') {
    return reply
  }
  if (typeof reply === 'bigint') {
    return Number(reply)
  }
  throw new RedisCommandError(`expected an integer reply, got ${typeof reply}`)
}

function asString(value: RedisValue): string {
  const reply = decodeScalarReply(value)
  return typeof reply === 'string' ? reply : String(reply)
}

function asStringOrNull(value: RedisValue): string | null {
  const reply = decodeScalarReply(value)
  if (reply === null) {
    return null
  }
  if (Buffer.isBuffer(reply)) {
    return reply.toString('utf8')
  }
  return typeof reply === 'string' ? reply : String(reply)
}

function asStringArray(value: RedisValue): string[] {
  const reply = decodeScalarReply(value)
  if (!Array.isArray(reply)) {
    return []
  }
  return reply.map(item =>
    Buffer.isBuffer(item) ? item.toString('utf8') : String(item),
  )
}
