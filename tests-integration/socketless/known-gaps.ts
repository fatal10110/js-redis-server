/**
 * Known divergences of the socketless client mocks (#412), applied by
 * `register.ts` when the suites run with `TEST_BACKEND=socketless`
 * (`npm run test:integration:socketless`).
 *
 * Every entry is a case the socketless backend cannot pass *today*. It is a
 * finding, not a waiver: fix the mock in `src/`, then delete the entry.
 * `register.ts` holds the list to that — see its header for the rules (a
 * listed test that passes, fails differently from its `error` pattern, or
 * matches nothing fails the file).
 *
 * `ioredis/**` has no entries: `createIoredisMock` is the real ioredis client
 * over a virtual socket, and the whole suite passes on it (bar the in-process
 * server's own gaps, which `scripts-typed-replies` marks for mock and socketless
 * alike). Everything below is the hand-written node-redis facade
 * (`createNodeRedisMock`), which curates a few dozen of node-redis' methods and
 * signatures — most of these are surface it does not have yet, not wrong
 * replies.
 *
 * Three divergences are not listed per test, because the harness works around
 * them or no suite can observe them through the facade: see
 * {@link FACADE_DEFAULT_PROTOCOL}, {@link FACADE_DUPLICATE_PROMISE} and
 * {@link FACADE_PUBSUB_PROTOCOL}.
 */
import assert from 'node:assert'
import { isDeepStrictEqual } from 'node:util'

export type KnownGap = {
  /** Test file, relative to `tests-integration/` (e.g. `ioredis/multi.test.ts`). */
  file: string
  /**
   * Titles of the listed tests — the test's own title, or its full
   * `Suite > … > title` path when the title alone is ambiguous. Omitted only by
   * a `skip` entry covering the whole file.
   */
  test?: readonly string[]
  /** Why it diverges on the socketless backend. */
  reason: string
  /**
   * `todo` entries: the failure every listed test must fail with — a pattern
   * over the error's message, or a predicate over the error itself. A listed
   * test failing with anything else fails the file.
   */
  error?: RegExp | ((err: unknown) => boolean)
  /**
   * `todo` (default): the test still runs; a matching failure is reported but
   * does not fail the run. `skip`: not run at all — for files (or tests) whose
   * setup the backend cannot provide.
   */
  mode?: 'todo' | 'skip'
}

/**
 * `createNodeRedisMock()` starts on RESP2, while node-redis 5+ (`redis@6`
 * here) defaults to RESP3 and says `HELLO 3` on connect. So out of the box the
 * facade answers ZSCORE with `'2.5'` and HGETALL (via `sendCommand`) with
 * `['f', 'v']` where a default node-redis 6 client gets `2.5` and
 * `{ f: 'v' }`. The socketless harness sends `HELLO 3` to every facade so the
 * suites run on the protocol they run on against mock/real. The divergence
 * itself is pinned by `node-redis/socketless-parity.test.ts`, which asserts
 * today's facade replies — so it fails, loudly, once the facade is fixed.
 * Follow-up for `src/`: default the facade to node-redis' own default.
 */
export const FACADE_DEFAULT_PROTOCOL =
  "createNodeRedisMock() starts on RESP2; node-redis 6's default client negotiates RESP3 (HELLO 3), so default-protocol replies differ (ZSCORE '2.5' vs 2.5, HGETALL flat array vs object)"

/**
 * `NodeRedisMockClient.duplicate()` returns a Promise of an already-usable
 * client; real node-redis returns an unconnected client synchronously (you
 * `connect()` it). The harness's `duplicateNodeRedisClient()` (test-config.ts)
 * absorbs the difference so the pub/sub and keyspace-notification suites reach
 * the facade's pub/sub. Follow-up for `src/`: return the client synchronously
 * and make `connect()` do the work.
 */
export const FACADE_DUPLICATE_PROMISE =
  'NodeRedisMockClient.duplicate() returns a Promise; node-redis returns the unconnected client synchronously'

/**
 * The facade's pub/sub runs on a dedicated session it opens on first
 * subscribe (`ensurePubSub()`), and that session never negotiates RESP3 —
 * not even on a client that sent `HELLO 3` — so a RESP3 facade subscriber
 * still receives RESP2 frames. The listener API (`(message, channel)`) hides
 * the frame shape, so no test through the facade can observe it; the parity
 * suite's facade pub/sub case therefore claims delivery parity only.
 * Follow-up for `src/`: open the pub/sub session at the client's protocol.
 */
export const FACADE_PUBSUB_PROTOCOL =
  "NodeRedisMockClient's pub/sub session (ensurePubSub) always runs RESP2, even after HELLO 3 on the client"

type Cause = { reason: string; error: NonNullable<KnownGap['error']> }

/**
 * The recurring causes, spelled once. Each `error` is the narrowest pattern
 * that still names the cause itself — never a bare assertion failure — so a
 * listed test that starts failing some other way is caught. Where a test
 * asserts on a server error, the facade's actual error text appears inside
 * node:assert's message, which is what these patterns then match.
 */
const CAUSE = {
  clusterSendCommand: {
    reason:
      "calls node-redis' cluster `sendCommand(firstKey, isReadonly, args)`; `NodeRedisMockCluster.sendCommand` only takes `(args)`, so the key is read as the argument list (`undefined` has no length; a key string is spread into characters)",
    error:
      /Cannot read properties of undefined \(reading 'length'\)|ERR unknown command '\{', with args beginning with/,
  },
  clusterTopology: {
    reason:
      "looks up a slot owner through node-redis' `cluster.slots`, which `NodeRedisMockCluster` does not expose",
    error: /cluster\.slots is not available on this cluster client/,
  },
  argumentShapes: {
    reason:
      "passes node-redis' array / object argument forms (`sAdd(key, [members])`, `hSet(key, { … })`, `set(key, v, { expiration })`, …); the facade's curated methods only take positional strings",
    error: /"arguments\[\d+\]" must be of type "string \| Buffer", got object/,
  },
  zRangeOptionsError: {
    reason:
      "the facade's `zRange(key, start, stop)` drops node-redis' options argument (`BY` / `REV` / `LIMIT`) and runs a plain index ZRANGE, which rejects score/lex bounds",
    error: /ERR value is not an integer or out of range/,
  },
  secondClusterClient: {
    reason:
      'needs a second cluster client on the same keyspace (blocking pop + push, WATCH from another connection); `NodeRedisMockCluster` has no `duplicate()` or equivalent',
    error: /second node-redis cluster client on the same keyspace/,
  },
} as const satisfies Record<string, Cause>

/** Skip reasons: setups the backend cannot provide at all. */
const SKIP = {
  topologyInBefore:
    "before() looks up a slot owner through node-redis' `cluster.slots`, which `NodeRedisMockCluster` does not expose",
  secondClusterClient: CAUSE.secondClusterClient.reason,
  flushAllInHook:
    'beforeEach() calls `flushAll()`, which the facade does not have',
  respOption:
    "opens node-redis clients at a chosen RESP version on a TCP port; `createNodeRedisMock()` takes no `RESP` option (RESP3 only via `sendCommand(['HELLO', '3'])`)",
} as const

/** The facade has no such curated method (node-redis' typed command API). */
function missing(...methods: string[]): Cause {
  return {
    reason: `the facade has no ${methods.map(m => `\`${m}()\``).join(', ')}`,
    error: new RegExp(`\\.(?:${methods.join('|')}) is not a function$`),
  }
}

/**
 * The facade's `zRange(key, start, stop)` silently drops node-redis' options
 * argument (`REV` / `BY`) and runs a plain index ZRANGE — wrong results, not an
 * error. The only observable failure is the test's own deep-equal, so each
 * title is pinned to the exact wrong reply it gets (`AssertionError.actual`);
 * any other failure of that test is a different one.
 */
function zRangeWrongResult(actual: readonly string[]): Cause {
  return {
    reason: `the facade's \`zRange(key, start, stop)\` silently drops node-redis' options argument (\`REV\` / \`BY\`) and runs a plain index ZRANGE — wrong results, not an error (it returns ${JSON.stringify(actual)})`,
    error: err =>
      err instanceof assert.AssertionError &&
      isDeepStrictEqual(err.actual, actual),
  }
}

function todo(file: string, cause: Cause, test: readonly string[]): KnownGap {
  return { file, reason: cause.reason, error: cause.error, test }
}

function skip(file: string, reason: string): KnownGap {
  return { file, reason, mode: 'skip' }
}

export const SOCKETLESS_KNOWN_GAPS: readonly KnownGap[] = [
  // Waiters are opened in each test (not in before()), so every test fails
  // on its own with this cause instead of a hook failure hiding them all.
  todo('node-redis/blocking-fifo.test.ts', CAUSE.secondClusterClient, [
    'BLMOVE: 3 waiters on one key are served in the order they blocked',
    'BLMOVE: a write of another type does not wake the waiter',
    'BLMPOP: a waiter woken to find nothing keeps its place in line',
    'BLMPOP: a write of another type does not wake the waiter',
    'BLPOP k1 k2: a type change on k2 keeps the client blocked for k1',
    'BLPOP: 3 waiters on one key are served in the order they blocked',
    'BLPOP: a single multi-value push serves the waiters in the order they blocked',
    'BLPOP: a waiter woken to find nothing keeps its place in line',
    'BLPOP: a write of another type does not wake the waiter',
    'BRPOP: 3 waiters on one key are served in the order they blocked',
    'BZMPOP: 3 waiters on one key are served in the order they blocked',
    'BZMPOP: a waiter woken to find nothing keeps its place in line',
    'BZMPOP: a write of another type does not wake the waiter',
    'BZPOPMIN: 3 waiters on one key are served in the order they blocked',
    'BZPOPMIN: a waiter woken to find nothing keeps its place in line',
    'BZPOPMIN: a write of another type does not wake the waiter',
    'XREAD BLOCK: a write of another type does not wake the waiter',
    'XREAD BLOCK: every waiter on one key is served the new entry',
    'XREADGROUP BLOCK on two streams: deleting the second unblocks it with NOGROUP',
    'XREADGROUP BLOCK: 3 waiters on one key are served in the order they blocked',
    'XREADGROUP BLOCK: DEL unblocks it with NOGROUP',
    'XREADGROUP BLOCK: MULTI; DEL; XADD; EXEC unblocks it with NOGROUP',
    'XREADGROUP BLOCK: MULTI; XGROUP DESTROY; XGROUP CREATE; EXEC keeps it blocked',
    'XREADGROUP BLOCK: PEXPIRE (active expiry) unblocks it with NOGROUP',
    'XREADGROUP BLOCK: RENAME unblocks it with NOGROUP',
    'XREADGROUP BLOCK: UNLINK unblocks it with NOGROUP',
    'XREADGROUP BLOCK: XGROUP DESTROY of another group keeps it blocked',
    'XREADGROUP BLOCK: XGROUP DESTROY unblocks it with NOGROUP',
    'XREADGROUP BLOCK: overwriting the stream (MULTI; DEL; SET; EXEC) unblocks it with WRONGTYPE',
    'XREADGROUP BLOCK: overwriting the stream (SET) unblocks it with WRONGTYPE',
  ]),
  todo('node-redis/cluster-integration.test.ts', CAUSE.clusterSendCommand, [
    'HELLO reports master and replica roles for direct node connections',
    'Lua redis.call and redis.pcall non-local key errors match Redis',
    'READONLY and READWRITE arity errors match Redis',
    'READONLY lets direct replica connections serve readonly commands for master slots',
    'RESET clears READONLY replica mode',
    'direct node connections return MOVED for keys owned by another node',
    'direct replica connections redirect keyed commands to the master',
  ]),
  todo('node-redis/cluster-integration.test.ts', CAUSE.clusterTopology, [
    'CLUSTER NODES reports bus port as client port + 10000',
    'CLUSTER SHARDS returns structured shard metadata',
    'CLUSTER arity and subcommand errors match Redis',
  ]),
  todo(
    'node-redis/command-info-keys.test.ts',
    missing('commandInfo', 'geoPos'),
    [
      'GEOPOS and GEOHASH accept a key alone (arity -2)',
      'reports arity and first/last/step keys like Redis',
    ],
  ),
  todo('node-redis/command-integration.test.ts', CAUSE.clusterSendCommand, [
    'COMMAND COUNT and HELP expose the command surface',
    'COMMAND DOCS ECHO reports summary, since, group, complexity and arguments',
    'COMMAND DOCS argument flags use RESP status strings',
    'COMMAND DOCS returns documentation entries and skips unknown commands',
    'COMMAND GETKEYS and GETKEYSANDFLAGS use command key extraction',
    'COMMAND INFO ECHO reports arity, flags and categories like Redis',
    'COMMAND INFO returns Redis command metadata and nulls for unknown commands',
    'COMMAND LIST returns names and supports Redis FILTERBY variants',
    'COMMAND errors match Redis',
  ]),
  todo('node-redis/commands.test.ts', missing('mGet'), [
    'cross slot error',
    'returns multiple key values',
  ]),
  skip('node-redis/config-integration.test.ts', SKIP.topologyInBefore),
  skip('node-redis/connection-integration.test.ts', SKIP.topologyInBefore),
  todo('node-redis/copy-integration.test.ts', missing('copy', 'select'), [
    'copies a value to a new destination and returns 1',
    'copies across databases with the DB option',
    'errors on an unknown option',
    'errors when DB has no value',
    'errors when source and destination are the same key',
    'errors when the DB index is not an integer',
    'errors when the DB index is out of range',
    'overwrites an existing destination with REPLACE and returns 1',
    'rejects keys that hash to different slots with CROSSSLOT',
    'returns 0 when destination already exists without REPLACE',
    'returns 0 when the source key does not exist',
    'same key name in a different DB is allowed (not "same objects")',
  ]),
  todo('node-redis/copy-integration.test.ts', CAUSE.argumentShapes, [
    'REPLACE overwrites the destination TTL with the source TTL (none)',
    'copies the TTL together with the value',
    'works on any value type (list)',
  ]),
  todo('node-redis/flush-async-sync.test.ts', missing('flushAll', 'flushDb'), [
    'FLUSHALL accepts ASYNC and clears all databases',
    'FLUSHALL accepts SYNC',
    'FLUSHALL with no argument still works',
    'FLUSHDB accepts ASYNC and clears the keyspace',
    'FLUSHDB accepts SYNC',
    'FLUSHDB with no argument still works',
  ]),
  todo('node-redis/geo/core.test.ts', missing('geoAdd', 'geoPos'), [
    'GEO commands on missing key return nil/empty per Redis semantics',
    'GEO commands reject wrong type keys',
    'GEOADD NX/XX/CH option flags match Redis',
    'GEOADD adds members and GEOPOS/GEODIST/GEOHASH read them back',
  ]),
  todo('node-redis/geo/core.test.ts', CAUSE.clusterSendCommand, [
    'GEOADD option/argument errors match Redis',
  ]),
  todo('node-redis/geo/search.test.ts', missing('geoAdd', 'geoSearch'), [
    'GEORADIUS (deprecated) matches GEOSEARCH-equivalent behavior',
    'GEORADIUSBYMEMBER matches GEOSEARCH FROMMEMBER-equivalent behavior',
    'GEORADIUSBYMEMBER_RO rejects STORE',
    'GEORADIUS_RO rejects STORE',
    'GEOSEARCH COUNT / COUNT ANY semantics',
    'GEOSEARCH FROMLONLAT BYRADIUS with WITHCOORD/WITHDIST/WITHHASH',
    'GEOSEARCH FROMMEMBER BYBOX matches real Redis component-wise box test',
    'GEOSEARCH argument and edge-case errors match Redis',
    'GEOSEARCH on missing key returns empty array',
    'GEOSEARCH rejects wrong type key',
  ]),
  todo('node-redis/geo/search.test.ts', CAUSE.argumentShapes, [
    'GEORADIUS STORE / STOREDIST write results, reject combining with WITH*',
    'GEOSEARCHSTORE rejects WITH* options',
    'GEOSEARCHSTORE stores geohash score, STOREDIST stores distance',
  ]),
  todo('node-redis/hash/basic.test.ts', missing('hExists', 'hLen', 'hSetNX'), [
    'HEXISTS command',
    'HLEN command',
    'HSETNX command',
  ]),
  todo('node-redis/hash/basic.test.ts', CAUSE.argumentShapes, [
    'HGETALL command',
    'HKEYS and HVALS commands',
    'HMSET and HMGET commands',
    'HSET and HGET commands',
  ]),
  todo('node-redis/hash/basic.test.ts', CAUSE.clusterTopology, [
    'HGETALL works over a RESP3 connection (HELLO 3)',
    'HRANDFIELD command matches Redis',
  ]),
  todo('node-redis/hash/del.test.ts', missing('hDel'), [
    'HDEL on a missing key does not create an empty hash',
  ]),
  todo('node-redis/hash/del.test.ts', CAUSE.argumentShapes, ['HDEL command']),
  todo('node-redis/hash/del.test.ts', CAUSE.clusterTopology, [
    'HGETDEL errors match Redis',
    'HGETDEL returns values and deletes fields',
  ]),
  todo('node-redis/hash/errors-workflow.test.ts', missing('hIncrBy'), [
    'Hash commands workflow - Shopping Cart',
  ]),
  todo('node-redis/hash/errors-workflow.test.ts', CAUSE.argumentShapes, [
    'Hash command errors match Redis',
    'Hash commands workflow - User Profile',
  ]),
  todo('node-redis/hash/field-expire.test.ts', CAUSE.clusterTopology, [
    'HEXPIRE arg-content errors stay inline inside MULTI/EXEC',
    'HEXPIRE condition flags match Redis',
    'HEXPIRE deletes the key when every field expires',
    'HEXPIRE errors match Redis',
    'HEXPIRE family sets and lazily expires hash fields',
    'HSCAN omits expired hash fields',
    'hash writes clear or preserve field TTLs like Redis',
  ]),
  todo('node-redis/hash/field-persist.test.ts', CAUSE.clusterTopology, [
    'HPERSIST, HTTL and HPTTL errors match Redis',
    'HPERSIST, HTTL and HPTTL handle missing keys',
    'HPERSIST, HTTL and HPTTL report and clear hash field TTLs',
    'HTTL rounds hash-field TTLs up to the next second, unlike key TTL',
  ]),
  todo('node-redis/hash/hgetex.test.ts', CAUSE.clusterTopology, [
    'HGETEX errors match Redis',
    'HGETEX returns values and updates field TTLs like Redis',
  ]),
  todo('node-redis/hash/hsetex.test.ts', CAUSE.clusterTopology, [
    'HSETEX FNX/FXX conditions match Redis',
    'HSETEX errors match Redis',
    'HSETEX sets fields and expiration like Redis',
  ]),
  todo('node-redis/hash/incr.test.ts', missing('hIncrBy', 'hIncrByFloat'), [
    'HINCRBY command',
    'HINCRBY respects Redis 64-bit signed integer range',
    'HINCRBYFLOAT command',
  ]),
  todo('node-redis/info-standalone.test.ts', missing('info'), [
    'INFO cluster omits cluster-only state fields for standalone servers',
    'INFO replication exposes Redis-compatible replication identifiers',
    'INFO still serves a known section after an unknown one',
    'INFO unknown section is case-insensitive and still returns empty',
    'INFO with an unknown section returns an empty bulk string, not an error',
  ]),
  todo('node-redis/key/commands.test.ts', missing('type'), ['TYPE command']),
  todo('node-redis/key/commands.test.ts', CAUSE.argumentShapes, [
    'DBSIZE command',
    'EXISTS command',
    'Key command errors and past expiration match Redis',
    'UNLINK command removes keys and returns the deleted count',
  ]),
  todo('node-redis/key/commands.test.ts', CAUSE.clusterTopology, [
    'TOUCH command counts live keys without mutating keyspace',
  ]),
  todo('node-redis/key/expire.test.ts', missing('expireAt', 'persist'), [
    'EXPIRE and EXPIREAT commands',
    'PERSIST removes expiration and EXPIRE 0 deletes the key',
    'TTL integration with EXPIRE and EXPIREAT',
  ]),
  todo('node-redis/key/expire.test.ts', CAUSE.argumentShapes, [
    'EXPIRETIME and PEXPIRETIME return absolute expiry, -1, -2',
    'TTL rounds to nearest second like real Redis (#59)',
  ]),
  todo('node-redis/key/expire.test.ts', CAUSE.clusterTopology, [
    'EXPIRE family supports conditional expiry options',
    'EXPIREAT/PEXPIREAT past timestamp respects conditional flags (#72)',
  ]),
  todo('node-redis/key/keyspace-empty-cleanup.test.ts', missing('hDel'), [
    'emptying a hash via HDEL deletes the key (no phantom empty hash persists)',
  ]),
  todo('node-redis/key/keyspace-empty-cleanup.test.ts', CAUSE.clusterTopology, [
    'emptying an EXISTING watched collection still invalidates the WATCH',
    'no-op HDEL on a non-existent key must not invalidate a WATCH on that key',
    'no-op SREM on a non-existent key must not invalidate a WATCH on that key',
  ]),
  // The facade's own pub/sub runs here (via duplicateNodeRedisClient()); what
  // stops these is the rest of its surface.
  todo(
    'node-redis/key/keyspace-notification-names.test.ts',
    missing('configSet'),
    [
      '8.x hash-field commands publish hdel / hexpire / hpersist',
      'BLPOP / BRPOP publish lpop / rpop (#446)',
      'LMOVE / BLMOVE / RPOPLPUSH publish the destination push, then the source pop (#446)',
      'LMPOP / BLMPOP publish lpop / rpop by direction (#446)',
      'SMOVE publishes srem on the source and sadd on the destination (#446)',
      'XGROUP subcommands publish xgroup-<subcommand> (#381)',
      'XREADGROUP / XCLAIM / XAUTOCLAIM publish xgroup-createconsumer for a new consumer',
      'ZMPOP / BZMPOP / BZPOPMIN / BZPOPMAX publish zpopmin / zpopmax (#446)',
      'a STORE over an existing destination publishes one event',
      'a blocked pop served by a push publishes the push, then the pop (#446)',
      'expired hash fields are published as hexpired, never as the reading command',
      'expired hash fields are removed by active expiry, with no access to the key',
    ],
  ),
  todo(
    'node-redis/key/keyspace-notifications.test.ts',
    CAUSE.argumentShapes,
    // pSubscribe([patterns], listener): the facade takes one pattern string.
    ['publishes set keyspace and keyevent notifications'],
  ),
  todo('node-redis/key/keyspace-notifications.test.ts', missing('configSet'), [
    'CONFIG normalizes flags and rejects invalid characters',
    'a parked blocking command does not name writes into its database (#444)',
    'blocking commands resumed out of nesting order leave no stale name (#444)',
    'delivers nothing when notify-keyspace-events is disabled',
    'does not name a cross-database write after an earlier SELECT',
    'emits the type-specific event before del when the last element goes (#379)',
    'gates events by configured class',
    'names write events after the originating command',
    'publishes del, expire and persist generic notifications',
    'publishes expired event from active expiry without a forcing read',
    'publishes expired event when a key lazily expires',
    'stream group metadata commands notify without dirtying WATCH (#379)',
    'translates RENAME into rename_from and rename_to',
  ]),
  todo('node-redis/key/workflow.test.ts', missing('expireAt'), [
    'Expiration workflow - Cache with Scheduled Invalidation',
  ]),
  todo('node-redis/key/workflow.test.ts', CAUSE.argumentShapes, [
    'DBSIZE workflow - Database Monitoring and Capacity Planning',
    'Key commands workflow - Cache Validation',
    'Key commands workflow - Data Type Validation',
    'Key commands workflow - Multi-tenant Data Isolation',
  ]),
  skip('node-redis/list/blmove.test.ts', SKIP.secondClusterClient),
  skip('node-redis/list/blocking.test.ts', SKIP.secondClusterClient),
  todo('node-redis/list/commands.test.ts', missing('lLen'), [
    'LLEN command',
    'List commands workflow - Undo Stack',
  ]),
  todo('node-redis/list/commands.test.ts', CAUSE.argumentShapes, [
    'LINDEX command',
    'LPOP and RPOP commands',
    'LPUSH and RPUSH commands',
    'LRANGE command',
    'LREM command',
    'LSET command',
    'LTRIM command',
    'List commands workflow - Chat Messages',
    'List commands workflow - Task Queue',
  ]),
  todo('node-redis/list/commands.test.ts', CAUSE.clusterTopology, [
    'LPOP and RPOP support count argument',
    'List command errors match Redis',
  ]),
  todo('node-redis/list/linsert.test.ts', CAUSE.clusterTopology, [
    'error paths match Redis',
    'inserts before and after the first matching pivot',
    'returns Redis-compatible values for missing keys and pivots',
  ]),
  skip('node-redis/list/lmpop-blmpop.test.ts', SKIP.secondClusterClient),
  todo('node-redis/list/lpos-lmove.test.ts', CAUSE.clusterTopology, [
    'LMOVE error paths match Redis',
    'LMOVE moves elements between list ends',
    'LMOVE on missing source returns nil and deletes emptied source',
    'LMOVE rotates a list onto itself',
    'LPOS error and edge paths match Redis',
    'LPOS finds matches with RANK, COUNT and MAXLEN',
  ]),
  todo('node-redis/move-integration.test.ts', missing('select'), [
    'errors when moving to the current database',
    'errors when the destination DB index is not an integer',
    'errors when the destination DB index is out of range',
    'moves a key to another database and removes it from the source',
    'preserves TTL on the moved key',
    'returns 0 and leaves both databases unchanged when destination exists',
    'returns 0 when the source key is missing',
  ]),
  todo('node-redis/move-integration.test.ts', CAUSE.clusterTopology, [
    'rejects MOVE in cluster mode',
  ]),
  todo('node-redis/multi.test.ts', missing('multi'), [
    'returns execution errors from EXEC replies',
  ]),
  todo('node-redis/multi.test.ts', CAUSE.clusterTopology, [
    'aborts the whole transaction (EXECABORT) when a command fails to queue',
    'runtime command errors are surfaced via MultiErrorReply',
  ]),
  todo('node-redis/multi.test.ts', CAUSE.secondClusterClient, [
    'Queue commands before execution without piplining',
  ]),
  skip('node-redis/randomkey-integration.test.ts', SKIP.flushAllInHook),
  todo('node-redis/scan/keys-scan.test.ts', CAUSE.clusterSendCommand, [
    'scan COUNT errors match Redis',
    'scan MATCH treats patterns as raw bytes',
  ]),
  todo('node-redis/scan/keys-scan.test.ts', CAUSE.clusterTopology, [
    'HSCAN NOVALUES returns only matching fields',
    'KEYS and top-level SCAN MATCH treat patterns as raw bytes',
    'KEYS and top-level SCAN support MATCH and TYPE filters',
    'KEYS matches Redis glob pattern semantics',
    'SCAN MATCH advances across non-matching COUNT batches',
    'SCAN MATCH handles interleaved matching and non-matching batches',
    'SCAN TYPE advances across non-matching type COUNT batches',
  ]),
  todo('node-redis/scan/typed-scan.test.ts', missing('zScan'), [
    'ZSCAN iterates with COUNT until cursor 0',
  ]),
  todo('node-redis/scan/typed-scan.test.ts', CAUSE.argumentShapes, [
    'HSCAN iterates with COUNT until cursor 0',
    'SSCAN iterates with COUNT until cursor 0',
    'keyed scan MATCH advances across non-matching COUNT batches',
  ]),
  todo('node-redis/scripts-noscript.test.ts', missing('clientSetName'), [
    'RESET and QUIT are refused from scripts',
    'a refused CLIENT SETNAME leaves the connection name untouched',
  ]),
  skip('node-redis/scripts-typed-replies.test.ts', SKIP.respOption),
  todo('node-redis/select-multi.test.ts', missing('select'), [
    'a queued SELECT switches the DB for later commands in the same EXEC',
  ]),
  todo('node-redis/set/core.test.ts', CAUSE.argumentShapes, [
    'SADD and SCARD commands',
    'SISMEMBER command',
    'SMEMBERS command',
    'SPOP command',
    'SPOP command with count',
    'SRANDMEMBER command',
    'SREM command',
    'Set command errors match Redis',
  ]),
  todo('node-redis/set/core.test.ts', CAUSE.clusterTopology, [
    'SMISMEMBER command matches Redis',
  ]),
  todo('node-redis/set/setops.test.ts', CAUSE.argumentShapes, [
    'SDIFF command',
    'SINTER command',
    'SMOVE command',
    'SUNION command',
  ]),
  todo('node-redis/set/setops.test.ts', CAUSE.clusterTopology, [
    'SINTERCARD command matches Redis',
  ]),
  todo('node-redis/set/workflow.test.ts', CAUSE.argumentShapes, [
    'Set commands workflow - Content Categories',
    'Set commands workflow - Online Users',
    'Set commands workflow - User Tags System',
  ]),
  todo('node-redis/sharded-pubsub-cluster.test.ts', missing('sUnsubscribe'), [
    'routes shard subscriptions and publishes by channel slot',
  ]),
  todo('node-redis/sharded-pubsub-cluster.test.ts', CAUSE.clusterSendCommand, [
    'rejects sharded Pub/Sub commands routed to the wrong slot',
  ]),
  todo('node-redis/sort-integration.test.ts', CAUSE.clusterTopology, [
    'SORT ALPHA BY missing weights keeps the source order',
    'SORT ALPHA BY orders a missing weight before an empty one',
    'SORT ALPHA DESC keeps tied elements in load order',
    'SORT ALPHA DESC reverses lexicographic order',
    'SORT ALPHA sorts lexicographically',
    'SORT BY a constant pattern skips sorting whatever slot it hashes to',
    'SORT BY a glob hash-tagged to an untagged source key is allowed',
    'SORT BY nosort skips sorting in cluster mode',
    'SORT BY orders by external keys and GET returns pattern values',
    'SORT DESC reverses numeric order',
    'SORT GET # returns source elements and missing pattern values as null',
    'SORT GET with a constant pattern yields nil for every element',
    'SORT LIMIT offset count paginates',
    'SORT LIMIT offset past the end returns empty',
    'SORT LIMIT rejects a non-integer bound',
    'SORT LIMIT with negative count returns all remaining',
    'SORT STORE with an empty result deletes the destination',
    'SORT STORE writes the result as a list and returns its length',
    'SORT against a string key fails with WRONGTYPE',
    'SORT breaks equal numeric weights lexicographically',
    'SORT errors inside MULTI are queued and surface in EXEC',
    'SORT finds STORE the way sortGetKeys() does',
    'SORT force-sorts a set with a constant BY when the order must be reproducible',
    'SORT keeps duplicate list elements',
    'SORT numerically ascending by default',
    'SORT on a missing key returns an empty array',
    'SORT reaches a denied pattern before a later option fails to parse',
    'SORT reads an integer-only set in ascending numeric order',
    'SORT rejects BY or GET patterns that hash to a different slot',
    'SORT reports the first denied BY/GET option in argument order',
    'SORT sorts a set numerically',
    'SORT sorts a zset by member, not score',
    'SORT sorts floats numerically',
    'SORT treats a non-string weight or GET key as missing',
    'SORT treats an empty-string element as numeric zero',
    'SORT treats option names in value position as values',
    'SORT with a constant BY keeps a zset rank tie-break by raw bytes',
    'SORT with a constant BY reads a zset in rank order',
    'SORT with a constant BY reads the source backwards for DESC',
    'SORT with an unknown option fails with a syntax error',
    'SORT with no key fails with wrong number of arguments',
    'SORT with several BY options: a constant one disables sorting, the last glob weighs',
    'SORT without ALPHA rejects non-numeric elements',
    'SORT_RO rejects STORE with a syntax error',
    'SORT_RO sorts like SORT',
    'SORT_RO supports BY and GET external patterns',
  ]),
  todo('node-redis/stream/add.test.ts', missing('xAdd'), [
    'XADD * generates monotonically increasing ids',
    'XADD <ms>-* auto-increments the sequence',
    'XADD rejects 0-0 and invalid ids',
    'XADD rejects ids equal to or smaller than the top item',
    'XADD with explicit ids and XLEN',
    'stream commands reject keys holding another type',
  ]),
  todo('node-redis/stream/add.test.ts', CAUSE.clusterTopology, [
    'XADD LIMIT validates approximate trim syntax before id',
    'XADD MAXLEN trims oldest entries after append',
    'XADD MAXLEN with ~ accepts LIMIT count before generated id',
    'XADD NOMKSTREAM appends to an existing stream normally',
    'XADD NOMKSTREAM returns null when key does not exist',
    'XADD handles auto-generated ids when the sequence overflows',
  ]),
  todo('node-redis/stream/claim-validation.test.ts', missing('xAdd'), [
    'XAUTOCLAIM accepts interval start ids',
    'XAUTOCLAIM rejects COUNT outside 1.. and creates no consumer',
    'XAUTOCLAIM validates its arguments before the key',
    'XCLAIM clamps out-of-range times instead of rejecting them',
    'XCLAIM parses ids up to the first non-id, then options',
    'XINFO CONSUMERS inactive stays -1 until a consumer gets entries',
  ]),
  todo('node-redis/stream/group.test.ts', CAUSE.clusterTopology, [
    'XCLAIM and XAUTOCLAIM transfer pending stream entries',
    'XGROUP MKSTREAM and SETID control group delivery position',
    'XGROUP creates, mutates, and destroys consumer groups',
    'XINFO STREAM FULL defaults to 10 stream entries and PEL rows',
    'XINFO reports stream, group, and consumer metadata',
    'XREADGROUP history keeps deleted pending entries visible',
    'XREADGROUP history returns an empty per-key list for consumers with no pending entries',
    'stream consumer group commands report Redis-compatible errors',
  ]),
  todo('node-redis/stream/range.test.ts', missing('xAdd', 'xLen'), [
    'XLEN and XRANGE on a missing key return empty results',
    'XRANGE honors exclusive bounds and COUNT',
    'XRANGE returns entries within an inclusive range',
    'XREVRANGE returns entries in descending order',
  ]),
  todo('node-redis/stream/range.test.ts', CAUSE.clusterTopology, [
    'XRANGE and XREVRANGE reject non-integer COUNT values',
  ]),
  todo('node-redis/stream/read.test.ts', CAUSE.clusterTopology, [
    'XREAD COUNT limits the number of returned entries',
    'XREAD from multiple streams on same slot returns combined results',
    'XREAD on a missing key returns null',
    'XREAD parses entries over a default RESP3 connection',
    'XREAD returns entries after the given id',
    'XREAD returns null when no new entries exist for the given id',
    'XREAD with $ id returns null (no entries after current last)',
    'XREAD with + id returns the last entry from each stream',
  ]),
  todo('node-redis/stream/setid.test.ts', CAUSE.clusterTopology, [
    'XSETID ENTRIESADDED updates XINFO STREAM metadata',
    'XSETID MAXDELETEDID updates XINFO STREAM metadata',
    'XSETID rejects lower ids, invalid options, and wrong types',
    'XSETID sets last-generated-id and advances generated XADD ids',
  ]),
  todo('node-redis/stream/trim.test.ts', missing('xAdd', 'xDel', 'xTrim'), [
    'XDEL on a missing key returns 0',
    'XDEL removes entries, keeps the empty stream, and retains last id',
    'XTRIM MAXLEN no-op when stream is within limit',
    'XTRIM MAXLEN removes oldest entries and returns removed count',
    'XTRIM on missing key returns 0 without creating it',
  ]),
  todo('node-redis/stream/trim.test.ts', CAUSE.clusterTopology, [
    'XTRIM LIMIT validates approximate trim syntax',
    'XTRIM MAXLEN with ~ (approximate) does not exact-trim tiny streams',
    'XTRIM MAXLEN with ~ accepts LIMIT count',
    'XTRIM MINID removes entries with id below threshold',
  ]),
  todo('node-redis/string/bitmap-offset-limit.test.ts', missing('configSet'), [
    'lowering proto-max-bulk-len lowers the BITFIELD ceiling',
    'lowering proto-max-bulk-len lowers the SETBIT/GETBIT ceiling',
    'raising the limit past 512MB raises the read ceiling but not the write ceiling',
    'the ceiling follows the setting back up',
  ]),
  skip('node-redis/string/bitmap.test.ts', SKIP.topologyInBefore),
  todo(
    'node-redis/string/core.test.ts',
    missing('append', 'getSet', 'mGet', 'mSetNX', 'strLen'),
    [
      'APPEND command',
      'GETSET command',
      'MGET command',
      'MGET cross-slot error',
      'MGET returns null for keys holding non-string values',
      'MSETNX command',
      'STRLEN command',
      'String commands workflow',
    ],
  ),
  todo('node-redis/string/core.test.ts', CAUSE.clusterTopology, [
    'SUBSTR aliases GETRANGE',
    'String numeric and expiration errors match Redis',
  ]),
  skip('node-redis/string/hyperloglog.test.ts', SKIP.topologyInBefore),
  todo(
    'node-redis/string/incr.test.ts',
    missing('decr', 'incrBy', 'incrByFloat'),
    [
      'INCR and DECR commands',
      'INCRBY and DECRBY commands',
      'INCRBYFLOAT command',
    ],
  ),
  todo('node-redis/string/incr.test.ts', CAUSE.clusterTopology, [
    'INCR/INCRBY/DECR/DECRBY operate over the full int64 range',
    'INCRBYFLOAT distinguishes invalid-float from NaN/Infinity result (#56)',
  ]),
  todo(
    'node-redis/string/proto-max-bulk-len.test.ts',
    missing('append', 'configGet', 'configSet', 'getRange', 'setRange'),
    [
      'APPEND on a missing key is not size-checked',
      'CONFIG GET reports the default proto-max-bulk-len',
      'CONFIG SET accepts every Redis memory-unit suffix',
      'CONFIG SET rejects a proto-max-bulk-len below the 1MB minimum',
      'CONFIG SET rejects a proto-max-bulk-len that is not a memory value',
      'CONFIG SET reports an empty proto-max-bulk-len as out of range',
      'GETRANGE clamps a resolved-negative end up to 0',
      'GETRANGE is never size-checked, however large the requested range',
      'SETRANGE and APPEND honour a lowered proto-max-bulk-len',
      'SETRANGE leaves an existing value untouched when the result would be too large',
      'SETRANGE rejects a negative offset before checking the limit',
      'SETRANGE rejects an offset+length beyond proto-max-bulk-len and creates no key',
      'SETRANGE reports WRONGTYPE before the size check',
      'SETRANGE with an empty value skips the limit check and creates no key',
    ],
  ),
  todo('node-redis/string/set.test.ts', CAUSE.argumentShapes, [
    'SET with EX option',
    'SET with GET option',
    'SET with NX option - key does not exist',
    'SET with NX option - key exists',
    'SET with PX option',
    'SET with XX option - key does not exist',
    'SET with XX option - key exists',
    'SET with multiple options',
  ]),
  todo('node-redis/string/set.test.ts', CAUSE.clusterTopology, [
    'SET KEEPTTL preserves the existing expiration',
    'SET and GET wrong-type and syntax errors match Redis',
  ]),
  todo(
    'node-redis/time-lastsave-integration.test.ts',
    missing('lastSave', 'time'),
    [
      'LASTSAVE returns a Unix timestamp integer not in the future',
      'TIME returns [seconds, microseconds] close to current time',
    ],
  ),
  todo('node-redis/watch.test.ts', CAUSE.clusterTopology, [
    'EXEC should clear watched keys',
    'WATCH inside MULTI should return error',
    'WATCH should abort transaction if watched key is modified',
    'WATCH should allow transaction if watched key is not modified',
    'WATCH should allow watching multiple keys in the same slot',
    'WATCH should reject multiple keys in different slots',
  ]),
  todo(
    'node-redis/zset/byte-order.test.ts',
    missing('zRangeByScore', 'zRank'),
    [
      'ZRANGEBYSCORE breaks equal-score ties by raw byte order',
      'ZRANK reflects raw byte order for equal scores',
    ],
  ),
  skip('node-redis/zset/bzpopmin-bzpopmax.test.ts', SKIP.secondClusterClient),
  todo(
    'node-redis/zset/core.test.ts',
    missing('zCard', 'zIncrBy', 'zRem', 'zScore'),
    [
      'ZADD and ZCARD commands',
      'ZADD option flags match Redis',
      'ZINCRBY command',
      'ZREM command',
      'ZSCORE command',
    ],
  ),
  todo('node-redis/zset/core.test.ts', CAUSE.argumentShapes, [
    'Sorted set command errors match Redis',
    'sorted set commands accept and return Redis infinity score tokens',
  ]),
  todo('node-redis/zset/core.test.ts', CAUSE.clusterSendCommand, [
    'ZADD option syntax errors match Redis',
  ]),
  todo(
    'node-redis/zset/double-format.test.ts',
    missing('zIncrBy', 'zRangeWithScores', 'zScore'),
    [
      'WITHSCORES parses back to the stored scores',
      'ZINCRBY parses back to the new score',
      'ZSCORE / ZMSCORE parse back to the stored score',
    ],
  ),
  todo(
    'node-redis/zset/lex.test.ts',
    missing('zLexCount', 'zRangeByLex', 'zRemRangeByLex'),
    [
      'ZLEXCOUNT counts members within lex bounds',
      'ZRANGEBYLEX honors LIMIT offset count',
      'ZRANGEBYLEX returns members within lex bounds',
      'ZREMRANGEBYLEX removes members within lex bounds and returns count',
      'invalid lex bound returns "not valid string range item" error',
      'lex commands on a missing key return empty/zero',
      'lex commands on a wrong-type key return WRONGTYPE',
      'lex ordering uses raw byte comparison, not locale',
    ],
  ),
  todo('node-redis/zset/lex.test.ts', CAUSE.clusterSendCommand, [
    'ZRANGEBYLEX validates LIMIT clause',
    'lex commands reject wrong arity',
  ]),
  todo('node-redis/zset/lex.test.ts', CAUSE.zRangeOptionsError, [
    'ZREVRANGEBYLEX returns members in reverse lex order (max then min)',
  ]),
  todo(
    'node-redis/zset/modern-range.test.ts',
    missing(
      'zRandMember',
      'zRandMemberCount',
      'zRandMemberCountWithScores',
      'zRangeStore',
      'zRangeWithScores',
      'zmScore',
    ),
    [
      'ZMSCORE on a missing key returns all nil',
      'ZMSCORE on a wrong-type key returns WRONGTYPE',
      'ZMSCORE returns scores for present members, nil for missing',
      'ZRANDMEMBER WITHSCORES returns member/score pairs',
      'ZRANDMEMBER on a missing key returns nil / empty array',
      'ZRANDMEMBER on a wrong-type key returns WRONGTYPE',
      'ZRANDMEMBER with count 0 returns empty array',
      'ZRANDMEMBER with count larger than cardinality returns all members',
      'ZRANDMEMBER with negative count allows repeats and matches |count| length',
      'ZRANDMEMBER with positive count returns distinct members',
      'ZRANDMEMBER without count returns one existing member',
      'ZRANGE legacy index form returns members in order',
      'ZRANGESTORE stores an index range and overwrites destination',
    ],
  ),
  todo('node-redis/zset/modern-range.test.ts', CAUSE.argumentShapes, [
    'ZRANGESTORE deletes destination when source is missing or range is empty',
    'ZRANGESTORE rejects invalid syntax and wrong-type sources',
    'ZRANGESTORE supports BYSCORE and BYLEX ranges',
  ]),
  todo('node-redis/zset/modern-range.test.ts', CAUSE.clusterSendCommand, [
    'ZMSCORE rejects wrong arity',
    'ZRANDMEMBER with non-integer count errors',
    'ZRANGE rejects invalid option combinations',
  ]),
  todo('node-redis/zset/modern-range.test.ts', CAUSE.clusterTopology, [
    'ZRANGESTORE rejects destination and source keys from different slots',
  ]),
  todo('node-redis/zset/modern-range.test.ts', CAUSE.zRangeOptionsError, [
    'ZRANGE BYLEX REV takes bounds as max min and reverses',
    'ZRANGE BYLEX filters by lex bounds',
    'ZRANGE BYSCORE filters by score bounds',
    'ZRANGE on a missing key returns empty array',
  ]),
  todo('node-redis/zset/modern-range.test.ts', zRangeWrongResult([]), [
    'ZRANGE BYSCORE REV takes bounds as max min and reverses',
  ]),
  todo(
    'node-redis/zset/modern-range.test.ts',
    zRangeWrongResult(['a', 'b', 'c']),
    ['ZRANGE REV reverses the index ordering'],
  ),
  todo(
    'node-redis/zset/range.test.ts',
    missing('zRangeByScore', 'zRangeWithScores', 'zRank'),
    [
      'ZRANGE command',
      'ZRANK and ZREVRANK commands',
      'score range commands support infinite and exclusive bounds',
    ],
  ),
  todo('node-redis/zset/range.test.ts', CAUSE.argumentShapes, [
    'ZRANK and ZREVRANK WITHSCORE option',
  ]),
  todo(
    'node-redis/zset/range.test.ts',
    zRangeWrongResult(['one', 'two', 'three']),
    ['ZREVRANGE command'],
  ),
  todo(
    'node-redis/zset/score-range.test.ts',
    missing(
      'zRangeByScore',
      'zRangeByScoreWithScores',
      'zRangeWithScores',
      'zRemRangeByRank',
    ),
    [
      'ZRANGEBYSCORE LIMIT negative count returns all remaining',
      'ZRANGEBYSCORE LIMIT negative offset returns empty',
      'ZRANGEBYSCORE LIMIT offset count paginates',
      'ZRANGEBYSCORE WITHSCORES and LIMIT in either order',
      'ZRANGEBYSCORE WITHSCORES returns member/score pairs',
      'ZRANGEBYSCORE exclusive bound with LIMIT',
      'ZRANGEBYSCORE on wrong type rejects WRONGTYPE',
      'ZRANGEBYSCORE rejects non-float bound',
      'ZREMRANGEBYRANK on missing key returns 0',
      'ZREMRANGEBYRANK on wrong type rejects WRONGTYPE',
      'ZREMRANGEBYRANK removes members in rank range',
      'ZREMRANGEBYRANK removing all members deletes the key',
      'ZREMRANGEBYRANK supports negative ranks',
      'ZREVRANGEBYSCORE returns descending range with max/min order',
    ],
  ),
  todo('node-redis/zset/score-range.test.ts', CAUSE.clusterSendCommand, [
    'ZRANGEBYSCORE LIMIT with non-integer rejects',
    'ZRANGEBYSCORE LIMIT without offset/count rejects with syntax error',
    'ZRANGEBYSCORE rejects wrong arity',
    'ZREMRANGEBYRANK rejects non-integer rank',
    'ZREMRANGEBYRANK rejects wrong arity',
  ]),
  todo('node-redis/zset/score-range.test.ts', CAUSE.zRangeOptionsError, [
    'ZREVRANGEBYSCORE exclusive bounds',
    'ZREVRANGEBYSCORE on missing key returns empty',
    'ZREVRANGEBYSCORE rejects non-float bound',
    'ZREVRANGEBYSCORE supports LIMIT',
  ]),
  todo('node-redis/zset/setops.test.ts', CAUSE.clusterTopology, [
    'ZDIFF rejects WEIGHTS (diff has no weights/aggregate)',
    'ZDIFF returns members of the first set not in the rest',
    'ZDIFFSTORE stores the difference and returns its cardinality',
    'ZINTER returns the intersection WITHSCORES',
    'ZINTERCARD counts the intersection and honors LIMIT',
    'ZINTERCARD rejects a negative LIMIT',
    'ZINTERCARD rejects numkeys <= 0 with the input-key error',
    'ZINTERSTORE keeps only common members, summing scores',
    'ZINTERSTORE with empty result deletes the destination key',
    'ZUNION propagates WRONGTYPE for a non-zset/non-set source',
    'ZUNION rejects numkeys <= 0 with the input-key error',
    'ZUNION rejects wrong arity',
    'ZUNION returns the union, with and without WITHSCORES',
    'ZUNIONSTORE applies WEIGHTS before aggregating',
    'ZUNIONSTORE honors AGGREGATE MIN and MAX',
    'ZUNIONSTORE rejects WITHSCORES (store variants have no scores option)',
    'ZUNIONSTORE rejects a WEIGHTS count mismatch',
    'ZUNIONSTORE rejects a non-float weight',
    'ZUNIONSTORE rejects a non-integer numkeys',
    'ZUNIONSTORE rejects an invalid AGGREGATE value',
    'ZUNIONSTORE rejects numkeys <= 0 with the input-key error',
    'ZUNIONSTORE rejects numkeys greater than available keys',
    'ZUNIONSTORE rejects wrong arity',
    'ZUNIONSTORE resets a SUM that becomes NaN (inf + -inf) to 0',
    'ZUNIONSTORE sums scores by default and stores the result',
    'ZUNIONSTORE treats a plain set source as scores of 1',
  ]),
  todo(
    'node-redis/zset/workflow.test.ts',
    missing('zRangeByScore', 'zRangeWithScores', 'zRem'),
    [
      'Sorted Set commands workflow - Leaderboard',
      'Sorted Set commands workflow - Priority Queue',
      'Sorted Set commands workflow - Search Results Ranking',
      'Sorted Set commands workflow - Time Series Events',
    ],
  ),
  skip('node-redis/zset/zmpop-bzmpop.test.ts', SKIP.secondClusterClient),
]
