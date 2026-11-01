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
 * over a virtual socket, and the whole suite passes on it. Everything below is
 * the node-redis facade (`createNodeRedisMock`), which has node-redis' own
 * command methods but not all of its client surface — most of these are
 * surface it does not have yet (a direct node connection, `nodeClient()`,
 * `withTypeMapping()`, cluster pub/sub), not wrong replies.
 */

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

type Cause = { reason: string; error: NonNullable<KnownGap['error']> }

/**
 * The recurring causes, spelled once. Each `error` is the narrowest pattern
 * that still names the cause itself — never a bare assertion failure — so a
 * listed test that starts failing some other way is caught. Where a test
 * asserts on a server error, the facade's actual error text appears inside
 * node:assert's message, which is what these patterns then match.
 */
const CAUSE = {
  directNodeConnection: {
    reason:
      "opens a direct node-redis connection to a slot owner (`connectToNodeRedisEndpoint()` on a node from `cluster.slots` / CLUSTER SLOTS); the facade's nodes have synthetic ports nothing listens on, so the harness refuses the dial instead of letting node-redis retry it until the test times out",
    error:
      /cannot provide a direct node-redis connection to [\d.]+:\d+ — it needs a TCP port/,
  },
} as const satisfies Record<string, Cause>

/** Skip reasons: setups the backend cannot provide at all. */
const SKIP = {
  directConnectionInBefore: `before() ${CAUSE.directNodeConnection.reason}`,
  nodeClientWithWaiters:
    'every test counts the parked waiters through `cluster.nodeClient(master).info()`, which the facade does not have; the blocking command a waiter already sent then outlives the failed test and rejects after it ended',
  tcpClients:
    'before() opens real node-redis clients (`createClient({ url, RESP })`) on a `setupRawStandalone()` TCP port, which the socketless backend has no way to provide',
} as const

/** The facade has no such curated method (node-redis' typed command API). */
function missing(...methods: string[]): Cause {
  return {
    reason: `the facade has no ${methods.map(m => `\`${m}()\``).join(', ')}`,
    error: new RegExp(`\\.(?:${methods.join('|')}) is not a function$`),
  }
}

function todo(file: string, cause: Cause, test: readonly string[]): KnownGap {
  return { file, reason: cause.reason, error: cause.error, test }
}

function skip(file: string, reason: string): KnownGap {
  return { file, reason, mode: 'skip' }
}

export const SOCKETLESS_KNOWN_GAPS: readonly KnownGap[] = [
  skip('node-redis/blocking-fifo.test.ts', SKIP.nodeClientWithWaiters),
  todo('node-redis/cluster-integration.test.ts', CAUSE.directNodeConnection, [
    'CLUSTER NODES reports bus port as client port + 10000',
    'CLUSTER SHARDS returns structured shard metadata',
    'CLUSTER arity and subcommand errors match Redis',
    'HELLO reports master and replica roles for direct node connections',
    'Lua redis.call and redis.pcall non-local key errors match Redis',
    'READONLY and READWRITE arity errors match Redis',
    'READONLY lets direct replica connections serve readonly commands for master slots',
    'RESET clears READONLY replica mode',
    'an empty first hash tag hashes the whole key; node-redis gives up on the MOVED (#88)',
    'direct node connections return MOVED for keys owned by another node',
    'direct replica connections redirect keyed commands to the master',
  ]),
  skip('node-redis/config-integration.test.ts', SKIP.directConnectionInBefore),
  skip(
    'node-redis/connection-integration.test.ts',
    SKIP.directConnectionInBefore,
  ),
  todo('node-redis/hash/basic.test.ts', CAUSE.directNodeConnection, [
    'HGETALL works over a RESP3 connection (HELLO 3)',
    'HRANDFIELD command matches Redis',
  ]),
  todo('node-redis/hash/del.test.ts', CAUSE.directNodeConnection, [
    'HGETDEL errors match Redis',
    'HGETDEL returns values and deletes fields',
  ]),
  todo('node-redis/hash/field-expire.test.ts', CAUSE.directNodeConnection, [
    'HEXPIRE arg-content errors stay inline inside MULTI/EXEC',
    'HEXPIRE condition flags match Redis',
    'HEXPIRE deletes the key when every field expires',
    'HEXPIRE errors match Redis',
    'HEXPIRE family sets and lazily expires hash fields',
    'HSCAN omits expired hash fields',
    'hash writes clear or preserve field TTLs like Redis',
  ]),
  todo('node-redis/hash/field-persist.test.ts', CAUSE.directNodeConnection, [
    'HPERSIST, HTTL and HPTTL errors match Redis',
    'HPERSIST, HTTL and HPTTL handle missing keys',
    'HPERSIST, HTTL and HPTTL report and clear hash field TTLs',
    'HTTL rounds hash-field TTLs up to the next second, unlike key TTL',
  ]),
  todo('node-redis/hash/hgetex.test.ts', CAUSE.directNodeConnection, [
    'HGETEX errors match Redis',
    'HGETEX returns values and updates field TTLs like Redis',
  ]),
  todo('node-redis/hash/hsetex.test.ts', CAUSE.directNodeConnection, [
    'HSETEX FNX/FXX conditions match Redis',
    'HSETEX errors match Redis',
    'HSETEX sets fields and expiration like Redis',
  ]),
  todo('node-redis/hash/incr.test.ts', CAUSE.directNodeConnection, [
    'HINCRBYFLOAT parses operands like strtold: hex floats, infinity and error order (#234)',
  ]),
  todo('node-redis/key/commands.test.ts', CAUSE.directNodeConnection, [
    'DBSIZE command',
    'TOUCH command counts live keys without mutating keyspace',
  ]),
  todo('node-redis/key/expire.test.ts', CAUSE.directNodeConnection, [
    'EXPIRE family supports conditional expiry options',
    'EXPIREAT/PEXPIREAT past timestamp respects conditional flags (#72)',
  ]),
  todo(
    'node-redis/key/keyspace-empty-cleanup.test.ts',
    CAUSE.directNodeConnection,
    [
      'emptying an EXISTING watched collection still invalidates the WATCH',
      'no-op HDEL on a non-existent key must not invalidate a WATCH on that key',
      'no-op SREM on a non-existent key must not invalidate a WATCH on that key',
    ],
  ),
  // The facade's own pub/sub runs here (via duplicateNodeRedisClient()); what
  // stops these is the rest of its surface.
  todo('node-redis/key/workflow.test.ts', CAUSE.directNodeConnection, [
    'DBSIZE workflow - Database Monitoring and Capacity Planning',
  ]),
  todo('node-redis/list/commands.test.ts', CAUSE.directNodeConnection, [
    'LPOP and RPOP support count argument',
    'List command errors match Redis',
  ]),
  todo('node-redis/list/linsert.test.ts', CAUSE.directNodeConnection, [
    'error paths match Redis',
    'inserts before and after the first matching pivot',
    'returns Redis-compatible values for missing keys and pivots',
  ]),
  todo('node-redis/list/lmpop-blmpop.test.ts', CAUSE.directNodeConnection, [
    'LMPOP / BLMPOP error paths match Redis',
  ]),
  todo('node-redis/list/lpos-lmove.test.ts', CAUSE.directNodeConnection, [
    'LMOVE error paths match Redis',
    'LMOVE moves elements between list ends',
    'LMOVE on missing source returns nil and deletes emptied source',
    'LMOVE rotates a list onto itself',
    'LPOS error and edge paths match Redis',
    'LPOS finds matches with RANK, COUNT and MAXLEN',
  ]),
  todo('node-redis/move-integration.test.ts', CAUSE.directNodeConnection, [
    'rejects MOVE in cluster mode',
  ]),
  todo('node-redis/multi.test.ts', CAUSE.directNodeConnection, [
    'aborts the whole transaction (EXECABORT) when a command fails to queue',
    'runtime command errors are surfaced via MultiErrorReply',
  ]),
  todo('node-redis/scan/keys-scan.test.ts', missing('withTypeMapping'), [
    'scan MATCH treats patterns as raw bytes',
  ]),
  todo('node-redis/scan/keys-scan.test.ts', CAUSE.directNodeConnection, [
    'HSCAN NOVALUES returns only matching fields',
    'KEYS and top-level SCAN MATCH treat patterns as raw bytes',
    'KEYS and top-level SCAN support MATCH and TYPE filters',
    'KEYS matches Redis glob pattern semantics',
    'SCAN MATCH advances across non-matching COUNT batches',
    'SCAN MATCH handles interleaved matching and non-matching batches',
    'SCAN TYPE advances across non-matching type COUNT batches',
  ]),
  skip('node-redis/scripts-typed-replies.test.ts', SKIP.tcpClients),
  todo('node-redis/set/core.test.ts', CAUSE.directNodeConnection, [
    'SMISMEMBER command matches Redis',
  ]),
  todo('node-redis/set/setops.test.ts', CAUSE.directNodeConnection, [
    'SINTERCARD command matches Redis',
  ]),
  todo('node-redis/sharded-pubsub-cluster.test.ts', missing('sUnsubscribe'), [
    'routes shard subscriptions and publishes by channel slot',
  ]),
  todo(
    'node-redis/sharded-pubsub-cluster.test.ts',
    CAUSE.directNodeConnection,
    ['rejects sharded Pub/Sub commands routed to the wrong slot'],
  ),
  todo('node-redis/sort-integration.test.ts', CAUSE.directNodeConnection, [
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
  todo('node-redis/sort-strtod.test.ts', CAUSE.directNodeConnection, [
    'SORT LIMIT takes the full int64 range',
    'SORT orders every infinity spelling as +/-inf',
    'SORT reads hex floats only before Redis 8.0 / Valkey 8.1',
    'SORT refuses trailing whitespace and NaN',
    'SORT takes 1e400 as infinity only on Redis 8.0',
    'SORT treats an empty element as 0, except on Valkey 8.1+',
  ]),
  todo('node-redis/stream/add.test.ts', CAUSE.directNodeConnection, [
    'XADD LIMIT validates approximate trim syntax before id',
    'XADD MAXLEN trims oldest entries after append',
    'XADD MAXLEN with ~ accepts LIMIT count before generated id',
    'XADD NOMKSTREAM appends to an existing stream normally',
    'XADD NOMKSTREAM returns null when key does not exist',
    'XADD handles auto-generated ids when the sequence overflows',
  ]),
  todo(
    'node-redis/stream/cluster-group-replication.test.ts',
    CAUSE.directNodeConnection,
    ['replicas receive in-place consumer-group and last-id changes'],
  ),
  todo('node-redis/stream/group.test.ts', CAUSE.directNodeConnection, [
    'XCLAIM and XAUTOCLAIM transfer pending stream entries',
    'XGROUP MKSTREAM and SETID control group delivery position',
    'XGROUP creates, mutates, and destroys consumer groups',
    'XINFO STREAM FULL defaults to 10 stream entries and PEL rows',
    'XINFO reports stream, group, and consumer metadata',
    'XREADGROUP history keeps deleted pending entries visible',
    'XREADGROUP history returns an empty per-key list for consumers with no pending entries',
    'stream consumer group commands report Redis-compatible errors',
  ]),
  todo('node-redis/stream/range.test.ts', CAUSE.directNodeConnection, [
    'XRANGE and XREVRANGE reject non-integer COUNT values',
  ]),
  todo('node-redis/stream/read.test.ts', CAUSE.directNodeConnection, [
    'XREAD COUNT limits the number of returned entries',
    'XREAD from multiple streams on same slot returns combined results',
    'XREAD on a missing key returns null',
    'XREAD parses entries over a default RESP3 connection',
    'XREAD returns entries after the given id',
    'XREAD returns null when no new entries exist for the given id',
    'XREAD with $ id returns null (no entries after current last)',
    'XREAD with + id returns the last entry from each stream',
  ]),
  todo('node-redis/stream/redelivery.test.ts', CAUSE.directNodeConnection, [
    'XPENDING and XINFO STREAM FULL see the bumped NACKs',
  ]),
  todo('node-redis/stream/setid.test.ts', CAUSE.directNodeConnection, [
    'XSETID ENTRIESADDED updates XINFO STREAM metadata',
    'XSETID MAXDELETEDID updates XINFO STREAM metadata',
    'XSETID rejects lower ids, invalid options, and wrong types',
    'XSETID sets last-generated-id and advances generated XADD ids',
  ]),
  todo('node-redis/stream/trim.test.ts', CAUSE.directNodeConnection, [
    'XTRIM LIMIT validates approximate trim syntax',
    'XTRIM MAXLEN with ~ (approximate) does not exact-trim tiny streams',
    'XTRIM MAXLEN with ~ accepts LIMIT count',
    'XTRIM MINID removes entries with id below threshold',
  ]),
  skip('node-redis/string/bitmap.test.ts', SKIP.directConnectionInBefore),
  todo('node-redis/string/core.test.ts', CAUSE.directNodeConnection, [
    'SUBSTR aliases GETRANGE',
    'String numeric and expiration errors match Redis',
  ]),
  skip('node-redis/string/hyperloglog.test.ts', SKIP.directConnectionInBefore),
  todo('node-redis/string/incr.test.ts', CAUSE.directNodeConnection, [
    'INCR/INCRBY/DECR/DECRBY operate over the full int64 range',
    'INCRBYFLOAT distinguishes invalid-float from NaN/Infinity result (#56)',
    'INCRBYFLOAT parses operands like strtold: hex floats and long double limits (#234)',
  ]),
  todo('node-redis/string/set.test.ts', CAUSE.directNodeConnection, [
    'SET KEEPTTL preserves the existing expiration',
    'SET and GET wrong-type and syntax errors match Redis',
  ]),
  todo('node-redis/watch.test.ts', CAUSE.directNodeConnection, [
    'EXEC should clear watched keys',
    'WATCH inside MULTI should return error',
    'WATCH should abort transaction if watched key is modified',
    'WATCH should allow transaction if watched key is not modified',
    'WATCH should allow watching multiple keys in the same slot',
    'WATCH should reject multiple keys in different slots',
  ]),
  todo(
    'node-redis/zset/bzpopmin-bzpopmax.test.ts',
    CAUSE.directNodeConnection,
    ['BZPOPMIN / BZPOPMAX error paths match Redis'],
  ),
  todo('node-redis/zset/modern-range.test.ts', CAUSE.directNodeConnection, [
    'ZRANGESTORE rejects destination and source keys from different slots',
  ]),
  todo('node-redis/zset/setops.test.ts', CAUSE.directNodeConnection, [
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
  todo('node-redis/zset/zmpop-bzmpop.test.ts', CAUSE.directNodeConnection, [
    'ZMPOP / BZMPOP error paths match Redis',
  ]),
]
