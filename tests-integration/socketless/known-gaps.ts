/**
 * Known divergences of the socketless client mocks (#412), applied by
 * `register.ts` when the suites run with `TEST_BACKEND=socketless`
 * (`npm run test:integration:socketless`).
 *
 * Every entry is a case the socketless backend cannot pass *today*. It is a
 * finding, not a waiver: fix the mock in `src/`, then delete the entry —
 * `register.ts` fails the file when a listed `todo` test passes (or an entry
 * matches nothing), so a stale entry cannot linger.
 *
 * `ioredis/**` has no entries: `createIoredisMock` is the real ioredis client
 * over a virtual socket, and the whole suite passes on it (bar the in-process
 * server's own gaps, which `scripts-typed-replies` marks for mock and socketless
 * alike). Everything below is the hand-written node-redis facade
 * (`createNodeRedisMock`), which curates a few dozen of node-redis' methods and
 * signatures — most of these are surface it does not have yet, not wrong
 * replies.
 */
export type KnownGap = {
  /** Test file, relative to `tests-integration/` (e.g. `ioredis/multi.test.ts`). */
  file: string
  /**
   * Exact test title, a list of them, or a RegExp over the title. Omit to
   * cover every test in the file.
   */
  test?: string | readonly string[] | RegExp
  /** Why it diverges on the socketless backend. */
  reason: string
  /**
   * `todo` (default): the test still runs; a failure is reported but does not
   * fail the run. `skip`: the test (or, for a whole-file entry, every suite) is
   * not run at all — for files whose setup the backend cannot provide.
   */
  mode?: 'todo' | 'skip'
}

/** The recurring causes, spelled once. */
const WHY = {
  clusterSendCommand:
    "calls node-redis' cluster `sendCommand(firstKey, isReadonly, args)`; `NodeRedisMockCluster.sendCommand` only takes `(args)`, so the key is read as the argument list",
  clusterTopology:
    "reads node-redis' cluster topology (`cluster.slots` / `cluster.masters`) to find a slot owner and open a direct node client; `NodeRedisMockCluster` exposes neither",
  topologyInBefore:
    "before() reads node-redis' cluster topology (`cluster.slots`) to find a slot owner; `NodeRedisMockCluster` does not expose it",
  argumentShapes:
    "passes node-redis' array / object argument forms (`sAdd(key, [members])`, `hSet(key, { … })`, `set(key, v, { expiration })`, …); the facade's curated methods only take positional strings",
  zRangeOptions:
    "the facade's `zRange(key, start, stop)` silently drops node-redis' options argument (`BY` / `REV` / `LIMIT`) and runs a plain index ZRANGE — wrong results, not an error",
  duplicatePromise:
    '`NodeRedisMockClient.duplicate()` returns a Promise; node-redis returns the (unconnected) client synchronously',
  secondClusterClient:
    'needs a second cluster client on the same keyspace (blocking pop + push, WATCH from another connection); `NodeRedisMockCluster` has no `duplicate()` or equivalent',
  respOption:
    "opens node-redis clients at a chosen RESP version on a TCP port; `createNodeRedisMock()` takes no `RESP` option (RESP3 only via `sendCommand(['HELLO', '3'])`)",
} as const

/** The facade has no such curated method (node-redis' typed command API). */
function missing(...methods: string[]): string {
  return `the facade has no ${methods.map(m => `\`${m}()\``).join(', ')}`
}

function combine(...reasons: string[]): string {
  return reasons.join('; ')
}

export const SOCKETLESS_KNOWN_GAPS: readonly KnownGap[] = [
  {
    file: 'node-redis/cluster-integration.test.ts',
    reason: combine(WHY.clusterSendCommand, WHY.clusterTopology),
  },
  {
    file: 'node-redis/command-integration.test.ts',
    reason: WHY.clusterSendCommand,
  },
  {
    file: 'node-redis/commands.test.ts',
    reason: missing('mGet'),
    test: ['cross slot error', 'returns multiple key values'],
  },
  {
    file: 'node-redis/config-integration.test.ts',
    mode: 'skip',
    reason: WHY.topologyInBefore,
  },
  {
    file: 'node-redis/connection-integration.test.ts',
    mode: 'skip',
    reason: WHY.topologyInBefore,
  },
  {
    file: 'node-redis/copy-integration.test.ts',
    reason: missing('copy', 'select'),
    test: [
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
    ],
  },
  {
    file: 'node-redis/copy-integration.test.ts',
    reason: WHY.argumentShapes,
    test: [
      'REPLACE overwrites the destination TTL with the source TTL (none)',
      'copies the TTL together with the value',
      'works on any value type (list)',
    ],
  },
  {
    file: 'node-redis/flush-async-sync.test.ts',
    reason: missing('flushAll', 'flushDb'),
    test: [
      'FLUSHALL accepts ASYNC and clears all databases',
      'FLUSHALL accepts SYNC',
      'FLUSHALL with no argument still works',
      'FLUSHDB accepts ASYNC and clears the keyspace',
      'FLUSHDB accepts SYNC',
      'FLUSHDB with no argument still works',
    ],
  },
  {
    file: 'node-redis/geo/core.test.ts',
    reason: combine(missing('geoAdd', 'geoPos'), WHY.clusterSendCommand),
  },
  {
    file: 'node-redis/geo/search.test.ts',
    reason: combine(
      missing('geoAdd', 'geoSearch'),
      WHY.argumentShapes,
      WHY.clusterSendCommand,
    ),
  },
  {
    file: 'node-redis/hash/basic.test.ts',
    reason: combine(
      missing('hExists', 'hLen', 'hSetNX'),
      WHY.argumentShapes,
      WHY.clusterTopology,
    ),
  },
  {
    file: 'node-redis/hash/del.test.ts',
    reason: combine(missing('hDel'), WHY.argumentShapes, WHY.clusterTopology),
  },
  {
    file: 'node-redis/hash/errors-workflow.test.ts',
    reason: combine(
      missing('hIncrBy'),
      WHY.argumentShapes,
      WHY.clusterSendCommand,
    ),
  },
  {
    file: 'node-redis/hash/field-expire.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/hash/field-persist.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/hash/hgetex.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/hash/hsetex.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/hash/incr.test.ts',
    reason: missing('hIncrBy', 'hIncrByFloat'),
  },
  {
    file: 'node-redis/info-standalone.test.ts',
    reason: missing('info'),
  },
  {
    file: 'node-redis/key/commands.test.ts',
    reason: combine(
      missing('type'),
      WHY.argumentShapes,
      WHY.clusterSendCommand,
      WHY.clusterTopology,
    ),
  },
  {
    file: 'node-redis/key/expire.test.ts',
    reason: combine(
      missing('expireAt', 'persist'),
      WHY.clusterSendCommand,
      WHY.clusterTopology,
    ),
  },
  {
    file: 'node-redis/key/keyspace-empty-cleanup.test.ts',
    reason: combine(missing('hDel'), WHY.clusterTopology),
  },
  {
    file: 'node-redis/key/keyspace-notifications.test.ts',
    reason: WHY.duplicatePromise,
  },
  {
    file: 'node-redis/key/workflow.test.ts',
    reason: missing('expireAt'),
    test: ['Expiration workflow - Cache with Scheduled Invalidation'],
  },
  {
    file: 'node-redis/key/workflow.test.ts',
    reason: WHY.argumentShapes,
    test: [
      'DBSIZE workflow - Database Monitoring and Capacity Planning',
      'Key commands workflow - Cache Validation',
      'Key commands workflow - Data Type Validation',
      'Key commands workflow - Multi-tenant Data Isolation',
    ],
  },
  {
    file: 'node-redis/list/blmove.test.ts',
    mode: 'skip',
    reason: WHY.secondClusterClient,
  },
  {
    file: 'node-redis/list/blocking.test.ts',
    mode: 'skip',
    reason: WHY.secondClusterClient,
  },
  {
    file: 'node-redis/list/commands.test.ts',
    reason: combine(missing('lLen'), WHY.argumentShapes, WHY.clusterTopology),
  },
  {
    file: 'node-redis/list/linsert.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/list/lmpop-blmpop.test.ts',
    mode: 'skip',
    reason: WHY.secondClusterClient,
  },
  {
    file: 'node-redis/list/lpos-lmove.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/move-integration.test.ts',
    reason: missing('select'),
    test: [
      'errors when moving to the current database',
      'errors when the destination DB index is not an integer',
      'errors when the destination DB index is out of range',
      'moves a key to another database and removes it from the source',
      'preserves TTL on the moved key',
      'returns 0 and leaves both databases unchanged when destination exists',
      'returns 0 when the source key is missing',
    ],
  },
  {
    file: 'node-redis/move-integration.test.ts',
    reason: WHY.clusterTopology,
    test: ['rejects MOVE in cluster mode'],
  },
  {
    file: 'node-redis/multi.test.ts',
    reason: combine(
      missing('multi'),
      WHY.clusterTopology,
      WHY.secondClusterClient,
    ),
  },
  {
    file: 'node-redis/pubsub-integration.test.ts',
    reason: WHY.duplicatePromise,
  },
  {
    file: 'node-redis/randomkey-integration.test.ts',
    reason: missing('flushAll'),
  },
  {
    file: 'node-redis/scan/keys-scan.test.ts',
    reason: combine(WHY.clusterSendCommand, WHY.clusterTopology),
  },
  {
    file: 'node-redis/scan/typed-scan.test.ts',
    reason: combine(missing('zScan'), WHY.argumentShapes),
  },
  {
    file: 'node-redis/scripts-noscript.test.ts',
    reason: missing('clientSetName'),
    test: [
      'RESET and QUIT are refused from scripts',
      'a refused CLIENT SETNAME leaves the connection name untouched',
    ],
  },
  {
    file: 'node-redis/scripts-typed-replies.test.ts',
    mode: 'skip',
    reason: WHY.respOption,
  },
  {
    file: 'node-redis/select-multi.test.ts',
    reason: missing('select'),
  },
  {
    file: 'node-redis/set/core.test.ts',
    reason: combine(
      WHY.argumentShapes,
      WHY.clusterSendCommand,
      WHY.clusterTopology,
    ),
  },
  {
    file: 'node-redis/set/setops.test.ts',
    reason: combine(WHY.argumentShapes, WHY.clusterTopology),
  },
  {
    file: 'node-redis/set/workflow.test.ts',
    reason: WHY.argumentShapes,
  },
  {
    file: 'node-redis/sharded-pubsub-cluster.test.ts',
    reason: combine(missing('sUnsubscribe'), WHY.clusterSendCommand),
  },
  {
    file: 'node-redis/sort-integration.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/stream/add.test.ts',
    reason: combine(missing('xAdd'), WHY.clusterTopology),
  },
  {
    file: 'node-redis/stream/group.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/stream/range.test.ts',
    reason: combine(missing('xAdd', 'xLen'), WHY.clusterTopology),
  },
  {
    file: 'node-redis/stream/read.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/stream/setid.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/stream/trim.test.ts',
    reason: combine(missing('xAdd', 'xDel', 'xTrim'), WHY.clusterTopology),
  },
  {
    file: 'node-redis/string/bitmap-offset-limit.test.ts',
    reason: missing('configSet'),
    test: [
      'lowering proto-max-bulk-len lowers the BITFIELD ceiling',
      'lowering proto-max-bulk-len lowers the SETBIT/GETBIT ceiling',
      'raising the limit past 512MB raises the read ceiling but not the write ceiling',
      'the ceiling follows the setting back up',
    ],
  },
  {
    file: 'node-redis/string/bitmap.test.ts',
    mode: 'skip',
    reason: WHY.topologyInBefore,
  },
  {
    file: 'node-redis/string/core.test.ts',
    reason: missing('append', 'getSet', 'mGet', 'mSetNX', 'strLen'),
    test: [
      'APPEND command',
      'GETSET command',
      'MGET command',
      'MGET cross-slot error',
      'MGET returns null for keys holding non-string values',
      'MSETNX command',
      'STRLEN command',
      'String commands workflow',
    ],
  },
  {
    file: 'node-redis/string/core.test.ts',
    reason: WHY.clusterTopology,
    test: [
      'SUBSTR aliases GETRANGE',
      'String numeric and expiration errors match Redis',
    ],
  },
  {
    file: 'node-redis/string/hyperloglog.test.ts',
    mode: 'skip',
    reason: WHY.topologyInBefore,
  },
  {
    file: 'node-redis/string/incr.test.ts',
    reason: combine(
      missing('decr', 'incrBy', 'incrByFloat'),
      WHY.clusterTopology,
    ),
  },
  {
    file: 'node-redis/string/proto-max-bulk-len.test.ts',
    reason: missing('append', 'configGet', 'configSet', 'getRange', 'setRange'),
    test: [
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
  },
  {
    file: 'node-redis/string/set.test.ts',
    reason: WHY.argumentShapes,
    test: [
      'SET with EX option',
      'SET with GET option',
      'SET with NX option - key does not exist',
      'SET with NX option - key exists',
      'SET with PX option',
      'SET with XX option - key does not exist',
      'SET with XX option - key exists',
      'SET with multiple options',
    ],
  },
  {
    file: 'node-redis/string/set.test.ts',
    reason: WHY.clusterTopology,
    test: [
      'SET KEEPTTL preserves the existing expiration',
      'SET and GET wrong-type and syntax errors match Redis',
    ],
  },
  {
    file: 'node-redis/time-lastsave-integration.test.ts',
    reason: missing('lastSave', 'time'),
    test: [
      'LASTSAVE returns a Unix timestamp integer not in the future',
      'TIME returns [seconds, microseconds] close to current time',
    ],
  },
  {
    file: 'node-redis/watch.test.ts',
    reason: WHY.clusterTopology,
  },
  {
    file: 'node-redis/zset/byte-order.test.ts',
    reason: missing('zRangeByScore', 'zRank'),
    test: [
      'ZRANGEBYSCORE breaks equal-score ties by raw byte order',
      'ZRANK reflects raw byte order for equal scores',
    ],
  },
  {
    file: 'node-redis/zset/bzpopmin-bzpopmax.test.ts',
    mode: 'skip',
    reason: WHY.secondClusterClient,
  },
  {
    file: 'node-redis/zset/core.test.ts',
    reason: combine(
      missing('zCard', 'zIncrBy', 'zRem', 'zScore'),
      WHY.argumentShapes,
      WHY.clusterSendCommand,
    ),
  },
  {
    file: 'node-redis/zset/double-format.test.ts',
    reason: missing('zIncrBy', 'zRangeWithScores', 'zScore'),
    test: [
      'WITHSCORES parses back to the stored scores',
      'ZINCRBY parses back to the new score',
      'ZSCORE / ZMSCORE parse back to the stored score',
    ],
  },
  {
    file: 'node-redis/zset/lex.test.ts',
    reason: combine(
      missing('zLexCount', 'zRangeByLex', 'zRemRangeByLex'),
      WHY.clusterSendCommand,
      WHY.zRangeOptions,
    ),
  },
  {
    file: 'node-redis/zset/modern-range.test.ts',
    reason: combine(
      missing(
        'zRandMember',
        'zRandMemberCount',
        'zRandMemberCountWithScores',
        'zRangeStore',
        'zRangeWithScores',
        'zmScore',
      ),
      WHY.argumentShapes,
      WHY.clusterSendCommand,
      WHY.clusterTopology,
      WHY.zRangeOptions,
    ),
  },
  {
    file: 'node-redis/zset/range.test.ts',
    reason: combine(
      missing('zRangeByScore', 'zRangeWithScores', 'zRank'),
      WHY.clusterSendCommand,
      WHY.zRangeOptions,
    ),
  },
  {
    file: 'node-redis/zset/score-range.test.ts',
    reason: missing(
      'zRangeByScore',
      'zRangeByScoreWithScores',
      'zRangeWithScores',
      'zRemRangeByRank',
    ),
    test: [
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
  },
  {
    file: 'node-redis/zset/score-range.test.ts',
    reason: WHY.clusterSendCommand,
    test: [
      'ZRANGEBYSCORE LIMIT with non-integer rejects',
      'ZRANGEBYSCORE LIMIT without offset/count rejects with syntax error',
      'ZRANGEBYSCORE rejects wrong arity',
      'ZREMRANGEBYRANK rejects non-integer rank',
      'ZREMRANGEBYRANK rejects wrong arity',
    ],
  },
  {
    file: 'node-redis/zset/score-range.test.ts',
    reason: WHY.zRangeOptions,
    test: [
      'ZREVRANGEBYSCORE exclusive bounds',
      'ZREVRANGEBYSCORE on missing key returns empty',
      'ZREVRANGEBYSCORE rejects non-float bound',
      'ZREVRANGEBYSCORE supports LIMIT',
    ],
  },
  {
    file: 'node-redis/zset/setops.test.ts',
    reason: WHY.clusterTopology,
    test: [
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
    ],
  },
  {
    file: 'node-redis/zset/workflow.test.ts',
    reason: missing('zRangeByScore', 'zRangeWithScores', 'zRem'),
  },
  {
    file: 'node-redis/zset/zmpop-bzmpop.test.ts',
    mode: 'skip',
    reason: WHY.secondClusterClient,
  },
]
