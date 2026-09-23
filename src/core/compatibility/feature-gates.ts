import type { FeatureId, VersionGate } from './profile'

export const FEATURE_GATES: Record<FeatureId, VersionGate> = {
  'expire.conditions': { redis: '7.0.0', valkey: '7.2.0' },
  'set.get': { redis: '6.2.0', valkey: '7.2.0' },
  'set.nx-get': { redis: '7.0.0', valkey: '7.2.0' },
  'set.exat-pxat': { redis: '6.2.0', valkey: '7.2.0' },
  'command.docs': { redis: '7.0.0', valkey: '7.2.0' },
  'command.getkeysandflags': { redis: '7.0.0', valkey: '7.2.0' },
  'acl.dryrun': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis 7.0 rewrote CONFIG SET and changed the failure wording from
  // `Invalid argument '<value>' for CONFIG SET '<name>' - <detail>` to
  // `CONFIG SET failed (possibly related to argument '<name>') - <detail>`,
  // and the unknown-parameter wording from `Unsupported CONFIG parameter: <name>`
  // to `Unknown option or number of arguments for CONFIG SET - '<name>'`.
  'config.set.failure-message': { redis: '7.0.0', valkey: '7.2.0' },
  // The same 7.0 rewrite let CONFIG SET take several parameter/value pairs.
  // 6.2 dispatches SET only for exactly one pair and answers every other shape
  // with the legacy subcommand syntax error; 7.0+ splits that into a
  // `config|set` arity error (no pair) and `syntax error` (dangling name), and
  // rejects a repeated parameter with `duplicate parameter`. Verified against
  // redis-server 6.2.24, 7.0.15, 8.0.6 and Valkey 7.2.14 (#419).
  'config.set.multi-pair': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis 6.2 saturates a memory value above the parameter's maximum to that
  // maximum; 7.0+ rejects it with the out-of-range error instead.
  'config.memory-value.reject-overflow': { redis: '7.0.0', valkey: '7.2.0' },
  // The `n` (new-key) notify-keyspace-events class is Redis 7.0+; 6.2 rejects
  // it as an invalid flag character (it does accept `m` and `d`).
  'notify.keyspace.new-key-class': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis 7.0 relaxed the RESP multibulk element-count bound in
  // `processMultibulkBuffer` from `ll > 1024*1024` to `ll > INT_MAX`: `*1048577`
  // is `-ERR Protocol error: invalid multibulk length` on 6.2.24 and accepted on
  // 7.0.15 / 8.0. Valkey forked after the change (7.2.14 accepts it).
  'protocol.multibulk-count-int-max': { redis: '7.0.0', valkey: '7.2.0' },
  'client.no-evict': { redis: '7.0.0', valkey: '7.2.0' },
  'client.kill.maxage': { redis: '7.4.0', valkey: '9.0.0' },
  'client.setinfo': { redis: '7.2.0', valkey: '7.2.0' },
  // Redis 7.0 moved container commands into the command table, which replaced
  // `Unknown subcommand or wrong number of arguments for '%s'. Try %s HELP.`
  // with `unknown subcommand '%.128s'. Try %s HELP.` — a new template, a
  // lower-case lead and `%.128s` truncation of the echoed name. The same flip
  // hit `addReplySubcommandSyntaxError`, which keeps the `or wrong number of
  // arguments` clause and gains no truncation, so one gate covers both (see
  // `unknownSubcommandError` / `subcommandSyntaxError` in
  // src/commands/helpers.ts). Verified against redis-server 6.2.24, 7.0.15 and
  // 8.0.6. Valkey forked at 7.2, so every Valkey profile has the newer wording.
  //
  // This gates the *wording* only. The same 7.0 change also moved *when* and
  // *whether* a container sees its subcommand, and none of that is modelled
  // yet. The known divergences, and which profiles they are wrong on:
  //  - MULTI, 7.0+ profiles. 7.0 rejects an unknown container subcommand at
  //    queue time and EXEC answers -EXECABORT; this server queues it and
  //    errors at EXEC (#435).
  //  - XGROUP/XINFO, `redis-6.2`. They resolve the subcommand in a schema
  //    parser, i.e. at queue time, so they abort a transaction that real 6.2
  //    queues. The same early resolution means that with a trailing key real
  //    6.2 checks the key first (`ERR no such key`, `WRONGTYPE`, `...requires
  //    the key to exist`) and only then the subcommand; this server always
  //    answers the subcommand error (#436).
  //  - Arity errors, `redis-6.2`. `addReplySubcommandSyntaxError` reaches only
  //    PUBSUB, 1 of 11 containers; the rest answer the 7.0-era `wrong number
  //    of arguments for '<c>|<sub>'` arity error on every profile (#437).
  //  - Scripts, 7.0+ profiles. An unknown subcommand called from Lua fails
  //    command lookup and answers `ERR Unknown Redis command called from
  //    script`; this server dispatches the container and produces the reply
  //    below instead (#439).
  'error.unknown-subcommand-wording': { redis: '7.0.0', valkey: '7.2.0' },
  'info.multi-section': { redis: '7.0.0', valkey: '7.2.0' },
  'shutdown.now-force-abort': { redis: '7.0.0', valkey: '7.2.0' },
  'pubsub.sharded': { redis: '7.0.0', valkey: '7.2.0' },
  'pubsub.resp3-publish-reply-first': { redis: '7.2.0', valkey: '8.0.0' },
  'stream.xautoclaim-deleted-ids': { redis: '7.0.0', valkey: '7.2.0' },
  // BITCOUNT/BITPOS BYTE|BIT range modifier — Redis 7.0 / Valkey 7.2.
  'bit.byte-bit-range': { redis: '7.0.0', valkey: '7.2.0' },
  'hscan.novalues': { redis: '7.4.0', valkey: '9.0.0' },
  'xread.plus-id': { redis: '7.4.0' },
  'cluster.multi-db': { valkey: '9.0.0' },
  // SORT BY/GET in cluster mode: compare each pattern's slot against the sort
  // key's slot (`patternHashSlot()`) instead of refusing every pattern, and use
  // the longer "...may be in different slots." wording — Redis 7.4 / Valkey 8.0.
  'sort.cluster-pattern-slot': { redis: '7.4.0', valkey: '8.0.0' },
  // SORT GET '#' (the element itself) is exempt from that slot comparison.
  // Bisected on single-node clusters: redis 7.4.0/7.4.1 refuse and 7.4.2+
  // accept; valkey 8.0.0/8.0.1 refuse and 8.0.2+ accept. Before that it is
  // hashed like any other pattern and therefore denied.
  'sort.cluster-get-hash': { redis: '7.4.2', valkey: '8.0.2' },
  // Double replies (ZSCORE, ZINCRBY, WITHSCORES, RESP3 `,`): Redis 7.2 moved
  // `addReplyDouble()` from `%.17g` (`0.1` → `0.10000000000000001`) to
  // `d2string()`, i.e. exact integer digits within ±2^62 and `fpconv_dtoa`
  // (Grisu2) otherwise (`0.1`). Checked against redis-server 6.2.14, 7.0.15,
  // 7.2.4, 7.4.4, 8.0.6 and valkey-server 8.0.0 / 9.0.0 — see
  // tests/fixtures/redis-double-format.json and src/core/double-format.ts.
  'reply.double-fpconv': { redis: '7.2.0', valkey: '7.2.0' },
  // GEOPOS / WITHCOORD coordinates: Redis 8.0 replies with `addReplyDouble()`
  // (the `d2string()` spelling above, `13.361389338970184`); 6.2-7.4 and every
  // Valkey through 9.0 use `addReplyHumanLongDouble()` (`%.17Lf` trimmed,
  // `13.36138933897018433`). Both are a `,` double on RESP3. Checked against
  // redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0, 8.0.6 and valkey-server
  // 8.0.0 / 9.0.0.
  'geo.coord-d2string': { redis: '8.0.0' },
  // Redis 7.0 moved `noscript` from the container to each subcommand, and no
  // container's HELP subcommand carries it: `redis.pcall('CLIENT','HELP')`
  // (likewise CONFIG/ACL/SCRIPT/FUNCTION) returns the help text on 7.0+, where
  // 6.2 refuses the whole container. Verified against redis-server 6.2, 7.0,
  // 8.0 and valkey 7.2/8.0/9.0.
  'script.per-subcommand-noscript': { redis: '7.0.0', valkey: '7.2.0' },
  // QUIT got a command-table entry in Redis 7.0; 6.2 special-cases it in the
  // connection loop, so a 6.2 script calling QUIT fails command lookup
  // (unknown command) instead of hitting its 7.0+ `noscript` refusal.
  'command.quit-table-entry': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis 7.0 moved the script-abort decoration from a prefix,
  // `Error running script (call to f_<sha>): @user_script:<line>: <error>`, to
  // a suffix, `<error> script: <sha>, on @user_script:<line>.`, and started
  // keeping a failing redis.call's own error code (`-WRONGTYPE ...`) instead of
  // folding it into an `-ERR` body. Verified against redis-server 6.2.24,
  // 7.0.15 and 8.0; Valkey 7.2 and 8.0 answer the 7.0 form.
  'script.abort-error-suffix': { redis: '7.0.0', valkey: '7.2.0' },
  // COMMAND GETKEYS / GETKEYSANDFLAGS took arity -4 in 7.0 (a command and at
  // least one argument: `COMMAND GETKEYS GET` is a `command|getkeys` arity
  // error); 7.2 relaxed it to -3 and answers a short target with `Invalid
  // number of arguments specified for command`, as 6.2 did. Only profiles
  // with `command.getkeysandflags` and without this gate (7.0) are strict.
  // Verified against 6.2.14, 7.0.15 and 7.2.4.
  'command.getkeys-single-arg': { redis: '7.2.0', valkey: '7.2.0' },
  // ZRANK / ZREVRANK WITHSCORE arrived in 7.2; before that both commands have
  // arity 3 and a trailing WITHSCORE is an arity error (6.2.14, 7.0.15).
  'zrank.withscore': { redis: '7.2.0', valkey: '7.2.0' },
  // XSETID ENTRIESADDED / MAXDELETEDID arrived in 7.0; 6.2 has arity 3 and
  // answers any extra token with an arity error (6.2.14).
  'xsetid.entries-added': { redis: '7.0.0', valkey: '7.2.0' },
}
