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
  // src/core/subcommand-errors.ts). Verified against redis-server 6.2.24, 7.0.15 and
  // 8.0.6. Valkey forked at 7.2, so every Valkey profile has the newer wording.
  //
  // This gates the *wording* only; *when* the subcommand is resolved is
  // `error.unknown-subcommand-dispatch-timing` below. Still not modelled:
  //  - Arity errors, `redis-6.2`. `addReplySubcommandSyntaxError` reaches only
  //    PUBSUB, 1 of 11 containers; the rest answer the 7.0-era `wrong number
  //    of arguments for '<c>|<sub>'` arity error on every profile (#437).
  'error.unknown-subcommand-wording': { redis: '7.0.0', valkey: '7.2.0' },
  // The same 7.0 change resolves `container|subcommand` at command-lookup
  // time, so an unknown subcommand fails before anything else looks at the
  // command: MULTI refuses to queue it and EXEC answers -EXECABORT (#435), a
  // trailing key is never looked up (#436), and a script's redis.call gets
  // `Unknown Redis command called from script` instead of the container's
  // reply (#439). 6.2 has no such lookup; every container, XGROUP/XINFO
  // included, rejects the subcommand only when it runs, and XGROUP/XINFO look
  // their key up first. The lookup runs in `CommandExecutor.plan()` against
  // the real subcommand tables in `subcommand-gates.ts`. Verified against
  // redis-server 6.2.24, 7.0.15, 8.0.6 and Valkey 7.2 / 8.0 / 9.0.
  'error.unknown-subcommand-dispatch-timing': {
    redis: '7.0.0',
    valkey: '7.2.0',
  },
  // The last line of a container's HELP reply: `Prints this help.` through
  // 7.0, `Print this help.` from Redis 7.2 / Valkey 7.2. Only XINFO/XGROUP HELP
  // read it so far; the other containers still say `Prints` on every profile.
  // Verified against redis-server 6.2.24, 7.0.15, 7.2, 7.4, 8.0.6 and Valkey
  // 7.2 / 8.0 / 8.1 / 9.0.
  'reply.help-print-wording': { redis: '7.2.0', valkey: '7.2.0' },
  // XGROUP HELP documents ENTRIESREAD (and gives DESTROY its own description
  // line) from Redis 7.0; 6.2's text has neither. Verified against 6.2.24 and
  // 7.0.15.
  'xgroup.help-entriesread': { redis: '7.0.0', valkey: '7.2.0' },
  // The unknown-command reply: Redis 6.2 quotes with backticks, separates args
  // with `, ` and echoes the whole name (`%s`); 7.0 switched to single quotes,
  // a space separator and `%.128s`. The args budget is 128 bytes either way, so
  // 6.2 echoes fewer args (30 one-to-two-digit args: a0..a19 on 6.2, a0..a22
  // on 7.0+). Verified against redis-server 6.2.24, 7.0, 7.2, 8.0.6 and
  // Valkey 7.2 / 8.0, which answer the 7.0 form (#384).
  'error.unknown-command-wording': { redis: '7.0.0', valkey: '7.2.0' },
  // An odd field/value tail the command table accepts but the command itself
  // refuses: 6.2 answers `wrong number of arguments for MSET` (MSET and
  // MSETNX alike) and `wrong number of arguments for XADD`; 7.0 moved both to
  // the standard arity wording (`... for 'mset' command`, `... for 'xadd'
  // command`). Verified against redis-server 6.2.24, 7.0.15 and 8.0.6, and
  // Valkey 8.0 / 9.0 (#492).
  'error.odd-pairs-arity-wording': { redis: '7.0.0', valkey: '7.2.0' },
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
  //
  // The same rewrite replaced 6.2's `luaPushError` rejections, which have no
  // error code, an inner `@user_script: <line>: ` position and their own
  // wording (`Unknown Redis command called from Lua script`, `This Redis
  // command is not allowed from scripts`, `Wrong number of args calling Redis
  // command From Lua script`, `Please specify at least one argument for
  // redis.call()`), so this gate also picks that wording (see
  // `scriptRejection` in src/core/lua-runtime.ts).
  'script.abort-error-suffix': { redis: '7.0.0', valkey: '7.2.0' },
  // Valkey 8.0 dropped the product name from the scripting layer's own
  // errors, where Redis (and Valkey 7.2) keep it:
  //   Unknown command called from script              (Unknown Redis command ...)
  //   Wrong number of args calling command from script (... calling Redis command ...)
  //   Please specify at least one argument for this call (... this redis lib call)
  //   Command arguments must be strings or integers    (Lua redis lib command ...)
  // Verified against Valkey 7.2, 8.0.11, 8.1 and 9.0.6 and Redis 7.0.15 /
  // 8.0.6 (#492).
  'script.unknown-command-valkey-wording': { valkey: '8.0.0' },
  // Valkey 9.0 names itself in the noscript refusal: `This Valkey command is
  // not allowed from script`; Redis and Valkey 7.2 through 8.1 say `This Redis
  // command ...`. Verified against Valkey 8.0.11, 8.1.10 and 9.0.6.
  'script.not-allowed-valkey-wording': { valkey: '9.0.0' },
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
