import type { FeatureId, VersionGate } from './profile'

export const FEATURE_GATES: Record<FeatureId, VersionGate> = {
  'expire.conditions': { redis: '7.0.0', valkey: '7.2.0' },
  'set.get': { redis: '6.2.0', valkey: '7.2.0' },
  'set.nx-get': { redis: '7.0.0', valkey: '7.2.0' },
  'set.exat-pxat': { redis: '6.2.0', valkey: '7.2.0' },
  'command.docs': { redis: '7.0.0', valkey: '7.2.0' },
  // COMMAND INFO entries grew tips, key specs and subcommands in 7.0; 6.2's
  // have 7 fields, ending with the ACL categories (redis-server 6.2.24).
  'command.info-extended-fields': { redis: '7.0.0', valkey: '7.2.0' },
  'command.getkeysandflags': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis 7.0 rewrote COMMAND as a container with per-subcommand entries:
  // COMMAND LIST arrived, a bare COMMAND INFO lists every command (6.2
  // answers an empty array) and HELP gained the new subcommands. 6.2
  // dispatches on the argument count alone (HELP and COUNT take none, GETKEYS
  // at least one, INFO any) and answers everything else, a wrong count
  // included, with `addReplySubcommandSyntaxError`. Verified against
  // redis-server 6.2.24 and 7.0.15.
  'command.list': { redis: '7.0.0', valkey: '7.2.0' },
  // Valkey 8.0 dropped "Redis" from COMMAND HELP (`Return details about all
  // commands.`); Valkey 7.2.14 still has it. Verified against 7.2.14, 8.0.11
  // and 9.0.6.
  'command.help-valkey-wording': { valkey: '8.0.0' },
  // A COMMAND INFO entry without subcommands ends in an empty set on Redis
  // (`~0` in RESP3) and an empty array on Valkey (`*0`); RESP2 cannot tell
  // them apart. Verified against redis-server 7.0.15 / 8.0.6 and Valkey
  // 7.2.14 / 8.0.11 / 9.0.6, the patch releases the command table is
  // captured from. Earlier Valkey patches still send `~0`: 8.0.0, 9.0.0 and
  // 9.0.1 do, so the valkey-8.0 / valkey-9.0 presets (nominally 8.0.0 /
  // 9.0.0) answer as 8.0.11 / 9.0.6 do, not as those first releases.
  'command.info-subcommands-array': { valkey: '7.2.0' },
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
  // Redis 7.0 looks up the source keys of SINTERSTORE / SUNIONSTORE /
  // SDIFFSTORE / ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE / ZRANGESTORE /
  // SORT ... STORE, COPY's source and a single-key PFCOUNT with
  // `lookupKeyRead`, so a missing one publishes `keymiss`; 6.2 uses
  // `lookupKeyWrite` there (6.2.14 t_set.c:869, t_zset.c:2608 / 3700,
  // sort.c:273, db.c:1253, hyperloglog.c:1261), which publishes nothing.
  // Verified against redis-server 6.2.14, 7.0.15, 7.2.4, 8.0.0 and Valkey
  // 8.0 / 9.0 (#514).
  'notify.keyspace.keymiss-store-sources': { redis: '7.0.0', valkey: '7.2.0' },
  // Valkey 9.0's HGETEX looks its hash up with `lookupKeyRead`, so a missing
  // one publishes `keymiss`; Redis 8.0's uses `lookupKeyWrite` (8.0.0
  // t_hash.c:2788) and publishes nothing. Probed on valkey 9.0.0 and redis
  // 8.0.0 (#514).
  'notify.keyspace.keymiss-hgetex': { valkey: '9.0.0' },
  // Redis 7.0 relaxed the RESP multibulk element-count bound in
  // `processMultibulkBuffer` from `ll > 1024*1024` to `ll > INT_MAX`: `*1048577`
  // is `-ERR Protocol error: invalid multibulk length` on 6.2.24 and accepted on
  // 7.0.15 / 8.0. Valkey forked after the change (7.2.14 accepts it).
  'protocol.multibulk-count-int-max': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis finds the end of a `*<count>` / `$<length>` header line with
  // `strchr(querybuf, '\r')`, which stops at a NUL byte: a NUL before the CR
  // reads as "no CR yet", so the request waits (or trips the 64KB `too big
  // ... count string` cap) instead of failing `string2ll`. Verified on
  // redis-server 7.0.15; the same `strchr` is in 6.2.14, 7.2.4, 7.4.4, 8.0.0
  // and Valkey 7.2.4 / 8.0.x. Valkey 8.1.0 switched to `memchr` over the
  // buffered length (networking.c, `parseMultibulk`), which scans past a NUL.
  // Inline requests still use `strchr(querybuf, '\n')` on every version.
  'protocol.header-scan-past-nul': { valkey: '8.1.0' },
  // Valkey 9.0 rewrote `sdssplitargs` on top of `sdsparsearg`: a closing quote
  // no longer ends the inline argument, so `"a"b` is one argument `ab`
  // instead of `Protocol error: unbalanced quotes in request`. Every Redis
  // version and Valkey up to 8.1.0 still require a separator after a closing
  // quote (sds.c, checked in 6.2.14 through 8.0.0 and Valkey 7.2.4 / 8.0.0 /
  // 8.1.0; the error verified on redis-server 7.0.15).
  'protocol.inline-adjacent-quotes': { valkey: '9.0.0' },
  'client.no-evict': { redis: '7.0.0', valkey: '7.2.0' },
  // CLIENT KILL MAXAGE <seconds> (`maxage should be greater than 0`). Valkey
  // 8.0.0 already parses it (networking.c clientKillCommand), Redis since 7.4.
  'client.kill.maxage': { redis: '7.4.0', valkey: '8.0.0' },
  // Redis 7.0 range checks CLIENT KILL ID to 1..LONG_MAX with `client-id
  // should be greater than 0` (getRangeLongFromObjectOrReply), a non-integer
  // included; 6.2 takes any long long, `value is not an integer or out of
  // range` otherwise, and ID 0 means no id filter. Valkey forked with 7.0's.
  'client.kill.id-positive': { redis: '7.0.0', valkey: '7.2.0' },
  // Valkey 9.0 takes a list of ids after one CLIENT KILL ID and stops at the
  // first non-integer (`parseClientFiltersOrReply`): `ID 1 2` kills both,
  // `ID abc` is a syntax error and a trailing `ID` matches nothing.
  'client.kill.id-list': { valkey: '9.0.0' },
  // Valkey 8.0 accepts `primary` as a client type name next to `master`
  // (`getClientTypeByName`); Redis and Valkey 7.2 answer `Unknown client type`.
  'client.kill.type-primary': { valkey: '8.0.0' },
  'client.setinfo': { redis: '7.2.0', valkey: '7.2.0' },
  'client.no-touch': { redis: '7.2.0', valkey: '7.2.0' },
  // Valkey 8.1 gave CLIENT PAUSE its own handler (`clientPauseCommand`, arity
  // -3): arguments past the timeout are accepted, and the mode is read only
  // when it is the last one (`argc == 4`), so `CLIENT PAUSE 10 WRITE x` is an
  // ALL pause. Redis and Valkey 8.0 answer the subcommand syntax error.
  'client.pause.extra-args': { valkey: '8.1.0' },
  // Timeouts (`getTimeoutFromObjectOrReply`: CLIENT PAUSE and XREAD BLOCK in
  // milliseconds, BLPOP and the other blocking commands in seconds) refuse an
  // end time past LLONG_MAX with `timeout is out of range` from Redis 7.2
  // (timeout.c); 6.2 / 7.0 let `tval + now` overflow, and cast a seconds
  // timeout of LLONG_MAX ms or more (`inf`, `1e400`) to a negative one.
  'timeout.ms-overflow-check': { redis: '7.2.0', valkey: '7.2.0' },
  // The CLIENT LIST / CLIENT INFO line (`catClientInfoString`) gains fields
  // per version; the order is fixed and every field is always printed. From
  // the format strings of redis 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
  // Valkey 7.2.4, 8.0.0, 8.1.0, 9.0.0, and redis-server 7.0.15's own output:
  //  - 7.0 added `ssub`, `multi-mem`, `rbs`, `rbp` and `resp` (Valkey forked
  //    with them). `lib-name` / `lib-ver` arrived with CLIENT SETINFO
  //    (`client.setinfo`) and are printed, empty, before any SETINFO.
  //  - `watch` (after `multi`): Redis 7.4, Valkey 8.0.
  //  - `io-thread` (last): Redis 8.0 only.
  //  - `tot-net-in`, `tot-net-out`, `tot-cmds` (last): Valkey 8.0.
  //  - `capa` (after `flags`): Valkey 8.1.
  'client.list.redis7-fields': { redis: '7.0.0', valkey: '7.2.0' },
  'client.list.watch': { redis: '7.4.0', valkey: '8.0.0' },
  'client.list.io-thread': { redis: '8.0.0' },
  'client.list.net-stats': { valkey: '8.0.0' },
  'client.list.capa': { valkey: '8.1.0' },
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
  // `error.unknown-subcommand-dispatch-timing` below.
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
  // 7.0, `Print this help.` from Redis 7.2 / Valkey 7.2, for every container
  // (`helpReply`, src/commands/helpers.ts).
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
  // COMMAND DOCS summaries were rewritten for Redis 7.2 / Valkey 7.2 (`Get
  // information about a stream` became `Returns information about a
  // stream.`). Only the XINFO / XGROUP entries read it so far. Verified
  // against redis-server 7.0.15, 7.2, 8.0.6 and Valkey 8.0 / 9.0.
  'docs.summary-7.2-wording': { redis: '7.2.0', valkey: '7.2.0' },
  // XREAD / XREADGROUP's odd STREAMS tail: 6.2 and 7.0 say `Unbalanced XREAD
  // list of streams ... an ID or '$'` for both; 7.2 names the command and
  // gives XREADGROUP its `'>'`. Verified against redis-server 6.2.24, 7.0.15,
  // 7.2, 8.0.6 and valkey 8.0 / 9.0.
  'stream.xread-unbalanced-wording': { redis: '7.2.0', valkey: '7.2.0' },
  // XREAD's Unbalanced error lists `'+'` from Redis 8.0.0, although `XREAD
  // ... +` itself is 7.4 (`xread.plus-id`): 7.4.0 - 7.4.11 still say `an ID
  // or '$'`. Verified against redis-server 7.4.0, 7.4.11, 8.0.0 and 8.0.6.
  'stream.xread-unbalanced-plus-wording': { redis: '8.0.0' },
  'error.unknown-command-wording': { redis: '7.0.0', valkey: '7.2.0' },
  // An odd field/value tail the command table accepts but the command itself
  // refuses: 6.2 answers `wrong number of arguments for MSET` (MSET and
  // MSETNX alike) and `wrong number of arguments for XADD`; 7.0 moved both to
  // the standard arity wording (`... for 'mset' command`, `... for 'xadd'
  // command`). Verified against redis-server 6.2.24, 7.0.15 and 8.0.6, and
  // Valkey 8.0 / 9.0 (#492).
  'error.odd-pairs-arity-wording': { redis: '7.0.0', valkey: '7.2.0' },
  // A MONITOR connection is a replica to `processCommand`, which refuses it
  // any command flagged readonly, write or may_replicate. 6.2 misspells the
  // refusal (`Replica can't interract with the keyspace`, 6.2.14 / 6.2.24
  // server.c); 7.0 fixed it to `interact` (7.0.15 server.c and a live
  // redis-server 7.0.15, 7.2.4 / 7.4.4 / 8.0.0 and Valkey 8.0 / 9.0 server.c).
  'error.replica-keyspace-wording': { redis: '7.0.0', valkey: '7.2.0' },
  // The refusal of a command on a RESP2 connection in subscribed mode: 6.2
  // names the command (`c->cmd->name`, so `config` for CONFIG GET) and lists
  // `(P)SUBSCRIBE / (P)UNSUBSCRIBE`; 7.0 names the looked-up table entry
  // (`c->cmd->fullname`, `config|get`) and, with sharded Pub/Sub, lists
  // `(P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE` (redis 6.2.14 / 7.0.0 server.c
  // processCommand, valkey 8.0.0 the same as 7.0). Verified against
  // redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and Valkey 8.0 / 9.0
  // (#384, #514).
  'error.subscribed-context-wording': { redis: '7.0.0', valkey: '7.2.0' },
  'info.multi-section': { redis: '7.0.0', valkey: '7.2.0' },
  // The INFO keyspace line counts the hashes with field expirations after
  // `avg_ttl`: `subexpiry=` from Redis 7.4 (the release that added HEXPIRE),
  // `keys_with_volatile_items=` on Valkey 9.0. 7.2.4 / Valkey 8.0.0 print
  // neither (their `genRedisInfoString` / `genValkeyInfoString`).
  'info.keyspace.subexpiry': { redis: '7.4.0' },
  'info.keyspace.volatile-items': { valkey: '9.0.0' },
  'shutdown.now-force-abort': { redis: '7.0.0', valkey: '7.2.0' },
  'pubsub.sharded': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis 7.2 holds a push for the calling connection (its own PUBLISH to a
  // channel it subscribes to) until the command's reply is out, and inside
  // EXEC until the EXEC reply is (`server.pending_push_messages`); older
  // versions write it as it happens, so inside the EXEC array.
  'pubsub.resp3-publish-reply-first': { redis: '7.2.0', valkey: '8.0.0' },
  // Redis 7.0 made XCLAIM / XAUTOCLAIM drop a pending entry whose stream
  // entry was deleted (XAUTOCLAIM lists it in a third reply element). 6.2
  // claims it like any other and replies nil for it (the id under JUSTID),
  // so it stays pending. The same change capped XAUTOCLAIM COUNT at
  // LONG_MAX / 16 (it allocates the deleted-ids array up front); 6.2 accepts
  // up to LONG_MAX. Verified against redis-server 6.2.24, 7.0.15 and 8.0.6.
  'stream.xautoclaim-deleted-ids': { redis: '7.0.0', valkey: '7.2.0' },
  // Consumer group lag tracking, Redis 7.0: XGROUP CREATE / SETID take
  // ENTRIESREAD, XINFO GROUPS reports `entries-read` and `lag`, and XINFO
  // STREAM reports `max-deleted-entry-id`, `entries-added` and
  // `recorded-first-entry-id` (FULL: and each group's `entries-read` /
  // `lag`). 6.2 has none of them, and parses XGROUP options its own way:
  // CREATE takes exactly `MKSTREAM` (checked before the key), SETID nothing.
  // Verified against redis-server 6.2.24, 7.0.15 and 8.0.6.
  'stream.consumer-group-lag': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis 7.2 added the consumer's active time: XINFO CONSUMERS reports
  // `inactive`, and XCLAIM / XAUTOCLAIM create (and refresh) the consumer up
  // front, whether or not they claim anything. Through 7.0 the consumer is
  // looked up only when the first entry is claimed, so a call that claims
  // nothing leaves a missing consumer missing. Verified against redis-server
  // 6.2.24, 7.0.15, 7.2.4 and 8.0.6.
  'stream.consumer-active-time': { redis: '7.2.0', valkey: '7.2.0' },
  // BITCOUNT/BITPOS BYTE|BIT range modifier — Redis 7.0 / Valkey 7.2.
  'bit.byte-bit-range': { redis: '7.0.0', valkey: '7.2.0' },
  // HSCAN ... NOVALUES. Before it, `NOVALUES` is an unknown option (`syntax
  // error`) on every SCAN family command; with it, SCAN / SSCAN / ZSCAN answer
  // `NOVALUES option can only be used in HSCAN`. Valkey took it in 8.0.0, not
  // 9.0: checked against redis-server 6.2.24, 7.2, 7.4.0, 7.4.4, 8.0.6 and
  // valkey-server 7.2.14 (refuses), 8.0.0, 8.0.11, 8.1 and 9.0.6 (#214).
  'hscan.novalues': { redis: '7.4.0', valkey: '8.0.0' },
  // `XREAD ... +` reads the last entry (and XREADGROUP answers `The + ID is
  // meaningless ...` for it, after its NOGROUP check); before, `+` is an
  // invalid stream id. Valkey took it in 8.0.0 (valkey 8.0.0 t_stream.c;
  // 7.2.5 has no `+` branch). Verified against redis-server 7.2.4, 7.4.4,
  // 8.0.0 and valkey-server 8.0.0 / 9.0.0.
  'xread.plus-id': { redis: '7.4.0', valkey: '8.0.0' },
  // XREADGROUP / XCLAIM / XAUTOCLAIM announce a consumer they create
  // implicitly as `xgroup-createconsumer` from Redis 7.0
  // (`streamCreateConsumer(..., SCC_DEFAULT)`); 6.2 creates it silently
  // (`streamLookupConsumer(..., SLC_NONE)`) and only XGROUP CREATECONSUMER
  // publishes the event (redis 6.2.14 / 7.0.15 / 7.2.4 t_stream.c).
  'stream.implicit-consumer-event': { redis: '7.0.0', valkey: '7.2.0' },
  // The consumer-group lag port (`streamReplyWithCGLag`). Redis 7.4.2 made
  // the counter estimate refuse an id behind the last tombstone, dropped the
  // tombstone helper's `first_id > max_deleted_entry_id` shortcut, and made
  // XREADGROUP check for tombstones after the group's last id instead of the
  // delivered one (redis 7.4.1 / 7.4.2 t_stream.c). Valkey took it in 8.0.3.
  // Backports one minimum version cannot express: redis 7.2.7+ has it too
  // (7.2.4, the pinned 7.2, does not), and valkey 8.1.0 lacks it (8.1.1 has
  // it).
  'stream.group-lag-tombstone-check': { redis: '7.4.2', valkey: '8.0.3' },
  // Redis 7.4.4 also stopped XREADGROUP counting while the group's last id
  // is behind the first entry (`streamCompareID(&group->last_id,
  // &s->first_id) >= 0`; absent from 7.4.2 / 7.4.3). Valkey took it with the
  // tombstone check in 8.0.3 (and 8.1.1, not 8.1.0).
  'stream.group-lag-read-first-id-check': { redis: '7.4.4', valkey: '8.0.3' },
  // Redis 7.4.2 also answers the lag directly for an emptied stream (0) and
  // when both the group's last id and the last tombstone are behind the
  // first entry (the stream length); 7.4.0 / 7.4.1 do not, and redis 7.2.7+
  // backported it. Valkey 9.0.0 has not taken these.
  'stream.group-lag-shortcuts': { redis: '7.4.2' },
  'cluster.multi-db': { valkey: '9.0.0' },
  // A standalone (`cluster-enabled no`) server refuses READONLY with `This
  // instance has cluster support disabled` on every version up to Valkey 8.0;
  // READWRITE skipped that check until Redis 7.0 added it (6.2 answers +OK).
  // Valkey 8.0 dropped the check from both, so they answer +OK there.
  // Verified against the redis 6.2.24, 7.0.15 (local), 8.0.6 and valkey
  // 7.2.5, 8.0.0, 8.0.6, 9.0.0 sources (`readonlyCommand` /
  // `readwriteCommand` in cluster.c) and the transcripts in #537.
  'cluster.standalone-readwrite-refused': { redis: '7.0.0', valkey: '7.2.0' },
  'cluster.standalone-readonly-allowed': { valkey: '8.0.0' },
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
  // Redis 7.0 gave scripts `redis.REDIS_VERSION` / `redis.REDIS_VERSION_NUM`;
  // Valkey has them from its first release, with the Redis version it forked
  // (7.2.4), plus `SERVER_NAME` and `VALKEY_VERSION` / `VALKEY_VERSION_NUM`.
  // Checked against redis-server 6.2.24 (none), 7.0.15, 7.2.16, 7.4.11, 8.0.6
  // and valkey-server 7.2.14, 8.0.11, 9.0.6 (see `luaRedisProps` in
  // src/core/lua-runtime.ts).
  'script.redis-version-props': { redis: '7.0.0', valkey: '7.2.0' },
  // Valkey 8.0 shortened EVALSHA's miss to `-NOSCRIPT No matching script.`;
  // Redis and Valkey 7.2 add ` Please use EVAL.` Checked against redis-server
  // 8.0.6 and valkey-server 7.2.14, 8.0.11, 9.0.6.
  'script.noscript-short-wording': { valkey: '8.0.0' },
  // Redis 7.0 taught a script's return value the `{big_number=}` and
  // `{verbatim_string=}` tables; 6.2 converts them like any other table
  // without an array part, to an empty array (`*0`), at top level and nested.
  // `{double=}`, `{map=}` and `{set=}` convert on 6.2 already. Checked against
  // redis-server 6.2.24, 7.0.15, 7.4.11, 8.0.6 and valkey-server 7.2.14,
  // 8.0.11, 9.0.6.
  'script.big-number-verbatim-returns': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis 7.0 lets a script open with a `#!lua [flags=...]` shebang line,
  // which it skips before compiling, keeping the line feed so line numbers
  // hold (`#!lua\nreturn +` is a compile error at `user_script:2`); 6.2
  // compiles it as Lua (`unexpected symbol near '#'`). Checked against
  // redis-server 6.2.24, 7.0.15, 8.0.6 and valkey-server 7.2.14, 8.0.11,
  // 9.0.6.
  'script.shebang': { redis: '7.0.0', valkey: '7.2.0' },
  // Valkey 8.1 made the shebang name a scripting engine: it reads the
  // `flags=` options first, then looks the engine up by name, ignoring case,
  // so `#!LUA` runs and `#!notlua` is `Could not find scripting engine
  // 'notlua'`. Redis and Valkey up to 8.0 check the engine first and accept
  // only `#!lua` (`Unexpected engine in script shebang: #!notlua`). Checked
  // against redis-server 7.0.15, the #536 transcripts (valkey-server 8.0.11
  // and 9.0.6) and the Valkey 8.1.0 / 9.0.0 sources (`evalRegisterNewScript`).
  'script.shebang-engine-lookup': { valkey: '8.1.0' },
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
  // Redis 7.2 added listpack sets (Valkey forked with them). Two ordering
  // rules come with it (#504). SADD creates an intset only when its member
  // count fits set-max-intset-entries (`setTypeCreate()`'s size hint), so with
  // the limit at 2 `SADD s 3 1 3` is the listpack `3 1` on 7.2+ and the
  // intset `1 3` on 6.2 / 7.0. And SPOP's "keep the few survivors" rebuild
  // makes a non-intset set a listpack, where 6.2 / 7.0 start the rebuilt set
  // from its first survivor, so all-integer survivors become a sorted intset.
  // Verified against redis-server 6.2.24, 7.0.15, 7.4, 8.0.6 and Valkey 8.0 /
  // 9.0.
  'set.listpack-encoding': { redis: '7.2.0', valkey: '7.2.0' },
  // Redis 8.0 / Valkey 8.0: a non-STORE SUNION / SDIFF (and so SPOP with a
  // count that covers the set) builds its result as a hashtable when any
  // source is not an intset, instead of starting from an empty intset. The
  // integers of `SADD u x 3 1; SREM u x` then stay `3 1` in `SUNION u` rather
  // than sorting to `1 3` (#504). Valkey 8.1+'s small hashtable walks in
  // insertion order, so Valkey 9 gives exactly that; a Redis 8.0 / Valkey 8.0
  // dict order is undefined. Verified against redis-server 7.2.16, 7.4,
  // 8.0.6 and Valkey 7.2.14 / 8.0 / 8.1 / 9.0.
  'set.union-diff-hashtable': { redis: '8.0.0', valkey: '8.0.0' },
  // Valkey 8.0: a SET whose EX / PX / EXAT / PXAT deadline is already past
  // (`checkAlreadyExpired`) writes nothing. It deletes a key that exists,
  // and creates none, so no `set` / `expire` is published. Redis (6.2-8.0)
  // and Valkey 7.2 write the key with the past TTL and publish `set` and
  // `expire`, and the key expires on its next access (#527). Verified against
  // the Valkey 7.2.14, 8.0.0, 8.0.11, 8.1.0, 9.0.0 and 9.0.6 sources
  // (`setGenericCommand`).
  'set.past-deadline-deletes': { valkey: '8.0.0' },
  // Valkey 8.1: a key deleted because a command gave it a deadline already
  // past (SET, GETEX EXAT / PXAT, the EXPIRE family) is published as
  // `expired` (class `x`), not `del` (class `g`):
  // `deleteExpiredKeyFromOverwriteAndPropagate` switched notifications in
  // 8.1.0. Redis and Valkey 7.2 / 8.0 publish `del` (#527). Verified against
  // the Valkey 8.0.0, 8.0.11, 8.1.0, 9.0.0 and 9.0.6 sources.
  'notify.past-deadline-expired-event': { valkey: '8.1.0' },
  // XREAD / XREADGROUP with BLOCK from a script: up to Redis 7.2 / Valkey 7.2
  // `xreadCommand` refuses it while parsing its options, as soon as it sees
  // BLOCK (before the timeout, the key and the group are looked at), with
  // `<command as sent> command is not allowed with BLOCK option from scripts`
  // (redis@7.2.4 and valkey@7.2.5 src/t_stream.c). Redis 7.4 / Valkey 8.0
  // dropped the check: the script client's `CLIENT_DENY_BLOCKING` turns the
  // block into the timeout reply at once, even with BLOCK 0 (#563). Verified
  // against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
  // valkey-server 7.2.5, 8.0.0, 9.0.0.
  'script.stream-block-allowed': { redis: '7.4.0', valkey: '8.0.0' },
  // The commands flagged CMD_MAY_REPLICATE (see src/core/compatibility/
  // may-replicate.ts, #568). Redis 7.0 made SCRIPT a container whose
  // subcommands lack the flag, and flagged its new SPUBLISH and FCALL
  // (7.0.15 commands.c, 7.2.4 / 7.4.4 / 8.0.0 and valkey 7.2.5 commands.def).
  'may-replicate.redis7-table': { redis: '7.0.0', valkey: '7.2.0' },
  // Valkey 8.0 flags CLUSTER SETSLOT (valkey@8.0.0 src/commands.def).
  'may-replicate.cluster-setslot': { valkey: '8.0.0' },
  // Valkey 9.0 adds CLUSTER SYNCSLOTS, flagged too (valkey@9.0.0
  // src/commands.def).
  'may-replicate.cluster-syncslots': { valkey: '9.0.0' },
  // Redis 8.0 and Valkey 8.1 convert score-range bounds (`zslParseRange()`)
  // and numeric SORT elements / BY weights with fast_float instead of C
  // `strtod`: hex floats (`0x10`) and leading whitespace are refused there,
  // and Redis 8.0's SORT stops seeing `ERANGE` (`1e400` sorts as inf).
  // redis@7.4.4 t_zset.c:556-591 / sort.c:479-484 vs redis@8.0.0
  // t_zset.c:541-576 / sort.c:504, valkey@8.0.0 vs valkey@8.1.0. Checked
  // against redis-server 6.2.14 .. 8.0.0 and valkey-server 8.0.0 / 9.0.0 (#534,
  // #513). See src/core/double-token.ts.
  'double.fast-float': { redis: '8.0.0', valkey: '8.1.0' },
  // Valkey 8.1's `valkey_strtod()` is fast_float with no `strtod` fallback, so
  // `string2d()` (ZADD, ZINCRBY, WEIGHTS, GEO*) refuses hex floats too, and
  // SORT, which now resets errno, refuses an empty element (EINVAL) and an
  // overflow or underflow to zero (ERANGE) but takes a subnormal. Redis 8.0
  // keeps the fallback (redis@8.0.0 util.c:660-681). valkey@8.1.0
  // util.c:599-609, sort.c:484-485; checked against valkey-server 9.0.0.
  'double.valkey-strtod': { valkey: '8.1.0' },
  // Container HELP texts (#491), from the real servers' help arrays. Redis
  // 7.0 rewrote CLIENT HELP's KILL TYPE as upper case, fixed `<timout>` and
  // added TRACKING's `[NOLOOP]` (redis@7.0.0 networking.c clientCommand), and
  // reworded SLOWLOG GET for `-1 mean all` (slowlog.c). Valkey 8.0 added CLIENT
  // CAPA and SCRIPT SHOW and re-wrapped SCRIPT FLUSH (valkey@8.0.0 eval.c);
  // Valkey 8.1 the CLIENT LIST filters, the CLIENT KILL ID list and
  // IMPORT-SOURCE; Valkey 9.0 the remaining CLIENT KILL / LIST filters, CLIENT
  // HELP punctuation and ACL SETUSER's `<rule>` (valkey@9.0.0 acl.c). Redis
  // 7.4 / Valkey 8.0 dropped FUNCTION LIST's `* Library description` line
  // (absent in redis@7.4.0 functions.c). Captured from redis-server 6.2.14 ..
  // 8.0.0 and valkey-server 8.0.0, 8.1 and 9.0.0.
  'help.client-redis7-text': { redis: '7.0.0', valkey: '7.2.0' },
  'help.client-capa': { valkey: '8.0.0' },
  'help.client-valkey81-filters': { valkey: '8.1.0' },
  'help.client-valkey90-filters': { valkey: '9.0.0' },
  'help.slowlog-get-all': { redis: '7.0.0', valkey: '7.2.0' },
  'help.acl-setuser-rules': { valkey: '9.0.0' },
  'help.script-valkey-text': { valkey: '8.0.0' },
  'help.function-no-library-description': { redis: '7.4.0', valkey: '8.0.0' },
  // Redis 7.0.1 appended `... or use negative to start from the end of the
  // list` to LPOS's `RANK can't be zero` (redis@7.0.0 vs 7.0.1 t_list.c);
  // 6.2 and 7.0.0 end at `2 from the second, ...`. Checked against
  // redis-server 6.2.14, 6.2.24, 7.0.0, 7.0.1, 7.0.15, 8.0.0 and valkey-server
  // 8.0.0, 9.0.0.
  'error.lpos-rank-zero-hint': { redis: '7.0.1', valkey: '7.2.0' },
  // Redis 7.0 quotes the command in `invalid expire time in 'set' command`;
  // 6.2 writes `invalid expire time in set` (SET / SETEX / PSETEX / GETEX).
  // Checked against redis-server 6.2.14, 6.2.24, 7.0.0, 7.0.15, 8.0.0 and
  // valkey-server 8.0.0, 9.0.0.
  'error.expire-time-quoted-wording': { redis: '7.0.0', valkey: '7.2.0' },
  // Valkey 9.0 names the refused command in `Command 'multi' not allowed
  // inside a transaction` (a `no_multi` command sent inside MULTI); Redis
  // 7.0+ and Valkey 8.0 / 8.1 leave it out. Checked against redis-server
  // 7.0.15, 8.0.0 and valkey-server 8.0.0, 8.1.0, 9.0.0.
  'error.no-multi-named-wording': { valkey: '9.0.0' },
  // Valkey 8.0 empties the MULTI queue when a queued command is refused
  // (`flagTransaction` calls `resetClientMultiState`, valkey@8.0.0
  // multi.c:113-117); Redis only flags the transaction and keeps the queue
  // (redis@8.0.0 multi.c:86-90). In a cluster, EXEC's slot check then sees
  // no commands, so a refused transaction whose earlier commands span slots
  // answers EXECABORT on Valkey and CROSSSLOT on Redis (#519). Checked against
  // redis-server 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0, 9.0.0.
  'transaction.refusal-empties-queue': { valkey: '8.0.0' },
  // Redis 7.0 stops queueing MULTI commands once a WATCHed key was touched
  // (`queueMultiCommand` returns on CLIENT_DIRTY_CAS as well as
  // CLIENT_DIRTY_EXEC, redis@7.0.0 multi.c:66; 6.2 checks only DIRTY_EXEC,
  // redis@6.2.14 multi.c:66). In a cluster, EXEC's slot check then sees only
  // what was queued before the touch, so a dirty WATCH whose later commands
  // span slots answers `*-1` from 7.0 and CROSSSLOT on 6.2 (#519). Checked
  // against redis-server 6.2.14, 7.0.15, 8.0.0 and valkey-server 8.0.0.
  'transaction.watch-dirty-stops-queue': { redis: '7.0.0', valkey: '7.2.0' },
  // Redis 7.0's config table (#506): the ziplist parameters renamed to
  // listpack, the old names kept as aliases (`hash-max-ziplist-entries` ->
  // `hash-max-listpack-entries`; 6.2 knows only the ziplist names), and new
  // parameters such as `maxmemory-clients` and `bind-source-addr`
  // (redis@7.0.15 config.c:3034-3132, absent from redis@6.2.14 config.c).
  'config.redis7-table': { redis: '7.0.0', valkey: '7.2.0' },
  // CONFIG GET takes several patterns from 7.0 and answers an exact (non-glob)
  // name with the name as sent; 6.2 takes exactly one pattern
  // (redis@6.2.14 config.c:2599-2602) and replies with the table spelling.
  'config.get.multi-pattern': { redis: '7.0.0', valkey: '7.2.0' },
  // `loglevel nothing` (LL_NOTHING): redis@7.2.0 config.c:78-84; absent from
  // 7.0.15; in every Valkey (valkey@7.2.4 config.c:83).
  'config.loglevel-nothing': { redis: '7.2.0', valkey: '7.2.0' },
  // `maxmemory-samples` capped at 64 instead of INT_MAX: redis@7.4.0
  // config.c:3163 (7.2.11 still INT_MAX), valkey@8.0.0 (7.2.4 INT_MAX).
  'config.maxmemory-samples-max-64': { redis: '7.4.0', valkey: '8.0.0' },
  // Blocking timeouts in seconds round up to whole milliseconds
  // (`ceill(ftval * 1000.0)`) from redis@7.0.8 timeout.c:174; redis@7.0.7 and
  // 6.2.14 truncate, so a timeout under 1 ms (`0.0001`) blocks forever there.
  'timeout.ceil-ms': { redis: '7.0.8', valkey: '7.2.0' },
  // LPOS RANK is read with `getRangeLongFromObjectOrReply(-LONG_MAX,
  // LONG_MAX)` from redis@7.2.0 t_list.c:974 (valkey@7.2.4 the same):
  // RANK -9223372036854775808 is out of range. redis@7.0.15 takes it, and its
  // negation wraps to itself, so it acts as RANK -1.
  'lpos.rank-range-check': { redis: '7.2.0', valkey: '7.2.0' },
  // A small sorted set (a listpack) stores an integral score as a listpack
  // integer (`double2ll`, redis@7.0.15 t_zset.c:1028-1053 zzlInsertAt), so a
  // score of -0 reads back as 0. Redis 6.2's ziplist stores d2string()'s text,
  // `-0`, and keeps the sign (redis@6.2.14 t_zset.c:1034-1055). A skiplist
  // keeps -0 on every version (#512).
  'zset.listpack-integer-scores': { redis: '7.0.0', valkey: '7.2.0' },
}
