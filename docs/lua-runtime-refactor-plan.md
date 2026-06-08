# Lua Runtime Refactor Plan

Audit of `src/core/lua-runtime.ts` (+ `src/commands/scripts.ts`) adapter/hack
points, with a plan to push lib-appropriate concerns into the internal
`lua-redis-wasm` library (repo: `github.com/fatal10110/lua-redis-wasm`,
currently `^1.2.2`) instead of working around them in this project.

## Current lib interface (v1.2.2)

- `ReplyValue = null | number | bigint | Buffer | {ok:Buffer} | {err:Buffer} | ReplyValue[]`
  — **errors are flattened to a single `Buffer`; no structured code/message.**
- Host: `{ redisCall, redisPcall, log }`; handler `(args: Buffer[]) => ReplyValue`.
  Throw or return `{err}` to signal an error to Lua.
- `evalWithArgs(script, keys, args): ReplyValue` — **no script-sha / error-context param.**
- `EngineOptions` supports limits (`_set_limits`: maxFuel / maxReplyBytes / maxArgBytes) — not currently used here.

The lib is internal and editable. Anything that is genuinely *Redis scripting
subsystem behavior* (error decoration, error structure, argument validation)
belongs in the lib, not in JS post-processing. Do not add compatibility shims,
feature checks, or source/regex adapters in `js-redis-server` to support the old
`lua-redis-wasm` interface; change the lib and then simplify this project.

---

## Hack catalog

### H1 — `scriptSha` threaded through every reply conversion (HACK → move to lib)
`lua-runtime.ts:63,88-92,136-177,190-226`. `scriptSha` is woven recursively
through `redisValueToLuaReply` / `formatRedisErrorValue` / `redisErrorToLuaReply`
purely so an error escaping `redis.call` gets the suffix
`script: <sha>, on @user_script:1.` appended. Real Redis appends this in the
scripting subsystem at the call→error boundary; `pcall` errors (returned to the
script as a table) get no decoration. This is engine behavior.
**Fix:** lib `evalWithArgs` (or engine option) takes a `scriptSha` and decorates
errors that propagate out of a `redis.call`. Host handlers return undecorated
errors. Removes sha from the entire JS conversion path.

### H2 — `{err: Buffer}` flattening forces regex re-parse (HACK → lib interface change)
`lua-runtime.ts:201-208` `luaErrorToRedisValue` and `scripts.ts:290-295`
`luaRuntimeError` both run `/^([A-Z][A-Z0-9]*) (.+)$/` to split a `CODE message`
string back into structured `{code, message}` — undoing a flatten the project
itself did on the way in. Lossy round-trip (a message that legitimately starts
with an uppercase word is misclassified as a code).
**Fix:** widen lib reply error to carry structure, e.g.
`{ err: Buffer, code?: Buffer }` (back-compat: `code` optional, `err` stays the
message). Project reads `code` directly; both regexes deleted.

### H3 — zero-arg `redis.call()` short-circuited in lib, recovered by source regex (HACK → lib)
The WASM engine validates an empty `redis.call()` itself and returns generic
`"ERR redis.call requires arguments"` **without invoking the host handler** — so
`lua-runtime.ts:65-67`'s correct `ScriptCallNoCommandError` is dead for that
path. To restore the real Redis message *and* the call/pcall distinction,
`scripts.ts:298-331` (`normalizeRedisNoArgsError` + `firstZeroArgRedisCallKind`)
**regex-scans the raw Lua source** (`/\bredis\s*\.\s*(p?call)\s*\(\s*\)/`). Brittle:
matches the first textual occurrence, not the one that actually executed; breaks
on multiple calls, comments, or strings containing the pattern.
**Fix (lib):** either delegate the zero-arg case to the host handler (preferred —
host already produces the exact message via `ScriptCallNoCommandError`), or have
the engine emit the real message with the correct call/pcall mode and sha
context. Deletes `normalizeRedisNoArgsError`, `firstZeroArgRedisCallKind`, and
the dead zero-arg branch in `lua-runtime.ts`.

### H4 — `normalizeScriptCommandValue` MOVED rewrite (KEEP in project, but relocate)
`lua-runtime.ts:179-188` rewrites a `MOVED` error from a command run inside a
script into a generic "non local key in a cluster node" message. This is
*cluster-routing policy of this server*, not Lua-engine behavior — the lib has no
cluster concept. Keep it server-side, but move it next to the executor/cluster
layer (where MOVED originates) rather than in the Lua bridge so the bridge stays
a pure value translator.

### H5 — RESP error wire-formatting duplicated in the bridge (KEEP, dedupe)
`lua-runtime.ts:228-264` `formatRedisError` / `sanitizeErrorText` re-implement
code-prefix dedup + CRLF stripping that the project's RESP encoder already owns.
Not a lib concern, but it's duplicated formatting logic living in the wrong file.
**Fix:** route through the shared RESP error formatter (wherever
`RedisValue.error` is serialized) instead of a private copy here.

### H6 — RESP3→RESP2 flattening (KEEP — correct domain logic, not a hack)
`redisValueToLuaReply` map→flat-kv-array, set→array, double/big-number→Buffer,
`formatNumber` (`inf`/`-inf`/`nan`/`-0`). This mirrors real Redis: Lua sees the
RESP2 flattening of replies. Genuine server semantics; leave in the bridge.
(Could move into the lib only if the lib modeled RESP3 reply kinds, which it
deliberately does not — out of scope.)

---

## Target end-state

`lua-runtime.ts` becomes a thin two-way translator:
`RedisValue ⇄ ReplyValue` (H6 only), plus host dispatch into `executor.plan` /
`executePlanSync`. No sha threading, no error regexes, no source scanning, no
private RESP formatter. `scripts.ts` `runLuaScript` just calls `eval` and maps
the reply; `normalizeLuaReplyError`, `luaRuntimeError`, `normalizeRedisNoArgsError`,
`firstZeroArgRedisCallKind` all deleted or collapsed to a 3-line structured map.

---

## Sequencing

**Phase 0 — lib changes (`lua-redis-wasm`, internal breaking changes allowed):**
1. H3: zero-arg `redis.call`/`pcall` delegates to host handler (or emits real
   message + mode). Add a test in the lib for both modes.
2. H2: extend error reply to `{ err: Buffer, code?: Buffer }`; populate `code`
   from the host-returned error and from engine-internal errors.
3. H1: `evalWithArgs(script, keys, args, { scriptSha })`; decorate
   `redis.call`-propagated errors with `script: <sha>, on @user_script:1.`;
   leave `pcall`-returned error tables undecorated.
4. Publish; bump dependency here to the new internal release.

**Phase 1 — project cleanup (depends on Phase 0):**
5. Drop `sha` from `redisValueToLuaReply` / `formatRedisErrorValue` /
   `redisErrorToLuaReply`; remove `scriptErrorMessage`.
6. Delete `luaErrorToRedisValue` regex; read `code` from structured reply.
7. Delete `normalizeRedisNoArgsError` + `firstZeroArgRedisCallKind` +
   `normalizeLuaReplyError`'s no-arg branch; simplify `luaRuntimeError`.
8. Relocate H4 MOVED rewrite to cluster/executor layer.
9. Route H5 through shared RESP error formatter.

**Phase 2 — verification:**
10. `npm test` + `npm run test:integration:mock`; focus
    `tests/commands-scripts.test.ts`, `tests-integration/ioredis/scripts.test.ts`.
    Confirm call vs pcall zero-arg messages, sha suffix, and structured error
    codes (WRONGTYPE, NOSCRIPT, MOVED→generic) all still match Redis.

## Risk / notes

- Phases are ordered: project cleanup is blocked on the published lib bump. Do
  not land project-side compatibility gates for both 1.2.2 and the new lib
  interface; the cleanup should target the new internal API only.
- H2/H3 may be breaking lib-interface changes if that produces the cleanest API;
  this is acceptable because `lua-redis-wasm` is internal.
- H1 sha decoration must match Redis exactly incl. the trailing
  `, on @user_script:1.`; cover with a lib test, not just here.
- Existing behavior (obs 459) — call/pcall zero-arg messages already matched
  Redis via the regex hack; the lib fix must preserve that exact output.

---

## Implementation status (done)

Shipped against `lua-redis-wasm` `1.3.0` (local source at
`../redis-lua-wasm`, npm-linked into this repo; **not yet published**).

Design refinement vs the plan: H1 decoration is driven by a new wire tag rather
than threaded sha. The C abort paths (`luaL_loadbuffer` / `lua_pcall` failures in
`eval` and `eval_with_args`) emit `REPLY_SCRIPT_ERROR = 0x06`; the engine
decorates *only* that tag, so call-aborts get `script: <sha>, on @user_script:N.`
while pcall error *values* the script returns stay clean. The script sha is the
engine's own `computeSha1Hex` — the project passes none.

- **H1 (done)** — lib owns all error decoration. `eval()` lost its `sha`
  argument; no sha threading remains in the project.
- **H2 (done)** — `ReplyValue` error is `{ err: Buffer; code?: Buffer }`.
  `codec.ts` splits the leading code on decode and re-joins it on encode. The
  project reads `code` directly; both `/^([A-Z][A-Z0-9]*) (.+)$/` regexes are
  gone.
- **H3 (done)** — zero-arg `redis.call()/pcall()` no longer short-circuits in C;
  it is dispatched to the host with `[]`, so `ScriptCallNoCommandError` and the
  call/pcall distinction come for free. `firstZeroArgRedisCallKind` /
  `normalizeRedisNoArgsError` (source-regex hack) deleted.
- **H4 (kept)** — `normalizeScriptCommandValue` (MOVED→generic) stays at the host
  dispatch in `lua-runtime.ts`, which is already the right layer (the pure
  translator no longer touches it); it no longer carries sha.
- **H5 (no-op)** — error wire-formatting (code-prefix join + CRLF sanitize) is
  already owned by `src/core/resp-encoder.ts`; the duplicate was simply removed
  from `lua-runtime.ts`.

Tests: lib 138/138 (incl. new zero-arg-delegation + decoration/code-split
cases); project 89 unit + 152 mock integration green, including the exact
contract in `tests-integration/ioredis/scripts.test.ts`.

**Follow-up required:** publish `lua-redis-wasm@1.3.0` to the registry. Until
then this repo only builds/tests via the local `npm link`; a fresh `npm install`
resolves `^1.3.0` against a registry that still has `1.2.2`.
