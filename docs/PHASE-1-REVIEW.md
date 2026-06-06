# Phase 1 Slice Review

Scope: new `src/core/` (executor, registry, schema, value/result/error/context,
response-stream, policies) + test. Old code untouched, new path is a standalone
scaffold per plan.

## Strengths

- `RedisValue` ADT matches plan; helper factories good.
- `CommandRegistry` is open: `register` / `override` / `registerAll`. Throws on
  duplicate without `override`. Acceptance criterion met.
- `ExecutionPolicy` before/after hook supports cluster/lua/transaction/readonly
  policy layering.
- `CommandPlan` produced before `execute`, matches pipeline (normalize → lookup
  → parse → keys → policies → execute).
- `ExecutorResult = RedisResult | ResponseStream` baked into executor signature
  from day 1.
- Tests cover happy path, unknown command, arity, policy short-circuit, override
  semantics.

## Bugs / Inconsistencies

### 1. `resolveExecutionResult` is dead

`src/core/command-executor.ts:103-111`

```ts
if (isResponseStream(result)) return result
return result
```

Both branches return the same value. The `await` happens at the call site.
Either inline `await` or have this function actually do something (e.g. wrap
streams). Confusing as written.

### 2. `executePlan` does not catch `RedisCommandError`; `executeRaw` does

`src/core/command-executor.ts:60-86` vs `44-58`

Direct callers (future MULTI batch loop will call `executePlan` repeatedly)
must catch themselves or errors escape. Asymmetric. Pick one place; recommend
`executePlan` always catches and converts.

### 3. `afterExecute` policies silently skipped for streams

`src/core/command-executor.ts:75-83`

`beforeExecute` runs for streaming commands but `afterExecute` is bypassed via
the `isResponseStream` early-return. Telemetry/audit on MONITOR/SUBSCRIBE
blind. Either document or add `onStream(plan, ctx, stream)` hook now.

### 4. Sync commands forced through async wrapper

`src/core/command-executor.ts:60-63`

`CommandExecutionResult` allows sync `RedisResult` but `executePlan` always
returns `Promise`. Synchronous commands pay a microtask. Not a correctness
bug; just guarantees the entire path is async even when commands aren't. Fine
for mock — call out in plan.

### 5. Type/value mismatch: `t.integer` rejects bigint input

`src/core/command-schema.ts:110-132`

`Number.isSafeInteger` gate. But `RedisValue.integer` accepts `number | bigint`
(`src/core/redis-value.ts:25`). Commands cannot input large ints
declaratively. Add `t.bigInteger()` or widen `t.integer` to return
`number | bigint`.

### 6. `createNoopParkHandler` ignores `timeoutMs` AND `signal`

`src/core/redis-context.ts:23-25`

```ts
return async request => request.waitFor
```

Tests using this think they tested parking; they tested promise pass-through.
Either rename `createIdentityParkHandler` or honor the contract (await with
timeout/abort). Footgun.

### 7. `RedisExecutionContext` fully generic with `unknown` defaults

`src/core/redis-context.ts:11-21`

Test passes `{}` for `db` / `server` / `session`. Real commands will need
`db.getString(...)`, `server.scriptCache`, `session.watch(...)`. With
`unknown` defaults, every command body will cast. Either:

- Define concrete `RedisDatabase` / `RedisServerState` / `ClientSession`
  interfaces now (even minimal), or
- Make `RedisExecutionContext` non-generic and rely on per-command capability
  declarations.

### 8. `t.variadic` is greedy without breakout

`src/core/command-schema.ts:178-198`

Consumes until end. `t.object({ values: t.variadic(...), key: t.key() })` is
impossible — variadic eats everything including the trailing key. Commands
like `GEORADIUS ... STORE key` cannot be expressed. Add `until` predicate or
`lookahead` schema combinator. Document the limitation in `command-schema.ts`
in the meantime.

### 9. `t.union` swallows non-first `RedisCommandError`

`src/core/command-schema.ts:220-246`

Stops at first `RedisCommandError`. If branch B is the intended one and throws
a more useful error, branch A's error wins. Confusing for parser authors.
Either always rethrow the most specific, or document.

### 10. `parseCommandArgs` enforces "all input consumed"

`src/core/command-schema.ts:53-56`

Good. But error class is `WrongNumberOfArgumentsError`. Extra trailing junk
also throws this. Matches real Redis behavior, OK.

### 11. `UnknownRedisCommandError` joins binary args as utf8

`src/core/redis-error.ts:37-42`

`args.map(arg => arg.toString())` — binary mojibake. Real Redis truncates and
hex-escapes. Mock fidelity drift.

### 12. `defineCommand` lowercases name, then `CommandRegistry.register` lowercases again

`src/core/command-definition.ts:46-53`, `src/core/command-registry.ts:10`

Harmless, just double work. Pick one site.

### 13. `CommandRegistry.getNames` returns lowercase only

`src/core/command-registry.ts:43-45`

Original case lost. Acceptable since `defineCommand` normalizes, but `COMMAND
INFO` later may want canonical-case names.

## Missing for the Slice

### 14. No RESP encoder

`RedisValue` exists but nothing turns it into bytes. Plan acceptance criterion
"RedisValue represents every RESP3 reply shape. RESP2 downgrade lives in the
encoder" is untestable until at minimum a stub encoder lands. Recommend small
`RespEncoder` in Phase 1 covering RESP2 subset, with a TODO for RESP3 shapes.

### 15. No bridge from existing `Session` to new executor

Confirmed expected per plan ("keep old system temporarily"). Just flag that
nothing currently runs Phase-1 code in production paths. Phase 4 will do the
splicing.

### 16. `ResponseStream` semantics under-specified

`src/core/response-stream.ts`

- Does `frames(signal)` return a fresh iterator per call, or a shared one?
- Multiple readers? Single reader assumption?
- Backpressure model?

Plan punts to "Open Questions". OK, but add a doc comment in
`response-stream.ts` so implementers don't pick incompatible answers.

### 17. `CommandFlag` union lacks `pubsub` / `subscribed`

`src/core/command-definition.ts:6-16`

Phase 6.9 will need them. Not blocking Phase 1 acceptance but cheap to add
now.

### 18. `CommandCapabilities.scriptKeys` declared but unused

`src/core/command-definition.ts:18-23`

`createPlan` calls `definition.keys(args)` unconditionally. For EVAL the
static keys list is meaningless until runtime `numkeys` parsed. The
capability flag has no effect on executor yet. Wire the dispatch or remove
until needed.

### 19. `RedisResult.options.disconnect` declared but no path uses it

`src/core/redis-result.ts:3-6`

Fine for now, but flag as "encoder must honor `close` and `disconnect`" in
plan.

### 20. No test for `Promise<RedisResult>` execute path or `ResponseStream` path

`tests/core-command-executor.test.ts`

Test covers sync results only. Async + stream paths are the
architecturally-load-bearing additions. Add one test each.

## Verdict

Phase 1 deliverable matches plan's "add new core beside old code" goal.
Type-level shape is right: `RedisValue` ADT, `CommandPlan`, registry with
override, policy pipeline, stream-aware result type, park primitive.

Most issues are footguns + missing tests, not direction errors. Fix #1, #2,
#5, #6, #7, #14, #20 before Phase 2 starts — they all become harder once
commands depend on these shapes.

**Blocking before Phase 2:** #2 (`executePlan` error path), #7 (concrete
context types), #14 (encoder smoke test).

**Cheap to land now:** #1 (delete), #6 (rename or honor), #12 (one-site
lowercase), #20 (two more tests).

**Defer with explicit notes:** #3 (`onStream` hook), #8 (`t.variadic`
lookahead), #16 (stream semantics doc), #17 (pubsub flags).
