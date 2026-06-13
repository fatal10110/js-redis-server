# Lua Debugger Plan — step-debug Lua scripts in VS Code while running the JS Redis mock

## Context

Today a developer points `ioredis` at the js-redis-server mock, loads a Lua script, runs `EVAL`, and has **no way to step through the Lua** — only `print`/log. We own the Lua interpreter as a WASM package (`redis-lua-wasm`, Lua 5.1 via Emscripten) *and* the mock that drives it, so we can wire a real VS Code debugging experience end-to-end: gutter breakpoints, step in/over/out, call stack, a Variables panel (KEYS/ARGV/locals/upvalues/nested tables), and watch/evaluate — all while the script executes inside the normal mock `EVAL` pipeline (`redis.call` still hits the live keyspace).

**Why it's feasible.** `ldebug.c` is already compiled into the WASM (`LUA_CORE` in `wasm/build/build.sh`), so the C-level debug API (`lua_sethook`, `lua_getstack`, `lua_getinfo`, `lua_getlocal`) exists in the binary. Two things block it today: (1) the Lua `debug` table is stripped in `disable_non_determinism()` (`wasm/src/runtime.c`), and (2) the build has **no Asyncify**, so WASM can't pause mid-run and hand control back to JS. Both are ours to change.

**Chosen shape:** virtual source keyed by script **SHA** (works with inline template-literal scripts in tests); **full** debugger scope; **adapter-only** packaging (ship a DAP server, no published extension).

---

## Why `debug` is stripped today — and the gating boundary

`disable_non_determinism()` removes `debug` (alongside `io`, `os`, `package`, `require`, `dofile`, `loadfile`, `math.random`/`randomseed`) for two reasons, matching **real Redis** exactly:

1. **Sandbox escape.** `debug.getregistry`, `debug.setmetatable`, `debug.getupvalue`/`setupvalue`, `debug.sethook` reach VM internals — a script could mutate other functions' upvalues, swap metatables on protected types, or read the C registry to break out of the restricted environment.
2. **Determinism.** Redis requires scripts be deterministic (historically for replication/AOF parity: same script → same effects on a replica). `debug` exposes execution-order/internal state that isn't reproducible.

So stripping it is a **security + compatibility** decision, not arbitrary — real Redis strips the identical set.

**Gating boundary (non-negotiable):** the production WASM binary is **never** touched. `debug` is re-enabled, and Asyncify added, **only** in a separate `-DREDIS_LUA_DEBUG` build flavor that the mock loads exclusively when `REDIS_LUA_DEBUG=1`. The default binary stays stripped, deterministic, and sandbox-parity-correct. The debug flavor trades those guarantees on purpose (you're stepping your own script on your own machine) — opt-in and isolated.

---

## Architecture

```
VS Code  ──DAP──►  LuaDebugAdapter (@vscode/debugadapter, in mock process)
(breakpoints,                │
 step, vars)                 │ drives
                             ▼
                    LuaDebugController ◄──── host_debug_request (Asyncify import)
                             ▲                         │
        ioredis ─EVAL─► RedisLuaRuntime ─► debug WASM ─┘
                             │                  │ runs
                             └─ redis.call ◄────┤  user chunk  (chunkname = SHA)
                                (sync, live      │  + Lua "debug agent" (line hook + loop)
                                 keyspace)
```

**Pause mechanism (the key idea).** A small Lua **debug agent** chunk installs `debug.sethook(hook, "l")`. When the hook decides to stop (breakpoint hit / step landed), the agent enters a **command loop** that round-trips to JS via one Asyncify host import, `host_debug_request`. Because suspension always happens *inside the agent's host call* — never unwinding the user's frames — the user's Lua stack and locals stay **live**. That gives full-fidelity inspection: lazy table expansion and arbitrary `evaluate` in the paused frame both work, with no Worker/SharedArrayBuffer. The C surface stays tiny; the debugger "brain" lives in the Lua agent + the TypeScript DAP layer.

Per-line suspends are avoided: the agent keeps the breakpoint set + step mode **locally** and only crosses to JS when it actually stops. Each resume response carries the updated breakpoint set.

---

## Part A — `redis-lua-wasm` (debug build flavor)

All changes gated behind `-DREDIS_LUA_DEBUG` so the default binary is unchanged.

1. **New build flavor** — `wasm/build/build.sh` (or a sibling `build-debug.sh`): emit `redis_lua.debug.{mjs,wasm}` with existing flags **plus**:
   - `-sASYNCIFY=1 -sASYNCIFY_IMPORTS='["host_debug_request"]'`
   - `-DREDIS_LUA_DEBUG`
   - add `_eval_debug`, `_set_source_name` to `-sEXPORTED_FUNCTIONS`.

2. **`wasm/src/runtime.c`**
   - Under `REDIS_LUA_DEBUG`, **do not** strip `debug` in `disable_non_determinism()` (leave the table available to the agent).
   - Add host import decl: `extern PtrLen host_debug_request(uint32_t ptr, uint32_t len);` (the one Asyncify import).
   - Register a Lua C function `__redis_debug_request(payload:string) -> string` that forwards bytes to `host_debug_request` and returns the JS reply bytes back to Lua. The agent's only door to JS.
   - `_set_source_name(ptr,len)` stores a chunk name; `eval`/`eval_debug` pass it to `luaL_loadbuffer` instead of the hardcoded `"@user_script"`. The mock sets it to the script **SHA** so DAP source identity == SHA.
   - `_eval_debug(...)`: same as `eval_with_args`, but first `luaL_loadbuffer`+`lua_pcall` the **agent chunk** (installs the line hook), then run the user chunk. Keep the fuel hook (coexists via `LUA_MASKCOUNT|LUA_MASKLINE`).

3. **Lua debug agent** — `wasm/src/debug_agent.lua` embedded as a C string (or a `_load_debug_agent` export). Pure Lua, owns all introspection:
   - `debug.sethook(hook,"l")`; `hook(_,line)` checks local `breakpoints[line]` / step state.
   - On stop: ping-pong loop calling `__redis_debug_request(json)`; handles ops `stackTrace` (`debug.getinfo`), `scopes`/`variables` (`debug.getlocal`, `debug.getupvalue`, recursive table walk with `variablesReference` handles), `evaluate` (`load(expr)` + `pcall` in the frame). Loop exits on a `resume{mode, breakpoints}` reply, updating local state; `mode` ∈ continue/stepOver/stepIn/stepOut via `debug.getinfo` stack-depth tracking.
   - One `ready` handshake at entry (supports `stopOnEntry`).
   - ABI: reuse the existing length-prefixed buffer convention (`helpers.ts`/`codec.ts`); payloads are JSON (perf is a non-goal per CLAUDE.md).

4. **JS API** — `src/engine.ts` + `src/loader.ts` + `src/types.ts`:
   - Add `host_debug_request` to `hostImports` and a `debugRequest` entry to `MutableHandlers` (mirror the swappable-handler pattern in `engine.ts`).
   - `module.create(host)` accepts optional `onDebugRequest(bytes): Promise<bytes>` (async — allowed because it's in `ASYNCIFY_IMPORTS`).
   - Add `engine.evalDebug(script, keys, args, { sourceName })` returning `Promise<ReplyValue>` (Asyncify makes `_eval_debug` async). `evalWithArgs` stays sync.
   - `LuaWasmModule.defaultDebugWasmPath()` / `defaultDebugModulePath()`.
   - Bump version; publish so js-redis-server consumes from registry.

---

## Part B — `js-redis-server` runtime bridge

1. **`src/core/lua-runtime.ts`** — `RedisLuaRuntime` gains a debug mode:
   - Construct with the **debug** WASM (`load({ wasmPath, modulePath: debug paths })`) when debugging is enabled; otherwise unchanged.
   - Provide `onDebugRequest` that forwards to a `LuaDebugController` (Part C) and awaits its decision.
   - `eval(...)` returns `ReplyValue | Promise<ReplyValue>`; debug path calls `engine.evalDebug(script, keys, args, { sourceName: sha })`. `sha` already available.
   - `redis.call` is **unchanged and still synchronous** (`host_redis_call` is not an Asyncify import) → `executePlanSync` path untouched.
   - Record `sha → script text` into a `DebugSourceRegistry` on each debug eval so DAP `source` requests resolve.

2. **`src/commands/scripts.ts`** — `runLuaScript` already `await`s the runtime; no change beyond awaiting the possibly-async debug eval. EVAL holding its DB turn while paused is **correct** (single-threaded Redis: a paused script blocks the keyspace, which the debugger wants frozen). Comment it; no `park`/`turn.suspend` needed for v1.

3. **Enablement** — a server-construction option (`Resp2ServerOptions.luaDebug` or env `REDIS_LUA_DEBUG`) selects the debug runtime + starts the DAP server (Part D). Default off.

---

## Part C — TypeScript Debug Adapter (DAP)

New dir `src/debug/`:

- **`lua-debug-adapter.ts`** — extends `LoggingDebugSession` from `@vscode/debugadapter` (new dep). Implements: `initialize`, `setBreakpoints` (store by source SHA), `configurationDone`, `threads` (single synthetic thread), `stackTrace`, `scopes`, `variables`, `evaluate`, `continue`, `next`, `stepIn`, `stepOut`, `source` (return script text by SHA from `DebugSourceRegistry`), `disconnect`. `stopOnEntry` supported.
- **`lua-debug-controller.ts`** — bridge between `onDebugRequest` (Part B) and the adapter:
  - Agent `stopped` → emit DAP `StoppedEvent`.
  - VS Code inspect requests (stackTrace/scopes/variables/evaluate) pumped **one at a time** into the paused agent via the `host_debug_request` ping-pong, results returned to DAP.
  - `continue`/step → resolve the pending `host_debug_request` with `resume{mode, breakpoints}`.
  - Owns `variablesReference` handle table mirroring the agent's.
- **Protocol** (over `host_debug_request`): agent→host `stopped|result`; host→agent `inspect{op,args}|resume{mode,breakpoints}`. JSON.

---

## Part D — VS Code wiring (adapter-only)

- **`src/debug/server.ts`** — `createLuaDebugServer(opts)` starts the adapter in **DAP server mode** (`net` socket) bound to the runtime; logs `lua-debug adapter listening on <port>`. Exported from `src/index.ts`. A `bin` entry allows spawning standalone too.
- **Developer flow**: run the existing ioredis test/app with `REDIS_LUA_DEBUG=1` (or pass `luaDebug` to the server) → adapter server comes up. In VS Code, `.vscode/launch.json`:
  ```json
  { "type": "redis-lua", "request": "attach", "name": "Debug Lua (mock)", "debugServer": 4711 }
  ```
- **Caveat (the one rough edge of adapter-only):** VS Code only lets you pick a debug *type* some extension registered. To stay extension-free we ship a **minimal unpublished contribution stub** (`contributes.debuggers` with `type: "redis-lua"` + the adapter path) living in a tiny local folder — ~15 lines, no UI/logic, not on the Marketplace. Documented in README. Fallback if too fiddly: a thin published extension (out of scope for v1).
- **Setting breakpoints on inline-string scripts**: default `stopOnEntry: true` pauses at line 1 of the SHA virtual doc; VS Code opens it (served via `source`), dev sets breakpoints, hits continue. Breakpoints persist across re-runs by SHA.

---

## Verification

1. **WASM**: build the debug flavor; a node smoke test runs `engine.evalDebug("local x=1\nreturn x", ...)` with an `onDebugRequest` stub that (a) sets a breakpoint on line 2, (b) on `stopped` issues `variables`, asserts `x==1`, (c) `evaluate("x+41")`==42, (d) resumes → result 1. Proves agent + Asyncify suspend/resume + introspection.
2. **Unit** (`tests/`, node:test): `LuaDebugController` protocol translation — fake agent ping-pong ⇄ DAP requests; breakpoint set/clear; step modes; variablesReference expansion of a nested table.
3. **Integration** (`tests-integration/`): start a mock server with `luaDebug`, connect a scripted DAP client (`@vscode/debugadapter-testsupport`) to the server socket, `ioredis.eval(script,...)` on another connection; assert: stop on breakpoint, `redis.call` inside the script still mutates the live keyspace, stackTrace/scopes/variables payloads, evaluate, continue → correct EVAL reply. **Mock backend only** (no WASM on real-Redis backend); keep the rest of `test:all` green.
4. **Manual**: `launch.json` attach against a sample test; confirm gutter breakpoints, Variables panel (KEYS/ARGV/locals/nested table), watch, step in/over/out in the VS Code UI.

## Risks / notes

- **Asyncify reentrancy**: only `host_debug_request` may suspend; keep `host_redis_call/pcall` out of `ASYNCIFY_IMPORTS` so the sync `executePlanSync` bridge is unaffected. Verify Emscripten doesn't auto-instrument them.
- **State reuse**: `g_state` is a singleton reused across evals; re-install the agent's hook + globals per debug eval and tear down after, so a later non-debug eval isn't left with a line hook.
- **Single in-flight script**: `RedisLuaRuntime` is non-reentrant; the debugger inherits that — one paused script at a time, matching Redis semantics.
- **Performance**: per-line hook + JSON ping-pong is slow, but debug-only and explicitly acceptable (CLAUDE.md: perf not a priority).
- **Adapter-only type registration**: the documented caveat above is the main UX wrinkle; revisit a published extension only if users push back.
