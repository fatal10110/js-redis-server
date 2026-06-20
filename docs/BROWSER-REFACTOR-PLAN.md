# Refactor Plan: Run the Redis Mock in the Browser

## Goal

Run the in-memory Redis-compatible server entirely in a browser (tests, playgrounds,
demos, offline apps) with the full command pipeline, including `SCRIPT`, `EVAL`, and
`EVALSHA`. There is no TCP in the browser target. Browser consumers drive the mock via
the function-call client (`createInMemoryClient`) or by feeding RESP bytes through the
socketless `InMemoryConnectionTransport`.

## Why this is still tractable

The architecture is already well-positioned:

- Commands are pure `(args, ctx) -> RedisResult` and never touch the transport.
- `InMemoryConnectionTransport` and `InMemoryRedisClient` are already socketless.
- `createRedisMock({ transport: 'memory' })` already builds a no-listener pipeline whose
  `.client()` works without any network.
- `Logger` is a plain interface; `setTimeout` is browser-native; `.unref?.()` is already
  optional-chained; `AbortController`/`AbortSignal` are native.

The work is not a core rewrite. The real work is module-boundary cleanup, one sync SHA1
replacement, and a mandatory browser-safe `lua-redis-wasm` release.

## Blocker Inventory

| #   | Blocker                                                                                                     | Location                                                                                    | Severity                                                          |
| --- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| 1   | `Buffer` (currently 686 source tokens / 72 source files) plus transitive users (`respjs`, `lua-redis-wasm`) | pervasive                                                                                   | high, but polyfillable                                            |
| 2   | `import crypto from 'node:crypto'` for sync SHA1                                                            | `src/state/script-cache.ts`                                                                 | low, single call site                                             |
| 3   | TCP `net` imports                                                                                           | `src/core/transports/resp2/server.ts`, `src/core/transports/socket-connection-transport.ts` | low, already transport-isolated                                   |
| 4   | `src/mock.ts` imports TCP/cluster modules at top level                                                      | `src/mock.ts` -> `Resp2Server`, `createRedisCluster`                                        | medium, needs source split for browser entry                      |
| 5   | `src/cluster.ts` imports `Resp2Server` at top level                                                         | `src/cluster.ts`                                                                            | medium, exclude from browser entry or split topology-only helpers |
| 6   | `lua-redis-wasm` top-level Node imports and file-URL glue loading                                           | dependency `dist/index.mjs`                                                                 | high, must be fixed upstream before browser v1 ships              |
| 7   | `process.argv` / `process.exitCode`                                                                         | `src/cli.ts`                                                                                | none, CLI remains Node-only and excluded                          |

Important correction: `src/cluster.ts` has no direct Node built-in import, but it is not
browser-safe today because it imports `Resp2Server`, which imports `net`.

## Strategy: Browser Entry + Real Browser-Safe Lua

Do **not** rewrite the repo from `Buffer` to `Uint8Array`. Per repo policy ("performance
is not a priority... prefer correctness and clarity"), keep `Buffer` and ship a browser
bundle with the `buffer` package injected.

Browser v1 includes Lua. That means the `lua-redis-wasm` browser loader work is on the
critical path, not a later enhancement. The browser build should not rely on empty stubs
for `node:fs`, `node:path`, `node:url`, `node:crypto`, or `net` to hide reachable code.
If a stub is used for an intentionally unreachable Emscripten dead branch, make it a
throwing stub and prove the branch is unreachable with an `EVAL` smoke test in a real
browser-like runtime.

## Phase 0 - Decisions Locked Before Coding

1. **Lua/EVAL ships in browser v1.** This makes the upstream `lua-redis-wasm` browser
   loader release mandatory before the browser target can be called done.
2. **Buffer strategy:** polyfill with the `buffer` npm package. It provides the APIs this
   code and dependencies use (`allocUnsafe`, `readUInt32LE`, `Buffer.concat`, etc.).
3. **WASM delivery:** support caller-provided `wasmBytes` or `wasmUrl`, and default the
   browser bundle to a co-located asset URL. Avoid base64 inlining by default.
4. **Import path:** ship an explicit `js-redis-server/browser` subpath. Keep the package
   root as the Node build unless we intentionally add a browser condition under
   `exports["."]`.

## Phase 1 - Split the Browser-Safe Source Boundary

- Do not import `src/index.ts`, `src/internal.ts`, `src/mock.ts`, or `src/cluster.ts` from
  `src/browser.ts`; each currently pulls Node-only or broad internal modules into the
  static graph.
- Extract the standalone no-listener pipeline and memory mock helpers into a Node-neutral
  module, for example `src/mock-memory.ts`:
  - `createStandalonePipeline`
  - `createMemoryMock`
  - shared `RedisMock` / option types that do not mention `Resp2Server` values
- Keep `src/mock.ts` as the Node facade that wires TCP and cluster support on top of the
  shared memory helpers. This preserves existing Node consumers.
- Add `src/browser.ts` exporting only the browser-safe surface:
  - memory-only `createRedisMock`
  - `createInMemoryClient` / `InMemoryRedisClient`
  - `RedisServerState`
  - `createRedisCommandExecutor`
  - `InMemoryConnectionTransport`
  - RESP codec and client-visible error classes
- Browser `createRedisMock` rejects `transport: 'tcp'` and `cluster` with explicit errors.
  Runtime rejection is not enough by itself; the module graph must also avoid importing
  TCP/cluster modules.

Verification:

- Static bundle check for `src/browser.ts` with esbuild `platform: 'browser'`, no Node
  builtin aliases, and no unresolved `net`, `node:fs`, `node:path`, `node:url`,
  `node:crypto`, or `process` references.
- Existing Node tests stay green to prove the shared helper extraction did not change
  `src/index.ts` behavior.

## Phase 2 - Make Scripting Browser-Safe

### 2a. Replace Node SHA1

`RedisScriptCache.load()` must stay sync because `SCRIPT LOAD` and `EVAL` cache scripts
synchronously. Browser `crypto.subtle.digest()` is async, so do not use it here.

- Add a small dependency-free sync SHA1 helper, for example `src/state/sha1.ts`.
- Change `src/state/script-cache.ts` to import the helper instead of `node:crypto`.
- Keep Node behavior identical: same Redis SHA1 digest strings and same cache semantics.

Verification:

- Unit tests for SHA1 vectors, including empty script and a known Lua script.
- `SCRIPT LOAD`, `SCRIPT EXISTS`, `EVAL`, and `EVALSHA` tests continue to pass.

### 2b. Ship a Browser-Safe `lua-redis-wasm`

Required upstream change in the separate `lua-redis-wasm` repo:

- Add a browser export condition whose top-level graph has no Node built-in imports.
- Allow browser glue loading without `pathToFileURL()` and `file://` dynamic import.
  Accept one or more of:
  - a pre-imported `moduleFactory`
  - an already loaded glue module
  - an HTTP(S) `moduleUrl`
- Continue accepting `wasmBytes` so callers can bypass filesystem reads.
- Publish a point release and update `js-redis-server` to that version.

Current blocker in the dependency:

```js
const moduleUrl = pathToFileURL(modulePath).href
const imported = await import(moduleUrl)
```

That path is Node-only. Browser v1 cannot ship until this is fixed upstream.

### 2c. Thread Lua Loader Options Through This Repo

- Add `RedisLuaRuntimeOptions` accepted by `createRedisLuaRuntime(options)`.
- Add either `luaRuntimeOptions` or `luaRuntimeFactory` to `RedisServerStateOptions`.
- `RedisServerState.getLuaRuntime()` keeps the current lazy per-server memoization, but
  uses the configured options/factory.
- Node default remains current behavior: load the bundled wasm using the dependency's
  Node path.
- Browser entry provides a browser loader: fetch `wasmUrl` or consume `wasmBytes`, then
  call the upstream browser-safe loader/factory.

Verification:

- Browser smoke test executes `EVAL "return 1+1" 0` and receives `2`.
- Browser smoke test runs `SCRIPT LOAD`, then `EVALSHA` with the returned digest.
- Existing Lua isolation and scripting tests remain green in Node.

## Phase 3 - Browser Build Target

- Add a third `tsup` config block:
  - `entry: { browser: 'src/browser.ts' }`
  - `format: ['esm']`
  - `platform: 'browser'`
  - `dts: true`
  - `splitting: false` unless the existing circular dependency concern has been removed
- Add `buffer` as a runtime dependency and inject it for the browser build.
- Emit or copy the Lua wasm asset into `dist/` unless the browser build is configured to
  require caller-provided bytes.
- Add `package.json` `exports["./browser"]` with JS and type entries, for example:

```json
"./browser": {
  "import": {
    "types": "./dist/browser.d.mts",
    "default": "./dist/browser.mjs"
  }
}
```

- Do not rely on a top-level `"browser"` field while an explicit `exports` map exists. If
  root-import browser auto-selection is desired, add a real `"browser"` condition under
  `exports["."]`; otherwise document the explicit subpath.
- Keep `main`, `module`, and the default `exports["."]` target pointing at the Node build.

Verification:

- `npm run build` produces `dist/browser.mjs` and browser declarations.
- `tests-package` covers `import 'js-redis-server/browser'`.
- A tiny bundled app imports `js-redis-server/browser` and contains no unresolved Node
  builtins.

## Phase 4 - Browser Tests & Docs

- Browser smoke suite in a real browser-like runtime (Playwright preferred over `jsdom`
  for WASM/fetch coverage):
  - `createRedisMock({ transport: 'memory' })`
  - `createInMemoryClient`
  - `SET` / `GET` / `HSET` / `EXPIRE`
  - `MULTI` / `EXEC`
  - `SCRIPT LOAD` / `SCRIPT EXISTS`
  - `EVAL`
  - `EVALSHA`
- Negative browser tests:
  - `createRedisMock({ transport: 'tcp' })` rejects clearly
  - `createRedisMock({ cluster: ... })` rejects clearly
  - accessing TCP endpoint helpers on a memory mock still throws the existing no-endpoint
    error
- Keep all existing Node tests green.
- Update `docs/ARCHITECTURE.md` with a "Browser target" note.
- Update `README.md` with:

```ts
import { createRedisMock } from 'js-redis-server/browser'
```

and document WASM delivery options (`wasmUrl` / `wasmBytes`) if exposed publicly.

## Effort & Sequencing

| Phase                               | Effort  | Blocking? |
| ----------------------------------- | ------- | --------- |
| 0 Locked decisions                  | trivial | yes       |
| 1 Browser-safe source boundary      | S       | yes       |
| 2a SHA1 swap                        | S       | yes       |
| 2b `lua-redis-wasm` browser release | M       | yes       |
| 2c Lua loader wiring                | S/M     | yes       |
| 3 Browser build target              | S       | yes       |
| 4 Tests/docs                        | S/M     | yes       |

Critical path: Phase 0 -> Phase 1 -> Phase 2a -> Phase 2b -> Phase 2c -> Phase 3 ->
Phase 4. Lua is mandatory, so there is no supported "browser without Lua" release path.

## Risks / Watch Items

- **Upstream release timing:** `lua-redis-wasm` must publish a browser-safe loader before
  this package can ship a complete browser target.
- **Static graph leaks:** `src/browser.ts` must not import Node facade modules that pull
  in `net`, CLI, or TCP cluster code.
- **Stub masking:** do not let empty aliases make tests pass while reachable Node paths
  remain in the browser bundle.
- **Buffer polyfill correctness:** run RESP encode/decode and command tests against the
  browser bundle, not just against TypeScript source in Node.
- **WASM delivery:** prefer external/co-located wasm assets or caller-provided bytes over
  base64 inlining to keep bundle size controlled.
