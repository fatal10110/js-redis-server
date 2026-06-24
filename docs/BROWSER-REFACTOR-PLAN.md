# Refactor Plan: Run the Redis Mock in the Browser

## Goal

**Scope: one local demo, not a published `npm` browser target.** Get the in-memory
Redis-compatible server running in a browser well enough to back one demo page, full
command pipeline including `SCRIPT`/`EVAL`/`EVALSHA`. No TCP in the browser path. The
demo drives the mock via `createInMemoryClient` or by feeding RESP bytes through the
socketless `InMemoryConnectionTransport`.

This is explicitly **not** a commitment to ship `js-redis-server/browser` as a supported
public export, publish a browser-safe `lua-redis-wasm` release, or guarantee
Node-consumer isolation beyond "don't break the existing Node build." Anything below
phrased as a publish/package-consumer concern is dropped or downgraded to "whatever the
demo's own bundler config needs."

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

## Strategy: Demo Vite Config Absorbs Node Blockers, Package Untouched

**Re-reviewed against the locked scope (demo-only, don't touch the package, polyfill in
the demo build).** The original plan below was written for a *published* browser target,
which forced source-level changes (a curated `src/browser.ts`, an in-source SHA1 swap,
threaded loader options) so that arbitrary npm consumers wouldn't have to configure a
bundler. A single owned GitHub-Pages demo has no such consumers — its own Vite config
absorbs every Node-builtin blocker, and `js-redis-server`'s `src/` stays byte-for-byte
unchanged.

Import-graph facts that drive this (traced from `src/in-memory-client.ts`, the demo's
entry):

- The only **invoked** Node-builtin blocker is `node:crypto` (sync SHA1 in
  `state/script-cache.ts`, hit on `SCRIPT LOAD`/`EVAL`).
- `net` is reachable only as `import`-but-never-called: the edge is
  `in-memory-client → seed → cluster → resp2/server`, and the demo never calls
  `.listen()`. A throwing/empty `net` stub is honest here — nothing in the demo path
  reaches `net.createServer()`.
- `lua-redis-wasm@1.3.0` is the one true blocker: `loadModule()` always does
  `pathToFileURL(modulePath)` + `import(file://)` for the Emscripten glue, with no
  factory/url escape hatch. `wasmBytes` skips the `.wasm` read but not the glue import.
  This must be fixed in the **owned** `lua-redis-wasm` repo (separate package, not
  `js-redis-server`), exactly as `texture2ddecoder-wasm` was.

So: keep `Buffer` (repo policy: clarity over perf), polyfill `buffer`/`crypto`/`net` via
the demo's Vite config only, and push all browser-loading smarts into the owned
`lua-redis-wasm` package so `js-redis-server`'s bare `load()` call keeps working
unchanged. Do not let `net` stubbing mask anything: the demo's smoke test runs a real
`EVAL`/`EVALSHA`, which would fail loudly if a genuinely-reachable Node path were stubbed
away.

## Phase 0 - Decisions Locked Before Coding

1. **Lua/EVAL is in the demo.** Confirmed by the user. The owned `lua-redis-wasm` browser
   fix is therefore on the critical path.
2. **No `js-redis-server` source changes.** The demo imports `createInMemoryClient`
   straight from `src/` and makes the graph browser-safe purely through its own Vite
   config. `src/` stays untouched; the package's published API/behavior/deps don't move.
3. **Polyfills live in the demo only.** Use one plugin — `vite-plugin-node-polyfills` —
   to cover `buffer`, `crypto` (sync `createHash('sha1')` via `crypto-browserify`, byte-
   identical digest to `node:crypto`), and `net` (unreachable stub) in one shot. These are
   `examples/browser-demo` devDependencies, never in the root `package.json`. (Confirmed:
   root `tsup.config.ts` has explicit entries `index`/`internal`/`cli` only, and `files`
   publishes `dist/` only — `examples/` is never built or published with the package.)
4. **WASM/glue delivery:** fix the owned `lua-redis-wasm` so its browser path self-resolves
   glue + `.wasm` via `new URL('./redis_lua.*', import.meta.url)` (bundler-inlined by
   Vite), making browser `load()` work with **zero args** — so `js-redis-server`'s existing
   `createRedisLuaRuntime()` → `load()` call needs no change.
5. **Import path:** demo imports `../../src/in-memory-client` (relative). No
   `package.json` exports map, no published subpath, no curated `src/browser.ts`. Revisit
   only if a real public browser release is ever wanted.

## Phase 1 - (dropped) No Browser-Safe Source Split

**Cut by the re-review.** A curated `src/browser.ts` + `mock-memory.ts` extraction only
earns its keep for a *published* subpath with external consumers. The demo imports
`createInMemoryClient` from `src/in-memory-client.ts` directly; Vite tree-shakes the graph
and polyfills the Node builtins (Phase 3). `src/` is not touched.

If a real public `js-redis-server/browser` export is ever wanted, resurrect this phase
from git history — but it is explicitly out of scope for the demo.

## Phase 2 - Make Scripting Browser-Safe

### 2a. (dropped) No In-Source SHA1 Swap

**Cut by the re-review.** `state/script-cache.ts` keeps `node:crypto`. The demo's Vite
`node-polyfills` plugin aliases `crypto` to `crypto-browserify`, whose
`createHash('sha1')` is synchronous (so `RedisScriptCache.load()` stays sync) and produces
a byte-identical digest to `node:crypto` — `SCRIPT LOAD`/`EVALSHA` digests still match
real Redis. Package source unchanged; verification happens in the Phase 4 smoke test.

### 2b. Make the Owned `lua-redis-wasm` Browser-Loadable (the only real code work here)

The one true blocker, in the **separate, owned** `lua-redis-wasm` repo — not
`js-redis-server`. Reference: [`texture2ddecoder-wasm`](https://github.com/fatal10110/texture2ddecoder-wasm)'s
`src/index.ts` (single isomorphic loader, runtime env detection, no build-time export
condition).

Current blocker in `loadModule()` (`dist/index.mjs`, confirmed in 1.3.0):

```js
const modulePath = options.modulePath ?? defaultModulePath()
const moduleUrl = pathToFileURL(modulePath).href   // Node-only
const imported = await import(moduleUrl)            // file:// import, Node-only
// wasmBytes already lets callers skip the .wasm fs.readFile, but NOT this glue import
```

Required change:

- Branch on environment (`isNode` via `process.versions.node`, else browser).
- Node path: unchanged (`pathToFileURL` + `fs.readFile`).
- Browser path: load the Emscripten glue and `.wasm` via
  `new URL('./redis_lua.mjs', import.meta.url)` / `new URL('./redis_lua.wasm', import.meta.url)`
  so Vite inlines/serves them — **no `pathToFileURL`, no `file://`, and zero required
  caller args**. (Zero-arg is the key constraint: it keeps `js-redis-server`'s existing
  `createRedisLuaRuntime()` → `load()` call working untouched, so Phase 2c is unnecessary.)
- Keep `wasmBytes` as an optional override.
- No npm publish required — the demo consumes the built `dist/` (vendored into
  `examples/browser-demo/vendor/lua-redis-wasm/`). Publish later if desired.

**Implementation outcome (done).** `src/loader.ts` rewritten: no top-level `node:*`
imports, `isNode`-branched, browser path uses literal `import('./redis_lua.mjs')` +
`fetch(new URL('./redis_lua.wasm', import.meta.url))`; Node path dynamic-imports the
builtins. All 138 Node tests stay green. Built TS-only (no Docker) since the wasm
artifacts already existed.

**Proper fix shipped — the glue is now built `-sENVIRONMENT=web,worker,node`.** The
original vendored glue was `-sENVIRONMENT=node` (hardcoded `ENVIRONMENT_IS_NODE=true`,
unconditional `require()` at init), which the demo briefly worked around with a
browser require-shim. That is now resolved at the source: `wasm/build/build.sh` compiles
with `web,worker,node`, and `wasm/build/docker-build.sh` was bumped to the arm64-native
`emscripten/emsdk:6.0.1` image (the old `3.1.56` forced `linux/amd64`, whose qemu-emulated
compiler segfaulted on Apple Silicon). The emsdk-6 glue does runtime env detection, uses
Web Crypto directly, and drops the `createRequire('module')` preamble entirely — so the
demo needs **no** require-shim and **no** `module` override. All 138 `lua-redis-wasm`
node tests pass across the emsdk 3.1→6 jump (ABI unchanged). Browser EVAL/EVALSHA verified
green with the shim removed.

**CDN delivery (jsdelivr).** `loadModule`'s browser branch also honors `modulePath` /
`wasmPath` as URLs, so a consumer can fetch the glue + `.wasm` from a CDN
(`https://cdn.jsdelivr.net/npm/lua-redis-wasm@VERSION/dist/...`) instead of bundling them.
This requires publishing the fixed `lua-redis-wasm` to npm; the bundled/vendored path
remains the zero-publish default.

### 2c. (dropped) No Loader-Option Threading

**Cut by the re-review.** Because the fixed `lua-redis-wasm` browser `load()` is zero-arg
self-resolving (2b), `js-redis-server`'s `createRedisLuaRuntime()` / `getLuaRuntime()`
need no `RedisLuaRuntimeOptions`, no `RedisServerStateOptions` changes. Package source
unchanged.

## Phase 3 - Demo Build

No `tsup` config, no `package.json` exports, nothing published to npm. Demo app lives at
`examples/browser-demo` (Vite) and imports `createInMemoryClient` straight from
`../../src/in-memory-client` via a relative path.

- `examples/browser-demo` gets its OWN `package.json` (own `node_modules`, own
  `devDependencies`: `vite`, `vite-plugin-node-polyfills`) — root `package.json` is never
  touched.
- Demo's `vite.config.ts`:
  - `plugins: [nodePolyfills({ include: ['buffer', 'crypto'] })]` (and `net` — the plugin
    stubs it; nothing in the demo path calls `net.createServer().listen()`). This one
    plugin covers `buffer`, sync-`crypto` SHA1, and the `net` stub at once.
  - `base: '/js-redis-server/'` so built asset URLs resolve under the GH Pages project-site
    path (`username.github.io/js-redis-server/`).
- Demo depends on the local browser-fixed `lua-redis-wasm` build (2b) via a
  `file:`/workspace dependency — no npm publish needed, just something importable. With
  2b's zero-arg self-resolving browser `load()`, the demo needs no special loader wiring.
- Existing Node tests/build untouched (nothing in `src/` changed, so this is automatic).

Verification: `vite build` on the demo resolves with no unresolved Node builtins, and
produces `examples/browser-demo/dist/`.

## Phase 4 - Demo Smoke Check + GH Pages Deploy

One runnable check, not a suite — this is a demo, not a release gate:

- Open the demo in a browser, run through its actual UI flow: a few basic commands
  (`SET`/`GET`) plus `EVAL`/`EVALSHA` since the demo needs Lua.
- If the demo has no UI yet, a one-off `assert`-based script run through the demo's own
  bundler is enough: `createRedisMock({ transport: 'memory' })` → `SET`/`GET` →
  `SCRIPT LOAD` → `EVALSHA` → assert expected replies.
- Keep existing Node tests green — that's the only regression gate that matters here.

Deploy (decided: GH Actions, not manual branch push):

- Add `.github/workflows/deploy-demo.yml`: on push to `main` (path-filtered to
  `examples/browser-demo/**` and `src/**` so unrelated commits don't redeploy), build the
  demo with `vite build`, upload `examples/browser-demo/dist` via
  `actions/upload-pages-artifact`, deploy via `actions/deploy-pages`.
- One-time manual step (repo owner, not automatable here): Settings → Pages → Source →
  "GitHub Actions".

## Effort & Sequencing

| Phase                                  | Effort  | Status              |
| --------------------------------------- | ------- | ------------------- |
| 0 Locked decisions                     | trivial | done                |
| 1 Browser-safe source split            | —       | dropped (re-review) |
| 2a In-source SHA1 swap                 | —       | dropped (re-review) |
| 2b `lua-redis-wasm` browser fix        | S/M     | done (loader.ts; branch feat/browser-loader) |
| 2c Lua loader wiring                   | —       | dropped (re-review) |
| 3 Demo app + Vite config               | S       | done (examples/browser-demo) |
| 4 Demo smoke + GH Pages deploy         | S       | done (browser EVAL verified; workflow added) |

`js-redis-server` `src/` was not modified at all — the entire browser target lives in
`examples/browser-demo/` (Vite app + vendored wasm + node-polyfills config) plus the
loader fix in the separate `lua-redis-wasm` repo. Browser smoke (chromium via Playwright):
SET/GET/HSET/HGETALL/RPUSH/LRANGE + EVAL + SCRIPT LOAD + EVALSHA all correct, zero console
errors.

**Open items / not done:**
- Neither repo is committed (no PR yet). `lua-redis-wasm` change is on branch
  `feat/browser-loader`; `js-redis-server` demo is untracked on `main`.
- The proper `lua-redis-wasm` wasm rebuild (`-sENVIRONMENT=web,worker,node`) is deferred;
  the demo uses the require-shim workaround instead.
- One-time manual step before the first deploy: repo Settings → Pages → Source →
  "GitHub Actions".

## Risks / Watch Items

- **`net` stub honesty:** the demo entry is `createInMemoryClient` only. If the demo ever
  imports `createRedisMock`/`createRedisServer`/`cluster`, the `net` stub stops being a
  dead branch and the build silently ships a broken TCP path. Keep the demo on the
  socketless client; the Phase 4 `EVAL` smoke test is the tripwire.
- **Polyfill correctness:** exercise RESP encode/decode + a real `SCRIPT LOAD`/`EVALSHA`
  through the actual demo bundle, not just TS source in Node — confirm `crypto-browserify`
  SHA1 digests match Redis exactly (they should; it's a faithful impl).
- **`lua-redis-wasm` browser fix scope:** make browser `load()` zero-arg self-resolving
  (`import.meta.url`) so `js-redis-server` stays untouched. Don't over-build a generic
  public loader API unless separately wanted.
- **Demo import reaching into `src/`:** relative `../../src/in-memory-client` import means
  the demo tracks source, not the built `dist/`. Fine for a demo; just don't let it imply
  a supported public entry.
