import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import { nodePolyfills } from 'vite-plugin-node-polyfills'

// Demo-only config. Polyfills the one Node builtin the js-redis-server source
// graph still needs in the browser (buffer). `lua-redis-wasm` ≥1.4 ships a
// browser build with no `node:*` imports (resolved via its `browser` export
// condition), and main.ts points its WASM + glue at jsDelivr, so there's no fs
// alias and no vendored copy. The demo imports the net-free `src/cluster`
// (node-assembly only); the socket-backed `src/cluster-server` is never imported,
// so no `net` shim either. None of this touches the published package.
//
// The demo imports `../../src/in-memory-client`, OUTSIDE this demo's package
// root. node-polyfills injects `import ... from
// 'vite-plugin-node-polyfills/shims/<x>'` into those source files, and Rollup
// resolves that bare specifier from the source file's location (repo root),
// where the shim isn't installed. Alias the three shims to absolute paths in the
// demo's own node_modules so they resolve regardless of importer location.
const abs = (rel: string) => fileURLToPath(new URL(rel, import.meta.url))

// Pin the CDN-loaded WASM + glue to the SAME version we bundle the JS loader
// from, so a root-level `lua-redis-wasm` bump can't leave the loader and the
// jsDelivr assets on mismatched (ABI-incompatible) versions.
const luaWasmVersion = JSON.parse(
  readFileSync(abs('./node_modules/lua-redis-wasm/package.json'), 'utf8'),
).version as string

const shim = (name: string) =>
  abs(`./node_modules/vite-plugin-node-polyfills/shims/${name}/dist/index.js`)

// lua-redis-wasm's browser loader keeps default co-located references to its
// `redis_lua.mjs` glue + `redis_lua.wasm` for the no-CDN case. We load both from
// jsDelivr (main.ts), so those references are dead code — but Vite resolves the
// `new URL(...)` / dynamic `import(...)` at build time regardless of the runtime
// branch and would emit the real ~260 kB of assets. Strip those two expressions
// from the dep's source (a `pre` transform, before Vite's asset handling sees
// them). Both sit behind the `options.wasmPath`/`options.modulePath` we set, so
// the replacements are never evaluated.
const stripBundledLuaAssets = {
  name: 'strip-bundled-lua-assets',
  enforce: 'pre' as const,
  transform(code: string, id: string) {
    if (!id.includes('lua-redis-wasm') || !/redis_lua\.(mjs|wasm)/.test(code)) {
      return null
    }
    return code
      .replace(
        /new URL\(\s*["']\.\/redis_lua\.(?:mjs|wasm)["']\s*,\s*import\.meta\.url\s*\)/g,
        'new URL("about:blank")',
      )
      .replace(
        /import\(\s*["']\.\/redis_lua\.mjs["']\s*\)/g,
        'Promise.reject(new Error("bundled glue disabled; using jsDelivr"))',
      )
  },
}

export default defineConfig({
  base: '/js-redis-server/',
  define: { __LUA_WASM_VERSION__: JSON.stringify(luaWasmVersion) },
  plugins: [stripBundledLuaAssets, nodePolyfills()],
  resolve: {
    alias: {
      'vite-plugin-node-polyfills/shims/buffer': shim('buffer'),
      'vite-plugin-node-polyfills/shims/global': shim('global'),
      'vite-plugin-node-polyfills/shims/process': shim('process'),
    },
  },
})
