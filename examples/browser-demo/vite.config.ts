import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import { nodePolyfills } from 'vite-plugin-node-polyfills'

// Demo-only config. Polyfills the Node builtins the js-redis-server source graph
// pulls in (buffer; crypto for sync SHA1; net as a never-invoked stub). None of
// this touches the published package — it lives entirely in examples/.
//
// The demo imports `../../src/in-memory-client`, OUTSIDE this demo's package
// root. node-polyfills injects `import ... from
// 'vite-plugin-node-polyfills/shims/<x>'` into those source files, and Rollup
// resolves that bare specifier from the source file's location (repo root),
// where the shim isn't installed. Alias the three shims to absolute paths in the
// demo's own node_modules so they resolve regardless of importer location.
const abs = (rel: string) => fileURLToPath(new URL(rel, import.meta.url))

const shim = (name: string) =>
  abs(`./node_modules/vite-plugin-node-polyfills/shims/${name}/dist/index.js`)

// node-polyfills/node-stdlib-browser can't resolve the `fs/promises` subpath
// (it appends `/promises` to the fs mock file). lua-redis-wasm's Node branch
// dynamic-imports it, so the specifier must resolve for the bundler to graph it
// — but the browser never runs that branch (isNode === false), so the empty mock
// is never invoked. The EVAL smoke test, which uses fetch (not fs), proves it.
const emptyMock = abs('./node_modules/node-stdlib-browser/esm/mock/empty.js')

export default defineConfig({
  base: '/js-redis-server/',
  plugins: [nodePolyfills()],
  resolve: {
    alias: {
      'vite-plugin-node-polyfills/shims/buffer': shim('buffer'),
      'vite-plugin-node-polyfills/shims/global': shim('global'),
      'vite-plugin-node-polyfills/shims/process': shim('process'),
      // Use the browser-loadable lua-redis-wasm build vendored into this demo
      // (its loader self-resolves redis_lua.mjs/.wasm via import.meta.url, so
      // Vite emits them as assets). Vendored so GitHub Pages CI — which only
      // checks out js-redis-server — can build without the sibling repo or npm.
      'lua-redis-wasm': abs('./vendor/lua-redis-wasm/index.mjs'),
      'node:fs/promises': emptyMock,
      'fs/promises': emptyMock,
    },
  },
})
