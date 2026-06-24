// Browser bridge for the Node-built Emscripten glue (redis_lua.mjs).
//
// The vendored glue was compiled with `-sENVIRONMENT=node`, so it has
// `ENVIRONMENT_IS_NODE = true` hardcoded and unconditionally does
// `require("fs"|"path"|"crypto"|"url")` at init. It reaches that require via
// `import { createRequire } from 'module'`. node-polyfills' default `module`
// mock has no createRequire, so this shim provides one — and, crucially, returns
// browser-backed implementations so the Node-built glue runs unmodified in the
// browser.
//
// Correctness rests on two facts:
//   1. We hand the loader the wasm bytes (wasmBinary) plus a custom
//      instantiateWasm, so the glue's fs/path/url file-loading paths are never
//      exercised — those requires return inert stubs.
//   2. The only functional Node dependency is crypto entropy, backed here by the
//      Web Crypto API. The EVAL/EVALSHA smoke test is the end-to-end proof.

function browserCrypto() {
  return {
    // Emscripten uses randomFillSync for getentropy(). getRandomValues caps at
    // 65536 bytes per call, so fill in chunks.
    randomFillSync(buf) {
      const view =
        buf instanceof Uint8Array
          ? buf
          : new Uint8Array(buf.buffer ?? buf, buf.byteOffset ?? 0, buf.byteLength ?? buf.length)
      for (let i = 0; i < view.length; i += 65536) {
        crypto.getRandomValues(view.subarray(i, Math.min(i + 65536, view.length)))
      }
      return buf
    },
    randomBytes(n) {
      const b = new Uint8Array(n)
      crypto.getRandomValues(b)
      return b
    },
  }
}

// Inert: the glue only needs these for locating/reading the wasm file, which we
// bypass by providing wasmBinary + a custom instantiateWasm.
function browserPath() {
  return {
    dirname: (p) => String(p).replace(/\/[^/]*$/, '') || '/',
    join: (...parts) => parts.join('/').replace(/\/+/g, '/'),
    normalize: (p) => p,
    resolve: (...parts) => parts.join('/').replace(/\/+/g, '/'),
    sep: '/',
  }
}

function browserUrl() {
  return { fileURLToPath: (u) => String(u).replace(/^file:\/\//, '') }
}

export function createRequire() {
  return function require(id) {
    const name = String(id).replace(/^node:/, '')
    if (name === 'crypto') return browserCrypto()
    if (name === 'path') return browserPath()
    if (name === 'url') return browserUrl()
    if (name === 'fs') return {} // unused — wasm supplied via wasmBinary
    throw new Error(`require('${id}') is not available in the browser build`)
  }
}

export default { createRequire }
