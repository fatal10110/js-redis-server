// The `process` global the demo bundle sees: vite-plugin-node-polyfills' own
// shim (the `process` npm package's browser build), plus `process.hrtime`,
// which that shim lacks. vite.config.ts aliases the plugin's injected
// `vite-plugin-node-polyfills/shims/process` import to this file, so every
// module that touches `process` — including ../../src — gets this object.
//
// src/core/clock.ts calls `process.hrtime.bigint()` at module load (MONITOR's
// microsecond timestamps, #388). Without this, that throws before the demo's
// first line of UI code runs and the page renders an empty terminal.

// Imported by path rather than by package name: the bare specifier is aliased
// to this very file.
// @ts-expect-error TS7016 — the plugin's shim ships no type declarations.
import shim from './node_modules/vite-plugin-node-polyfills/shims/process/dist/index.js'

const process = shim as NodeJS.Process

if (typeof process.hrtime !== 'function') {
  // Node's semantics on top of performance.now(): a monotonic [s, ns] tuple
  // (optionally relative to `prev`), and `.bigint()` in nanoseconds. Browsers
  // coarsen performance.now() to microseconds or worse, which clock.ts
  // tolerates — it only needs a monotonic source to offset a Date.now() anchor.
  const nowNanos = () => BigInt(Math.round(performance.now() * 1e6))
  const hrtime = (prev?: [number, number]): [number, number] => {
    let ns = nowNanos()
    if (prev) {
      ns -= BigInt(prev[0]) * 1_000_000_000n + BigInt(prev[1])
    }
    return [Number(ns / 1_000_000_000n), Number(ns % 1_000_000_000n)]
  }
  hrtime.bigint = nowNanos
  process.hrtime = hrtime
}

export { process }
export default process
