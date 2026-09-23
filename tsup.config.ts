import { defineConfig } from 'tsup'

// Dual ESM + CJS build. The library entry (`index`) ships both formats plus
// type declarations so ESM/vitest consumers can `import` and CJS consumers can
// `require`. The CLI is CJS-only — it relies on `require.main === module` to
// detect direct execution — and gets a shebang banner so the published bin is
// runnable.
export default defineConfig([
  {
    entry: { index: 'src/index.ts', core: 'src/internal.ts' },
    format: ['esm', 'cjs'],
    dts: true,
    sourcemap: true,
    clean: true,
    // Bundle each entry standalone — no shared chunks between `index` and
    // `core`. Keeps the published output a couple of self-contained files
    // rather than a web of cross-referenced chunks.
    splitting: false,
    // The node-redis facade resolves its optional `redis` peer lazily and
    // synchronously with `createRequire(__filename)` (destroy() is synchronous,
    // so it cannot await an import). `__filename` is native in the CJS build;
    // this injects the ESM equivalent (`fileURLToPath(import.meta.url)`) into
    // the ESM build, only where it is referenced.
    shims: true,
    outDir: 'dist',
  },
  {
    entry: { cli: 'src/cli.ts' },
    format: ['cjs'],
    sourcemap: true,
    outDir: 'dist',
    banner: { js: '#!/usr/bin/env node' },
  },
])
