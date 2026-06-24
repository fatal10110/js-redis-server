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
