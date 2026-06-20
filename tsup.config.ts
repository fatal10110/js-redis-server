import { defineConfig } from 'tsup'

// Dual ESM + CJS build. The library entry (`index`) ships both formats plus
// type declarations so ESM/vitest consumers can `import` and CJS consumers can
// `require`. The CLI is CJS-only — it relies on `require.main === module` to
// detect direct execution — and gets a shebang banner so the published bin is
// runnable.
export default defineConfig([
  {
    entry: { index: 'src/index.ts' },
    format: ['esm', 'cjs'],
    dts: true,
    sourcemap: true,
    clean: true,
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
