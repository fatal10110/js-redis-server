import typescript from '@rollup/plugin-typescript'
import resolve from '@rollup/plugin-node-resolve'
import commonjs from '@rollup/plugin-commonjs'
import dts from 'rollup-plugin-dts'

const external = [
  'net',
  'node:net',
  'node:fs',
  'node:path',
  'node:url',
  'node:crypto',
  'node:events',
  'async-mutex',
  'cluster-key-slot',
  'ioredis-mock',
  'lua-redis-wasm',
  'respjs',
]

export default [
  // Main library build (ESM + CJS)
  {
    input: './src/index.ts',
    output: [
      {
        file: 'dist/index.mjs',
        format: 'esm',
        sourcemap: true,
      },
      {
        file: 'dist/index.cjs',
        format: 'cjs',
        sourcemap: true,
      },
    ],
    external,
    plugins: [
      resolve({
        preferBuiltins: true,
      }),
      commonjs(),
      typescript({
        tsconfig: './tsconfig.json',
        declaration: true,
        declarationDir: './dist',
        sourceMap: true,
      }),
    ],
  },
  // CLI build (ESM only, since it's a binary)
  {
    input: './src/cli.ts',
    output: {
      file: 'dist/cli.mjs',
      format: 'esm',
      sourcemap: true,
      banner: '#!/usr/bin/env node',
    },
    external,
    plugins: [
      resolve({
        preferBuiltins: true,
      }),
      commonjs(),
      typescript({
        tsconfig: './tsconfig.json',
        sourceMap: true,
      }),
    ],
  },
  // Bundled declaration file
  {
    input: './dist/index.d.ts',
    output: {
      file: 'dist/index.d.ts',
      format: 'es',
    },
    external,
    plugins: [dts()],
  },
]
