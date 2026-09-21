// Unit-tests `readExportSurface` against synthetic declaration files.
//
// Two jobs.
//
// First, the invariants. `readExportSurface` throws when a program only
// half-resolves, because a degraded read returns a plausible-looking surface
// with members and type-only kinds quietly missing, and `skipLibCheck` — which
// all-declaration inputs need — suppresses the TS2307 that would reveal it.
// Those throws had no test: deleting both of them left the suite at 27/27,
// which is the same vacuity argument the sibling diff test exists to answer,
// reopened one layer up. A healthy `dist/` never reaches either branch, so only
// a deliberately broken input can pin them.
//
// Second, the extraction rules. Member and variant shapes that do not occur in
// this package today — a negative-number union, a bigint union, a class with
// colliding static and instance names — are exactly the ones that would degrade
// silently when someone writes them for the first time. Pinning them here costs
// nothing and needs no build.
//
// Every fixture is written to a scratch directory at run time; nothing here
// reads the built declarations. The scratch lives under `dist/` (gitignored)
// rather than the OS temp dir so that module resolution still walks up to the
// repo's `node_modules` — a fixture in `/tmp` cannot resolve `@types/node`, and
// would make the builtin case below fail for the wrong reason.

import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
import { readExportSurface, type ExportSurface } from './export-surface.js'

let scratch: string

before(() => {
  const distDir = fileURLToPath(new URL('../dist/', import.meta.url))
  mkdirSync(distDir, { recursive: true })
  scratch = mkdtempSync(join(distDir, 'export-surface-fixtures-'))
})

after(() => {
  rmSync(scratch, { recursive: true, force: true })
})

/** Write a `.d.ts` fixture and extract its surface. */
function surfaceOf(name: string, source: string): ExportSurface {
  const file = join(scratch, `${name}.d.ts`)
  writeFileSync(file, source, 'utf8')
  return readExportSurface(file)
}

describe('readExportSurface invariants', () => {
  test('throws when the entry resolves to zero exports', () => {
    const file = join(scratch, 'empty.d.ts')
    writeFileSync(file, 'declare const unused: number;\nexport {};\n', 'utf8')

    assert.throws(
      () => readExportSurface(file),
      /resolved to zero exports — the build is broken/,
    )
  })

  test('throws, naming symbols, when a re-export target does not resolve', () => {
    // The orphaned-chunk shape: tsup hoists most of `/core` into a shared
    // chunk, and reading the entry without it yields a full symbol list whose
    // declarations are all missing.
    const file = join(scratch, 'orphan.d.ts')
    writeFileSync(
      file,
      "export { Alpha, Beta } from './missing-chunk.js';\n",
      'utf8',
    )

    assert.throws(
      () => readExportSurface(file),
      (error: unknown) => {
        const message = (error as Error).message
        assert.match(message, /have no resolvable declaration/)
        assert.match(message, /Alpha/)
        assert.match(message, /Beta/)
        return true
      },
    )
  })

  test('does not throw on a well-formed entry', () => {
    assert.deepStrictEqual(
      surfaceOf('healthy', 'export declare function ping(): void;\n'),
      { ping: { kind: 'value' } },
    )
  })
})

describe('readExportSurface extraction rules', () => {
  test('separates class statics from instance members', () => {
    const surface = surfaceOf(
      'statics',
      `export declare class Both {
         static create(): Both;
         create(): Both;
         private hidden: number;
         protected guarded: number;
         open: number;
       }\n`,
    )

    assert.deepStrictEqual(surface.Both, {
      kind: 'value',
      members: ['create', 'open', 'static:create'],
    })
  })

  test('keeps numeric and string literal unions apart', () => {
    // `.text` strips quotes, which made these byte-identical — so normalising
    // `RespVersion` from `2 | 3` to `'2' | '3'`, which breaks every consumer
    // passing `{ version: 2 }`, used to diff to nothing.
    const numeric = surfaceOf('numeric', 'export type N = 2 | 3;\n')
    const textual = surfaceOf('textual', "export type N = '2' | '3';\n")

    assert.deepStrictEqual(numeric.N, { kind: 'type', variants: ['2', '3'] })
    assert.deepStrictEqual(textual.N, {
      kind: 'type',
      variants: ["'2'", "'3'"],
    })
    assert.notDeepStrictEqual(numeric.N, textual.N)
  })

  test('records union arms that are not plain string or numeric literals', () => {
    // Recording only string and numeric literals was worse than recording
    // nothing: the entry looked pinned while these arms vanished next to their
    // captured siblings. `-1` in particular is the canonical Redis sentinel.
    const surface = surfaceOf(
      'arms',
      `export type Sentinel = -1 | 0 | 1;
       export type Big = 1n | 2n;
       export type Maybe = 'a' | undefined;
       export type Tmpl = \`a\${string}\` | 'lit';
       export type Refs = Date | RegExp;\n`,
    )

    assert.deepStrictEqual(surface.Sentinel.variants, ['-1', '0', '1'])
    assert.deepStrictEqual(surface.Big.variants, ['1n', '2n'])
    assert.deepStrictEqual(surface.Maybe.variants, ["'a'", 'undefined'])
    assert.deepStrictEqual(surface.Tmpl.variants, ["'lit'", '`a${string}`'])
    assert.deepStrictEqual(surface.Refs.variants, ['Date', 'RegExp'])
  })

  test('takes the union of members across object-shaped union arms', () => {
    // The `CreateIoredisMockOptions` shape: anonymous arms exported nowhere
    // else, so nothing else pins `seed`.
    const surface = surfaceOf(
      'arms-object',
      `export type Opts =
         | { cluster?: false; seed?: readonly string[] }
         | { cluster: { masters: number }; seed?: readonly string[] };\n`,
    )

    assert.deepStrictEqual(surface.Opts, {
      kind: 'type',
      members: ['cluster', 'seed'],
    })
  })

  test('descends intersections without recording the referenced half twice', () => {
    const surface = surfaceOf(
      'intersection',
      `export type Base = { alpha: string };
       export type Wired = Base & { beta: number };\n`,
    )

    assert.deepStrictEqual(surface.Base.members, ['alpha'])
    assert.deepStrictEqual(surface.Wired.members, ['beta'])
  })

  test('prefixes the type half of a declaration-merged symbol', () => {
    // The `RedisValue` shape: a union type and a factory const sharing a name.
    // `Merged.kind` is a field of the union, not something a consumer calls.
    const surface = surfaceOf(
      'merged',
      `export type Merged = { kind: 'a'; value: number };
       export declare const Merged: { make(): Merged };\n`,
    )

    assert.deepStrictEqual(surface.Merged, {
      kind: 'value',
      members: ['make', 'type:kind', 'type:value'],
    })
  })

  test('pins enum members', () => {
    const surface = surfaceOf(
      'enum',
      'export declare enum Level { Low = 0, High = 1 }\n',
    )

    assert.deepStrictEqual(surface.Level, {
      kind: 'value',
      members: ['High', 'Low'],
    })
  })

  test('resolves a mapped-type const to its real keys', () => {
    // `FEATURE_GATES: Record<FeatureId, VersionGate>` — a type reference, so
    // the syntactic walk saw nothing and the checker pass is what pins it.
    const surface = surfaceOf(
      'mapped',
      `export type Key = 'alpha' | 'beta';
       export declare const GATES: Record<Key, number>;\n`,
    )

    assert.deepStrictEqual(surface.GATES, {
      kind: 'value',
      members: ['alpha', 'beta'],
    })
  })

  test('does not duplicate a const annotated with a type this surface exports', () => {
    // The members are pinned under `Opts`. Recording them on `DEFAULTS` too
    // made the outcome depend on whether `Opts` was spelled `type` or
    // `interface` — a change invisible to consumers that the diff then reported
    // as BREAKING.
    const asAlias = surfaceOf(
      'const-alias',
      `export type Opts = { alpha: string; beta: number };
       export declare const DEFAULTS: Opts;\n`,
    )
    const asInterface = surfaceOf(
      'const-interface',
      `export interface Opts { alpha: string; beta: number }
       export declare const DEFAULTS: Opts;\n`,
    )

    assert.deepStrictEqual(asAlias.DEFAULTS, { kind: 'value' })
    assert.deepStrictEqual(asAlias.DEFAULTS, asInterface.DEFAULTS)
    assert.deepStrictEqual(asAlias.Opts.members, ['alpha', 'beta'])
  })

  test('does not pull members out of node_modules', () => {
    // Re-exporting a dependency's type would otherwise snapshot its declared
    // members, turning every version bump of that dependency into baseline
    // churn and its API changes into "breaks" in this package. The symbol is
    // still pinned, just without members.
    //
    // `ioredis` rather than a Node builtin: under this program's options a
    // re-exported builtin does not resolve at all and trips the
    // unresolvable-declaration invariant first, so it cannot exercise this
    // branch. A real dependency is the same churn vector and does reach it.
    const surface = surfaceOf(
      'dependency',
      "export type { Cluster } from 'ioredis';\n",
    )

    assert.ok(surface.Cluster, 're-exported dependency type should be pinned')
    assert.strictEqual(surface.Cluster.members, undefined)
  })
})
