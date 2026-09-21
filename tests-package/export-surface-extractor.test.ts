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
// reads the built declarations. The scratch must sit inside a tree that
// resolves the repo's own `node_modules` — the `ioredis` case below needs that
// dependency to resolve — so it lives under `dist/` (gitignored) rather than
// the OS temp dir. Moving it to `/tmp` silently breaks that one test.

import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
import {
  findAdditions,
  findRemovals,
  readExportSurface,
  type ExportSurface,
} from './export-surface.js'

let scratch: string | undefined

before(() => {
  const distDir = fileURLToPath(new URL('../dist/', import.meta.url))
  mkdirSync(distDir, { recursive: true })
  scratch = mkdtempSync(join(distDir, 'export-surface-fixtures-'))
})

after(() => {
  // Guarded: if `before` threw — a full or read-only disk — an unguarded
  // `rmSync(undefined)` would land an ERR_INVALID_ARG_TYPE at the top of the
  // log, on top of the real failure.
  if (scratch) {
    rmSync(scratch, { recursive: true, force: true })
  }
})

function scratchDir(): string {
  assert.ok(scratch, 'scratch directory was not created')
  return scratch
}

/** Write a `.d.ts` fixture and extract its surface. */
function surfaceOf(name: string, source: string): ExportSurface {
  const file = join(scratchDir(), `${name}.d.ts`)
  writeFileSync(file, source, 'utf8')
  return readExportSurface(file)
}

describe('readExportSurface invariants', () => {
  test('throws when the entry resolves to zero exports', () => {
    const file = join(scratchDir(), 'empty.d.ts')
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
    const file = join(scratchDir(), 'orphan.d.ts')
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

  test('a surface read from inside node_modules keeps its members', () => {
    // The guard used to match `/node_modules/` in the file's own path, so a
    // `dist/` that itself lives under one — a linked or vendored checkout, a
    // workspace install, CI against an installed copy — lost every member and
    // variant while still looking plausible. Neither invariant catches that:
    // the declarations all resolve. Run `export-baseline` there and the
    // emptied baseline gets committed, disarming the guard permanently.
    const source = `export type Foo = { alpha: string; beta: number };
       export declare class Bar { go(): void }\n`

    const normal = surfaceOf('located-normally', source)

    const nested = join(scratchDir(), 'node_modules', 'js-redis-server', 'dist')
    mkdirSync(nested, { recursive: true })
    const nestedFile = join(nested, 'located-under-node-modules.d.ts')
    writeFileSync(nestedFile, source, 'utf8')

    assert.deepStrictEqual(readExportSurface(nestedFile), normal)
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

  test('canonicalises quoting so the baseline does not track source style', () => {
    // Raw `getText()` copied the source's quote characters into the baseline,
    // coupling 87 of 107 variants to `.prettierrc`: flipping `singleQuote`
    // would have reported a pure reformat as 87 breaking removals.
    const single = surfaceOf('quote-single', "export type Q = 'a' | 'b';\n")
    const double = surfaceOf('quote-double', 'export type Q = "a" | "b";\n')

    assert.deepStrictEqual(single.Q.variants, ["'a'", "'b'"])
    assert.deepStrictEqual(single.Q, double.Q)
  })

  test('ignores redundant parentheses around a union arm', () => {
    const plain = surfaceOf('paren-plain', "export type P = 'a' | 'b';\n")
    const parens = surfaceOf('paren-extra', "export type P = ('a') | 'b';\n")

    assert.deepStrictEqual(parens.P, plain.P)
  })

  test('takes the union of members across object-shaped union arms', () => {
    // The `CreateIoredisMockOptions` shape: anonymous arms exported nowhere
    // else, so nothing else pins `seed`. Both arms declare `seed`, which must
    // not make the second arm look like it contributed nothing.
    const surface = surfaceOf(
      'arms-object',
      `export type Opts =
         | { cluster?: false; seed?: readonly string[] }
         | { cluster: { masters: number }; seed?: readonly string[] };\n`,
    )

    assert.deepStrictEqual(surface.Opts, {
      kind: 'type',
      members: ['type:cluster', 'type:seed'],
    })
  })

  test('records a union arm that contributes no nameable members', () => {
    // An arm the extractor descends into but which yields nothing — an
    // intersection of two named types, or an index-signature-only object —
    // used to vanish on both paths, leaving the entry looking pinned by its
    // literal siblings while deleting the whole arm diffed to nothing.
    const surface = surfaceOf(
      'arms-empty',
      `export type Foo = { f: string };
       export type Bar = { b: string };
       export type U = 'lit' | (Foo & Bar);
       export type U2 = 'lit' | { [k: string]: number };\n`,
    )

    assert.deepStrictEqual(surface.U.variants, ["'lit'", 'Foo & Bar'])
    assert.deepStrictEqual(surface.U2.variants, [
      "'lit'",
      '{ [k: string]: number }',
    ])
  })

  test('descends intersections without recording the referenced half twice', () => {
    const surface = surfaceOf(
      'intersection',
      `export type Base = { alpha: string };
       export type Wired = Base & { beta: number };\n`,
    )

    assert.deepStrictEqual(surface.Base.members, ['type:alpha'])
    assert.deepStrictEqual(surface.Wired.members, ['type:beta'])
  })

  test('namespaces members by the declaration they were written on', () => {
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

  test('namespaces the interface spelling of a merge identically', () => {
    // `interface Foo {} + declare const Foo` is the other, more idiomatic way
    // to spell the merge. It used to take the interface branch and flatten
    // unprefixed, so the factory `Coll.name()` could be deleted unreported
    // because the interface half still contributed `name`.
    const surface = surfaceOf(
      'merged-interface',
      `export interface Coll { name: string }
       export declare const Coll: { name(): void; other(): void };\n`,
    )

    assert.deepStrictEqual(surface.Coll, {
      kind: 'value',
      members: ['name', 'other', 'type:name'],
    })
  })

  test('adding a value half is purely additive, not a removal', () => {
    // The prefix follows the declaration, not whether some other declaration
    // exists. A conditional prefix renamed every member of a type alias the
    // moment a const was added beside it, so the diff reported a strictly
    // additive change as BREAKING.
    const before = surfaceOf(
      'additive-before',
      'export type Foo = { a: string };\n',
    )
    const after = surfaceOf(
      'additive-after',
      `export type Foo = { a: string };
       export declare const Foo: { make(): void };\n`,
    )

    assert.deepStrictEqual(findRemovals(before, after), [])
    assert.deepStrictEqual(findAdditions(before, after), [
      'Foo — is now a value export (was type-only)',
      'Foo.make',
    ])
  })

  test('converting an interface to a type alias is invisible', () => {
    const asInterface = surfaceOf(
      'flip-interface',
      'export interface Shape { alpha: string }\n',
    )
    const asAlias = surfaceOf(
      'flip-alias',
      'export type Shape = { alpha: string };\n',
    )

    assert.deepStrictEqual(asInterface.Shape, asAlias.Shape)
    assert.deepStrictEqual(findRemovals(asInterface, asAlias), [])
    assert.deepStrictEqual(findAdditions(asInterface, asAlias), [])
  })

  test('pins enum members with their values', () => {
    // A `const enum` value is inlined into a consumer's build, so changing
    // `A = 0` to `A = 5` is a break of the same class as `2 | 3` becoming
    // `'2' | '3'`, which the union handling goes out of its way to surface.
    const surface = surfaceOf(
      'enum',
      'export declare const enum Level { Low = 0, High = 1 }\n',
    )

    assert.deepStrictEqual(surface.Level, {
      kind: 'value',
      members: ['High=1', 'Low=0'],
    })

    const changed = surfaceOf(
      'enum-changed',
      'export declare const enum Level { Low = 0, High = 5 }\n',
    )

    assert.deepStrictEqual(findRemovals(surface, changed), [
      'Level.High=1 — member gone',
    ])
  })

  test('pins the members of a merged namespace', () => {
    const surface = surfaceOf(
      'namespace',
      `export declare function fn(): void;
       export declare namespace fn { const helper: number; function sub(): void }\n`,
    )

    assert.deepStrictEqual(surface.fn, {
      kind: 'value',
      members: ['helper', 'sub'],
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
    assert.deepStrictEqual(asAlias.Opts.members, ['type:alpha', 'type:beta'])
  })

  test('the const gate fires through a renamed re-export', () => {
    // Matched against the *export* names, the gate missed this: the alias is
    // still called `Opts` while the surface holds `Options`, so `DEFAULTS`
    // duplicated the members and the `type`/`interface` flip-flop stayed
    // reachable through the rename.
    const renamed = `type Opts = { alpha: string; beta: number };
       declare const DEFAULTS: Opts;
       export { type Opts as Options, DEFAULTS };\n`

    const asAlias = surfaceOf('renamed-alias', renamed)
    const asInterface = surfaceOf(
      'renamed-interface',
      renamed.replace(
        'type Opts = { alpha: string; beta: number };',
        'interface Opts { alpha: string; beta: number }',
      ),
    )

    assert.deepStrictEqual(asAlias.DEFAULTS, { kind: 'value' })
    assert.deepStrictEqual(asAlias.Options.members, ['type:alpha', 'type:beta'])
    assert.deepStrictEqual(findRemovals(asAlias, asInterface), [])
    assert.deepStrictEqual(findAdditions(asAlias, asInterface), [])
  })

  test('the const gate does not fire on a mere name collision', () => {
    // The other direction, and the worse one: `DEFAULTS` is annotated with a
    // *non-exported* alias whose name happens to match an unrelated export.
    // Compared by name the gate drops its members, and nothing else pins them.
    const surface = surfaceOf(
      'alias-collision',
      `type Opts = { alpha: string; beta: number };
       declare const DEFAULTS: Opts;
       interface Unrelated { gamma: string }
       export { DEFAULTS, type Unrelated as Opts };\n`,
    )

    assert.deepStrictEqual(surface.DEFAULTS, {
      kind: 'value',
      members: ['alpha', 'beta'],
    })
    assert.deepStrictEqual(surface.Opts.members, ['type:gamma'])
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
