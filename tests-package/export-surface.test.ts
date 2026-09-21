// Guards the published export surface of both entry points — the package root
// (`js-redis-server`) and the `js-redis-server/core` subpath — against silent
// removals.
//
// The sibling `dual-format.test.ts` spot-checks a handful of names by hand;
// anything outside that list can disappear with CI green. PRs #374, #375, #376
// and #377 each removed something from `/core` and all four shipped green.
//
// This test snapshots the *whole* surface into `export-surface.json` and diffs:
//
//   - a symbol, member or union variant that disappears -> FAIL, named
//     individually. Breaking.
//   - a `value` export that becomes type-only -> FAIL. Breaking: it still type
//     checks at the import site but the runtime binding is gone.
//   - an addition -> FAIL, in its own test, labelled as *not* a break.
//
// Additions fail on purpose. Silently tolerating them means a symbol added in
// one PR never reaches the baseline, so deleting it in the next PR is invisible
// — the exact hole this test exists to close. The fix is one command
// (`npm run export-baseline`), and the resulting diff is the review signal:
// "this PR widens the published API" should be visible in the diff, not only in
// the PR body. The two failures are separate tests with distinct wording so
// nobody has to guess whether they just broke consumers or forgot a snapshot.
//
// The diff functions themselves are unit-tested in `export-surface-diff.test.ts`
// against synthetic surfaces — every assertion here is `deepStrictEqual(x, [])`,
// which also passes when the producer is broken.
//
// Requires `dist/`; run via `npm run test:package`, which builds first.

import { test, describe, before } from 'node:test'
import assert from 'node:assert'
import { createRequire } from 'node:module'
import { existsSync, readFileSync } from 'node:fs'
import {
  PUBLISHED_ENTRY_NAMES,
  declarationFileFor,
  findAdditions,
  findRemovals,
  importPathFor,
  readExportSurface,
  type ExportBaseline,
  type ExportSurface,
  type PublishedEntry,
} from './export-surface.js'

const require = createRequire(import.meta.url)
const { name: packageName } = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
) as { name: string }

const baseline = JSON.parse(
  readFileSync(new URL('./export-surface.json', import.meta.url), 'utf8'),
) as ExportBaseline

const REFRESH = 'npm run export-baseline'

function importPath(entry: PublishedEntry): string {
  return importPathFor(packageName, entry)
}

before(() => {
  assert.ok(
    existsSync(declarationFileFor('index')),
    'dist/ is missing — run `npm run build` first (or use `npm run test:package`)',
  )
})

/** Current `.d.ts` and `.d.mts` surfaces, read once. */
const current = new Map<PublishedEntry, ExportSurface>()
const currentEsm = new Map<PublishedEntry, ExportSurface>()

before(() => {
  for (const entry of PUBLISHED_ENTRY_NAMES) {
    current.set(entry, readExportSurface(declarationFileFor(entry)))
    currentEsm.set(
      entry,
      readExportSurface(declarationFileFor(entry, '.d.mts')),
    )
  }
})

function surfaceOf(entry: PublishedEntry): ExportSurface {
  const surface = current.get(entry)
  assert.ok(surface, `no surface read for the "${entry}" entry`)
  return surface
}

function bullets(lines: readonly string[]): string {
  return lines.map(line => `  - ${line}`).join('\n')
}

describe('published export surface', () => {
  for (const entry of PUBLISHED_ENTRY_NAMES) {
    describe(`${importPath(entry)}`, () => {
      test('no exported symbol, member or variant was removed', () => {
        const recorded = baseline.entries[entry]
        assert.ok(
          recorded,
          `export-surface.json has no baseline for the "${entry}" entry — run \`${REFRESH}\``,
        )

        const gone = findRemovals(recorded, surfaceOf(entry))

        assert.deepStrictEqual(
          gone,
          [],
          `BREAKING: ${gone.length} export(s) disappeared from "${importPath(entry)}".\n` +
            `${bullets(gone)}\n` +
            `If the removal is intentional, record it under "Unreleased" in CHANGELOG.md, ` +
            `then refresh the baseline with \`${REFRESH}\` in the same commit.`,
        )
      })

      test('baseline records every current export', () => {
        const added = findAdditions(
          baseline.entries[entry] ?? {},
          surfaceOf(entry),
        )

        assert.deepStrictEqual(
          added,
          [],
          `The export baseline is stale for "${importPath(entry)}" — ${added.length} addition(s).\n` +
            `${bullets(added)}\n` +
            `Additions are NOT a breaking change; this is a bookkeeping failure. ` +
            `Run \`${REFRESH}\` and commit tests-package/export-surface.json so the ` +
            `next PR that deletes one of these is caught.`,
        )
      })

      test('ESM and CJS declarations describe the same surface', () => {
        // Deep, not just the symbol names: the baseline is only ever read from
        // `.d.ts`, so this is the sole thing pinning `.d.mts` at member and
        // kind level. A dual-emit drift that dropped a method from the ESM
        // declarations alone would otherwise ship green.
        assert.deepStrictEqual(
          currentEsm.get(entry),
          surfaceOf(entry),
          `"${importPath(entry)}" describes a different surface under the ` +
            `import and require conditions — the dual build has drifted.`,
        )
      })
    })
  }
})

describe('published export surface reaches the runtime', () => {
  // The declaration bundle is a claim about the JS. These two tests check it is
  // true for the half of the surface that has a runtime binding: a symbol the
  // types promise but the bundler dropped would otherwise ship broken.
  //
  // Driven from the *current* declarations rather than the committed baseline,
  // so a newly added export that the bundler drops fails on the same run that
  // introduces it, not on the next one.
  for (const entry of PUBLISHED_ENTRY_NAMES) {
    const expectedValues = (): string[] =>
      Object.entries(surfaceOf(entry))
        .filter(([, record]) => record.kind === 'value')
        .map(([name]) => name)

    test(`${importPath(entry)} exports every value symbol (require)`, () => {
      const loaded = require(importPath(entry)) as Record<string, unknown>
      const absent = expectedValues().filter(name => !(name in loaded))

      assert.deepStrictEqual(
        absent,
        [],
        `declared by the emitted .d.ts but absent from the CJS build:\n${bullets(absent)}`,
      )
    })

    test(`${importPath(entry)} exports every value symbol (import)`, async () => {
      const loaded = (await import(importPath(entry))) as Record<
        string,
        unknown
      >
      const absent = expectedValues().filter(name => !(name in loaded))

      assert.deepStrictEqual(
        absent,
        [],
        `declared by the emitted .d.mts but absent from the ESM build:\n${bullets(absent)}`,
      )
    })
  }
})
