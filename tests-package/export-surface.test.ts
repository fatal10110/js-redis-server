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
//   - a symbol or member that disappears  -> FAIL, named individually. Breaking.
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
// Requires `dist/`; run via `npm run test:package`, which builds first.

import { test, describe, before } from 'node:test'
import assert from 'node:assert'
import { createRequire } from 'node:module'
import { existsSync, readFileSync } from 'node:fs'
import {
  PUBLISHED_ENTRIES,
  declarationFileFor,
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

const entryNames = Object.keys(PUBLISHED_ENTRIES) as PublishedEntry[]

/** How a consumer spells the import for this entry, for failure messages. */
function importPath(entry: PublishedEntry): string {
  return entry === 'index' ? packageName : `${packageName}/core`
}

before(() => {
  assert.ok(
    existsSync(declarationFileFor('index')),
    'dist/ is missing — run `npm run build` first (or use `npm run test:package`)',
  )
})

const actual = new Map<PublishedEntry, ExportSurface>()

before(() => {
  for (const entry of entryNames) {
    actual.set(entry, readExportSurface(declarationFileFor(entry)))
  }
})

type Change = { readonly symbol: string; readonly detail: string }

function removals(
  before_: ExportSurface,
  after: ExportSurface,
): readonly Change[] {
  const out: Change[] = []

  for (const [name, entry] of Object.entries(before_)) {
    const current = after[name]

    if (!current) {
      out.push({ symbol: name, detail: `${name} (${entry.kind}) — gone` })
      continue
    }

    if (entry.kind === 'value' && current.kind === 'type') {
      out.push({
        symbol: name,
        detail: `${name} — was a value export, is now type-only (no runtime binding)`,
      })
    }

    for (const member of entry.members ?? []) {
      if (!(current.members ?? []).includes(member)) {
        out.push({
          symbol: name,
          detail: `${name}.${member} — member gone`,
        })
      }
    }
  }

  return out
}

function additions(
  before_: ExportSurface,
  after: ExportSurface,
): readonly string[] {
  const out: string[] = []

  for (const [name, entry] of Object.entries(after)) {
    const previous = before_[name]

    if (!previous) {
      out.push(`${name} (${entry.kind})`)
      continue
    }

    for (const member of entry.members ?? []) {
      if (!(previous.members ?? []).includes(member)) {
        out.push(`${name}.${member}`)
      }
    }
  }

  return out
}

function bullets(lines: readonly string[]): string {
  return lines.map(line => `  - ${line}`).join('\n')
}

describe('published export surface', () => {
  for (const entry of entryNames) {
    describe(`${importPath(entry)}`, () => {
      test('no exported symbol or member was removed', () => {
        const recorded = baseline.entries[entry]
        assert.ok(
          recorded,
          `export-surface.json has no baseline for the "${entry}" entry — run \`${REFRESH}\``,
        )

        const gone = removals(recorded, actual.get(entry) ?? {})

        assert.deepStrictEqual(
          gone.map(change => change.detail),
          [],
          `BREAKING: ${gone.length} export(s) disappeared from "${importPath(entry)}".\n` +
            `${bullets(gone.map(change => change.detail))}\n` +
            `If the removal is intentional, record it under "Unreleased" in CHANGELOG.md, ` +
            `then refresh the baseline with \`${REFRESH}\` in the same commit.`,
        )
      })

      test('baseline records every current export', () => {
        const added = additions(
          baseline.entries[entry] ?? {},
          actual.get(entry) ?? {},
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

      test('ESM and CJS declarations expose the same symbols', () => {
        const cjs = Object.keys(actual.get(entry) ?? {})
        const esm = Object.keys(
          readExportSurface(declarationFileFor(entry, '.d.mts')),
        )

        assert.deepStrictEqual(
          esm,
          cjs,
          `"${importPath(entry)}" resolves to different symbols under the ` +
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
  for (const entry of entryNames) {
    const expectedValues = () =>
      Object.entries(baseline.entries[entry] ?? {})
        .filter(([, record]) => record.kind === 'value')
        .map(([name]) => name)

    test(`${importPath(entry)} exports every value symbol (require)`, () => {
      const loaded = require(importPath(entry)) as Record<string, unknown>
      const missing = expectedValues().filter(name => !(name in loaded))

      assert.deepStrictEqual(
        missing,
        [],
        `declared by dist/${entry}.d.ts but absent from dist/${entry}.js:\n${bullets(missing)}`,
      )
    })

    test(`${importPath(entry)} exports every value symbol (import)`, async () => {
      const loaded = (await import(importPath(entry))) as Record<
        string,
        unknown
      >
      const missing = expectedValues().filter(name => !(name in loaded))

      assert.deepStrictEqual(
        missing,
        [],
        `declared by dist/${entry}.d.mts but absent from dist/${entry}.mjs:\n${bullets(missing)}`,
      )
    })
  }
})
