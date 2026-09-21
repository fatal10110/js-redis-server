// Regenerates `tests-package/export-surface.json` from the current `dist/`.
//
// Run it with `npm run export-baseline` (which builds first) and commit the
// result in the same change that altered the surface. See the header of
// `export-surface.test.ts` for what the baseline is for.

import { writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import * as prettier from 'prettier'
import { readAllEntrySurfaces, type ExportBaseline } from './export-surface.js'

const baselineFile = fileURLToPath(
  new URL('./export-surface.json', import.meta.url),
)

async function main(): Promise<void> {
  const baseline: ExportBaseline = {
    generatedBy: 'npm run export-baseline',
    entries: readAllEntrySurfaces(),
  }

  // Format through the repo's own Prettier config so the committed file passes
  // `prettier --check "**/*.{js,ts,json}"` in CI.
  const config = await prettier.resolveConfig(baselineFile)
  const formatted = await prettier.format(JSON.stringify(baseline), {
    ...config,
    filepath: baselineFile,
    parser: 'json',
  })

  await writeFile(baselineFile, formatted, 'utf8')

  const counts = Object.entries(baseline.entries)
    .map(([entry, surface]) => `${entry}: ${Object.keys(surface).length}`)
    .join(', ')

  console.log(`wrote ${baselineFile} (${counts})`)
}

main().catch((error: unknown) => {
  console.error(error)
  process.exitCode = 1
})
