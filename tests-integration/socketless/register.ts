/**
 * Preload for the `socketless` integration backend (#412) — loaded with
 * `--import` by the `test:integration:socketless*` npm scripts only, so the
 * mock and real runs never see it.
 *
 * The existing `ioredis/**` and `node-redis/**` suites run unchanged against
 * the socketless client mocks. The cases those mocks cannot pass yet are
 * listed centrally in {@link SOCKETLESS_KNOWN_GAPS} instead of being marked in
 * each test file: this preload wraps `node:test`'s `test`/`it`/`describe`/
 * `suite` so a listed test gets `{ todo }` (still runs; failure reported, not
 * fatal) or `{ skip }` (not run), with the listed reason.
 *
 * The list is kept honest: when a test file finishes, a root `after` hook
 * fails the file if one of its entries (or listed titles) matched no test —
 * renamed or typo'd — or if a `todo` case passes now: a listed title on its
 * own, a whole-file or RegExp entry once every test it matched passes. So a fix
 * in `src/` has to delete its entry, the way an xfail-strict marker would.
 */
import { createRequire, syncBuiltinESMExports } from 'node:module'
import path from 'node:path'
import { SOCKETLESS_KNOWN_GAPS, type KnownGap } from './known-gaps'

type TestFn = (...args: unknown[]) => unknown
/** The mutable CommonJS face of `node:test` this preload patches. */
type NodeTestModule = {
  test: TestFn
  it: TestFn
  describe: TestFn
  suite: TestFn
  after: (fn: () => void) => void
}

const INTEGRATION_ROOT = `${path.sep}tests-integration${path.sep}`

// Only a test-file child process has anything to wrap. `node --test` runs each
// file in a child it marks with NODE_TEST_CONTEXT, handing it that one file on
// the command line; the orchestrating parent (which also inherits `--import`)
// is left alone.
const isTestFileProcess = (process.env.NODE_TEST_CONTEXT ?? '').startsWith(
  'child',
)
const testFile = isTestFileProcess
  ? process.argv
      .slice(1)
      .find(arg => arg.includes(INTEGRATION_ROOT) && arg.endsWith('.test.ts'))
  : undefined
const fileKey = testFile
  ?.slice(testFile.lastIndexOf(INTEGRATION_ROOT) + INTEGRATION_ROOT.length)
  .split(path.sep)
  .join('/')

const entries: readonly KnownGap[] = fileKey
  ? SOCKETLESS_KNOWN_GAPS.filter(gap => gap.file === fileKey)
  : []

if (entries.length > 0) {
  install(entries)
}

function install(gaps: readonly KnownGap[]): void {
  const require = createRequire(path.join(process.cwd(), 'noop.js'))
  const nodeTest = require('node:test') as NodeTestModule

  /** Per entry: the titles it matched, and which of those passed. */
  const tally = new Map<KnownGap, Tally>()
  for (const gap of gaps) {
    tally.set(gap, { matched: [], passed: new Set() })
  }

  const wholeFile = gaps.find(gap => gap.test === undefined)

  const lookup = (name: string): KnownGap | undefined =>
    gaps.find(gap => matchesTitle(gap.test, name)) ?? wholeFile

  const wrapTest = (original: TestFn): TestFn =>
    copyProps(original, function (this: unknown, ...args: unknown[]) {
      const [name, options, fn] = normalize(args)
      const gap = name === undefined ? wholeFile : lookup(name)
      if (!gap) {
        return original.apply(this, args)
      }
      const counts = tally.get(gap)!
      const title = name ?? '<anonymous>'
      counts.matched.push(title)
      const mode = gap.mode ?? 'todo'
      const reason = `socketless: ${gap.reason}`
      return original.call(
        this,
        name,
        { ...options, [mode]: reason },
        fn && mode === 'todo'
          ? recordPass(fn, () => counts.passed.add(title))
          : fn,
      )
    })

  // Only whole-file `skip` entries act on suites: a suite whose `before` hook
  // cannot even set up (it needs a TCP port) must not run at all.
  const wrapSuite = (original: TestFn): TestFn =>
    copyProps(original, function (this: unknown, ...args: unknown[]) {
      if (wholeFile?.mode !== 'skip') {
        return original.apply(this, args)
      }
      const [name, options, fn] = normalize(args)
      tally.get(wholeFile)!.matched.push(name ?? '<anonymous suite>')
      return original.call(
        this,
        name,
        { ...options, skip: `socketless: ${wholeFile.reason}` },
        fn,
      )
    })

  nodeTest.test = wrapTest(nodeTest.test)
  nodeTest.it = wrapTest(nodeTest.it)
  nodeTest.describe = wrapSuite(nodeTest.describe)
  nodeTest.suite = wrapSuite(nodeTest.suite)
  syncBuiltinESMExports()

  nodeTest.after(() => {
    const stale: string[] = []
    for (const [gap, { matched, passed }] of tally) {
      stale.push(...staleness(gap, matched, passed))
    }
    if (stale.length > 0) {
      throw new Error(
        `stale entries in tests-integration/socketless/known-gaps.ts:\n  ${stale.join('\n  ')}`,
      )
    }
  })
}

type Tally = { matched: string[]; passed: Set<string> }

/**
 * Why an entry no longer describes the file: it (or one of its listed titles)
 * matched no test, or its todo'd tests pass now. A listed title is checked on
 * its own, so fixing one case of a list flags exactly that title.
 */
function staleness(
  gap: KnownGap,
  matched: readonly string[],
  passed: ReadonlySet<string>,
): string[] {
  const where = `${gap.file}${gap.test instanceof RegExp ? ` > ${String(gap.test)}` : ''}`
  if (Array.isArray(gap.test)) {
    const problems: string[] = []
    for (const title of gap.test as readonly string[]) {
      if (!matched.includes(title)) {
        problems.push(
          `${where} > '${title}': matched no test (renamed or removed?)`,
        )
      } else if ((gap.mode ?? 'todo') === 'todo' && passed.has(title)) {
        problems.push(
          `${where} > '${title}': passes now — remove it from the list`,
        )
      }
    }
    return problems
  }
  const label =
    typeof gap.test === 'string' ? `${where} > '${gap.test}'` : where
  if (matched.length === 0) {
    return [`${label}: matched no test (renamed or removed?)`]
  }
  if ((gap.mode ?? 'todo') === 'todo' && matched.every(t => passed.has(t))) {
    return [
      `${label}: all ${matched.length} matched test(s) pass now — delete the entry`,
    ]
  }
  return []
}

function matchesTitle(selector: KnownGap['test'], name: string): boolean {
  if (selector === undefined) {
    return false
  }
  if (typeof selector === 'string') {
    return selector === name
  }
  if (selector instanceof RegExp) {
    return selector.test(name)
  }
  return selector.includes(name)
}

/** `test([name][, options][, fn])` → `[name, options, fn]`. */
function normalize(
  args: unknown[],
): [string | undefined, Record<string, unknown>, TestFn | undefined] {
  const rest = [...args]
  const name =
    typeof rest[0] === 'string' ? (rest.shift() as string) : undefined
  const options =
    rest[0] !== null && typeof rest[0] === 'object'
      ? (rest.shift() as Record<string, unknown>)
      : {}
  const fn = typeof rest[0] === 'function' ? (rest[0] as TestFn) : undefined
  return [name, options, fn]
}

/**
 * Count a pass for a todo'd test body. A `(t, done)` callback-style body is
 * passed through untouched (its arity is how node:test detects it), so it can
 * never make its entry look stale.
 */
function recordPass(fn: TestFn, onPass: () => void): TestFn {
  if (fn.length >= 2) {
    return fn
  }
  return async function (this: unknown, t: unknown) {
    await fn.call(this, t)
    onPass()
  }
}

/** Keep `test.skip`, `test.only`, `describe.todo`, … on the wrapper. */
function copyProps(original: TestFn, wrapper: TestFn): TestFn {
  return Object.assign(wrapper, original)
}
