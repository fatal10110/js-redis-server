/**
 * Preload for the `socketless` integration backend (#412) — loaded with
 * `--import` by the `test:integration:socketless*` npm scripts only, so the
 * mock and real runs never see it.
 *
 * The existing `ioredis/**` and `node-redis/**` suites run unchanged against
 * the socketless client mocks. The cases those mocks cannot pass yet are
 * listed centrally in {@link SOCKETLESS_KNOWN_GAPS} instead of being marked in
 * each test file: this preload wraps `node:test`'s `test`/`it`/`describe`/
 * `suite` (and their `.skip`/`.todo`/`.only` variants) so a listed test gets
 * `{ todo }` (still runs; failure reported, not fatal) and a listed file whose
 * setup the backend cannot provide gets `{ skip }`, with the listed reason.
 * The default export (`import test from 'node:test'`) is not patched; no suite
 * uses it.
 *
 * The list is strict. When a test file finishes, a root `after` hook fails the
 * file if any listed title:
 *  - matched no test, or matched tests in more than one suite (ambiguous — list
 *    it by its full `Suite > Sub > title` path instead);
 *  - passes now (fixed: delete it from the list);
 *  - failed with an error its entry's `error` pattern does not match (a
 *    different failure than the one recorded — a regression hiding behind the
 *    todo);
 *  - never ran its body (a hook failed first: the cause is not the recorded
 *    one, so the file needs a `skip` entry);
 *  - ran its body but never finished it — node:test timed it out or cancelled
 *    it, so it neither passed nor failed with the recorded error (a hang is a
 *    different failure too).
 * Every `todo` entry names its titles; only `skip` entries may cover a whole
 * file, so a test added to a listed file still has to pass or be listed.
 *
 * `SOCKETLESS_AUDIT_SKIPS=1` runs `skip` entries' files anyway (as todo) and
 * fails the file if any of their tests pass, so a skip can be narrowed.
 *
 * Before any of that, every test file's process checks the whole list: each
 * entry's `file` must exist (a renamed or deleted file cannot leave a dead
 * entry) and each `todo` entry must name its titles and `error`. A bad list
 * fails every file. (The `--test` orchestrator itself does not run `--import`
 * preloads, so the check cannot live there.)
 *
 * `SOCKETLESS_KNOWN_GAPS_MODULE` swaps in another list (a module exporting
 * `SOCKETLESS_KNOWN_GAPS`); only `tests/socketless-register.test.ts` uses it,
 * to run this preload against fixtures.
 */
import { createRequire, syncBuiltinESMExports } from 'node:module'
import { existsSync } from 'node:fs'
import path from 'node:path'
import {
  SOCKETLESS_KNOWN_GAPS as DEFAULT_KNOWN_GAPS,
  type KnownGap,
} from './known-gaps'

type TestFn = (...args: unknown[]) => unknown
type Variant = 'skip' | 'todo' | 'only'
type Wrapped = TestFn & Partial<Record<Variant, TestFn>>
/** The mutable CommonJS face of `node:test` this preload patches. */
type NodeTestModule = {
  test: Wrapped
  it: Wrapped
  describe: Wrapped
  suite: Wrapped
  after: (fn: () => void) => void
}

if (process.env.TEST_BACKEND !== 'socketless') {
  throw new Error(
    'tests-integration/socketless/register.ts is only for TEST_BACKEND=socketless',
  )
}

const INTEGRATION_DIR = path.resolve(__dirname, '..')
const INTEGRATION_ROOT = `${path.sep}tests-integration${path.sep}`
const AUDIT_SKIPS = process.env.SOCKETLESS_AUDIT_SKIPS === '1'
const requireHere = createRequire(path.join(process.cwd(), 'noop.js'))

const SOCKETLESS_KNOWN_GAPS: readonly KnownGap[] = process.env
  .SOCKETLESS_KNOWN_GAPS_MODULE
  ? (
      requireHere(path.resolve(process.env.SOCKETLESS_KNOWN_GAPS_MODULE)) as {
        SOCKETLESS_KNOWN_GAPS: readonly KnownGap[]
      }
    ).SOCKETLESS_KNOWN_GAPS
  : DEFAULT_KNOWN_GAPS

validateList()

// `node --test` runs each file in a child it marks with NODE_TEST_CONTEXT,
// handing it that one file on the command line.
if ((process.env.NODE_TEST_CONTEXT ?? '').startsWith('child')) {
  const testFile = process.argv
    .slice(1)
    .find(arg => arg.includes(INTEGRATION_ROOT) && arg.endsWith('.ts'))
  const fileKey = testFile
    ?.slice(testFile.lastIndexOf(INTEGRATION_ROOT) + INTEGRATION_ROOT.length)
    .split(path.sep)
    .join('/')
  const entries = fileKey
    ? SOCKETLESS_KNOWN_GAPS.filter(gap => gap.file === fileKey)
    : []
  if (entries.length > 0) {
    install(entries)
  }
}

/** Every entry names a real file, and every todo entry is precise. */
function validateList(): void {
  const problems: string[] = []
  for (const gap of SOCKETLESS_KNOWN_GAPS) {
    if (!existsSync(path.join(INTEGRATION_DIR, gap.file))) {
      problems.push(`${gap.file}: no such test file`)
    }
    if (gap.mode !== 'skip' && (!gap.test || !gap.error)) {
      problems.push(
        `${gap.file}: a todo entry must list its titles and an \`error\` pattern (only skip entries may cover a whole file)`,
      )
    }
  }
  if (problems.length > 0) {
    throw new Error(
      `invalid tests-integration/socketless/known-gaps.ts:\n  ${problems.join('\n  ')}`,
    )
  }
}

/** What one listed title (or a whole-file skip entry) saw while the file ran. */
type Seen = {
  /** Full `Suite > … > title` path of every test it matched. */
  paths: string[]
  passed: string[]
  ran: Set<string>
  /** Bodies that finished: passed, or failed (with any error). */
  settled: Set<string>
  unexpected: string[]
}

function install(gaps: readonly KnownGap[]): void {
  const nodeTest = requireHere('node:test') as NodeTestModule

  const seen = new Map<string, Seen>()
  const seenFor = (gap: KnownGap, title: string): Seen => {
    const key = `${gaps.indexOf(gap)}\0${title}`
    let entry = seen.get(key)
    if (!entry) {
      entry = {
        paths: [],
        passed: [],
        ran: new Set(),
        settled: new Set(),
        unexpected: [],
      }
      seen.set(key, entry)
    }
    return entry
  }

  const wholeFile = gaps.find(gap => gap.test === undefined)
  const suitePath: string[] = []

  const lookup = (
    name: string,
    fullPath: string,
  ): { gap: KnownGap; title: string } | undefined => {
    for (const gap of gaps) {
      const title = gap.test?.find(t => t === fullPath || t === name)
      if (title !== undefined) {
        return { gap, title }
      }
    }
    return wholeFile ? { gap: wholeFile, title: '' } : undefined
  }

  const wrapTest = (original: Wrapped): Wrapped =>
    withVariants(original, function (this: unknown, ...args: unknown[]) {
      const [name, options, fn] = normalize(args)
      if (name === undefined) {
        return original.apply(this, args)
      }
      const fullPath = [...suitePath, name].join(' > ')
      const match = lookup(name, fullPath)
      if (!match) {
        return original.apply(this, args)
      }
      const record = seenFor(match.gap, match.title)
      record.paths.push(fullPath)
      const skipping = match.gap.mode === 'skip' && !AUDIT_SKIPS
      const reason = `socketless: ${match.gap.reason}`
      return original.apply(
        this,
        rebuild(
          name,
          { ...options, [skipping ? 'skip' : 'todo']: reason },
          fn && !skipping ? observe(fn, fullPath, record, match.gap.error) : fn,
        ),
      )
    })

  const wrapSuite = (original: Wrapped): Wrapped =>
    withVariants(original, function (this: unknown, ...args: unknown[]) {
      const [name, options, fn] = normalize(args)
      const skipping = wholeFile?.mode === 'skip' && !AUDIT_SKIPS
      if (wholeFile && suitePath.length === 0) {
        seenFor(wholeFile, '').paths.push(name ?? '<anonymous suite>')
      }
      const body =
        fn &&
        function (this: unknown, ...inner: unknown[]) {
          // node:test runs a suite's body synchronously while registering it,
          // so this stack names the enclosing suites of every test() inside.
          suitePath.push(name ?? '<anonymous suite>')
          try {
            return fn.apply(this, inner)
          } finally {
            suitePath.pop()
          }
        }
      return original.apply(
        this,
        rebuild(
          name,
          skipping
            ? { ...options, skip: `socketless: ${wholeFile.reason}` }
            : options,
          body,
        ),
      )
    })

  nodeTest.test = wrapTest(nodeTest.test)
  nodeTest.it = wrapTest(nodeTest.it)
  nodeTest.describe = wrapSuite(nodeTest.describe)
  nodeTest.suite = wrapSuite(nodeTest.suite)
  syncBuiltinESMExports()

  nodeTest.after(() => {
    const stale: string[] = []
    for (const gap of gaps) {
      stale.push(
        ...staleness(gap, title => seen.get(`${gaps.indexOf(gap)}\0${title}`)),
      )
    }
    if (stale.length > 0) {
      throw new Error(
        `stale entries in tests-integration/socketless/known-gaps.ts:\n  ${stale.join('\n  ')}`,
      )
    }
  })
}

/** Why an entry no longer describes the file (see the header). */
function staleness(
  gap: KnownGap,
  get: (title: string) => Seen | undefined,
): string[] {
  if (gap.test === undefined) {
    const record = get('')
    if (!record || record.paths.length === 0) {
      return [`${gap.file}: matched no suite or test (renamed or removed?)`]
    }
    return record.passed.map(
      p => `${gap.file} > '${p}': passes now — narrow the skip entry`,
    )
  }

  const problems: string[] = []
  for (const title of gap.test) {
    const where = `${gap.file} > '${title}'`
    const record = get(title)
    if (!record || record.paths.length === 0) {
      problems.push(`${where}: matched no test (renamed or removed?)`)
      continue
    }
    if (new Set(record.paths).size > 1) {
      problems.push(
        `${where}: ambiguous — matched ${[...new Set(record.paths)].join(' | ')}; list the full suite path`,
      )
    }
    if (gap.mode === 'skip') {
      continue
    }
    for (const p of record.passed) {
      problems.push(
        `${gap.file} > '${p}': passes now — remove it from the list`,
      )
    }
    problems.push(...record.unexpected.map(u => `${gap.file} > ${u}`))
    for (const p of record.paths) {
      if (!record.ran.has(p)) {
        problems.push(
          `${gap.file} > '${p}': its body never ran (a hook failed first) — that is not the recorded cause; use a skip entry`,
        )
      } else if (!record.settled.has(p)) {
        problems.push(
          `${gap.file} > '${p}': never finished (timed out or cancelled) — that is not the recorded cause`,
        )
      }
    }
  }
  return problems
}

/**
 * Wrap a todo'd test body to record whether it ran, passed, or failed with the
 * error its entry expects. Callback-style `(t, done)` bodies keep their arity.
 */
function observe(
  fn: TestFn,
  fullPath: string,
  record: Seen,
  expected: RegExp | undefined,
): TestFn {
  const failed = (err: unknown) => {
    record.settled.add(fullPath)
    const message = err instanceof Error ? err.message : String(err)
    if (expected && !expected.test(message)) {
      record.unexpected.push(
        `'${fullPath}': failed with an unexpected error: ${message.split('\n')[0]}`,
      )
    }
  }
  if (fn.length >= 2) {
    // Two declared parameters: node:test tells a callback body by its arity.
    return function (this: unknown, t: unknown, done: unknown) {
      const callback = done as (err?: unknown) => void
      record.ran.add(fullPath)
      try {
        fn.call(this, t, (err?: unknown) => {
          if (err) {
            failed(err)
          } else {
            record.settled.add(fullPath)
            record.passed.push(fullPath)
          }
          callback(err)
        })
      } catch (err) {
        failed(err)
        throw err
      }
    }
  }
  return async function (this: unknown, t: unknown) {
    record.ran.add(fullPath)
    try {
      await fn.call(this, t)
    } catch (err) {
      failed(err)
      throw err
    }
    record.settled.add(fullPath)
    record.passed.push(fullPath)
  }
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

function rebuild(
  name: string | undefined,
  options: Record<string, unknown>,
  fn: TestFn | undefined,
): unknown[] {
  const args: unknown[] = name === undefined ? [] : [name]
  args.push(options)
  if (fn) {
    args.push(fn)
  }
  return args
}

/**
 * Give the wrapper its own `.skip` / `.todo` / `.only`, routed through the
 * wrapper (so a listed `test.skip(...)` is still matched), and keep any other
 * property of the original.
 */
function withVariants(original: Wrapped, wrapper: TestFn): Wrapped {
  const wrapped = Object.assign(wrapper, original) as Wrapped
  for (const variant of ['skip', 'todo', 'only'] as const) {
    wrapped[variant] = function (this: unknown, ...args: unknown[]) {
      const [name, options, fn] = normalize(args)
      return wrapper.apply(
        this,
        rebuild(name, { ...options, [variant]: true }, fn),
      )
    }
  }
  return wrapped
}
