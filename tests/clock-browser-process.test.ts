import { describe, test } from 'node:test'
import assert from 'node:assert'
import { spawnSync } from 'node:child_process'
import path from 'node:path'
import { pathToFileURL } from 'node:url'

/**
 * Browser bundles give the package a `process` polyfill with no `hrtime`
 * (vite-plugin-node-polyfills uses the `process` npm package's browser build).
 * `src/core/clock.ts` used to call `process.hrtime.bigint()` at module load, so
 * importing the package in a browser threw before any caller code ran and the
 * Pages demo rendered an empty terminal (#499).
 *
 * The package has to be imported into a fresh module graph with the global
 * already swapped, and swapping `process` inside the test runner's own process
 * would break the runner, so this runs in a child process.
 */
const ROOT = path.resolve(__dirname, '..')

const CHILD_SCRIPT = `
const nodeProcess = globalThis.process
// The shape of the \`process\` npm package's browser build: no hrtime.
const browserProcess = {
  browser: true,
  title: 'browser',
  env: {},
  argv: [],
  version: '',
  versions: {},
  nextTick: (fn, ...args) => queueMicrotask(() => fn(...args)),
  cwd: () => '/',
}
Object.defineProperty(globalThis, 'process', {
  value: browserProcess,
  configurable: true,
  writable: true,
})

const result = {}
try {
  const pkg = await import(${JSON.stringify(pathToFileURL(path.join(ROOT, 'src/index.ts')).href)})
  const core = await import(${JSON.stringify(pathToFileURL(path.join(ROOT, 'src/internal.ts')).href)})
  result.hrtime = typeof globalThis.process.hrtime
  result.samples = []
  for (let i = 0; i < 64; i++) {
    result.samples.push(core.monitorTimestampMicros())
  }
  result.now = Date.now()

  const instance = await pkg.createInMemoryRedis()
  const monitor = instance.connect()
  const other = instance.connect()
  result.monitorReply = await monitor.command('MONITOR')
  const pushes = monitor.pushes()[Symbol.asyncIterator]()
  await other.command('SET', 'clock-browser', 'value')
  result.monitorLine = (await pushes.next()).value
  instance.close()
} catch (error) {
  result.error = String(error && error.stack ? error.stack : error)
}
nodeProcess.stdout.write(JSON.stringify(result))
`

type ChildResult = {
  error?: string
  hrtime?: string
  samples?: number[]
  now?: number
  monitorReply?: unknown
  monitorLine?: unknown
}

function runWithoutHrtime(): ChildResult {
  const result = spawnSync(
    process.execPath,
    [
      '--import',
      'tsx',
      '--no-warnings',
      '--input-type=module',
      '-e',
      CHILD_SCRIPT,
    ],
    { cwd: ROOT, encoding: 'utf8', timeout: 30000 },
  )
  assert.strictEqual(result.status, 0, `${result.stdout}${result.stderr}`)
  return JSON.parse(result.stdout) as ChildResult
}

describe('clock without process.hrtime (browser bundles)', () => {
  test('importing the package does not throw and MONITOR still stamps lines', () => {
    const result = runWithoutHrtime()
    assert.strictEqual(result.error, undefined, result.error)
    assert.strictEqual(result.hrtime, 'undefined')

    const samples = result.samples!
    for (let i = 1; i < samples.length; i++) {
      assert.ok(Number.isSafeInteger(samples[i]))
      assert.ok(
        samples[i] >= samples[i - 1],
        `sample ${i} went backwards: ${samples[i - 1]} then ${samples[i]}`,
      )
    }
    // performance.now() is the fallback, and in Node it is sub-millisecond.
    assert.ok(samples.some(sample => sample % 1000 !== 0))
    assert.ok(Math.abs(samples[samples.length - 1] / 1000 - result.now!) < 1000)

    assert.strictEqual(result.monitorReply, 'OK')
    assert.match(
      String(result.monitorLine),
      /^\d+\.\d{6} \[0 [^\]]+\] "SET" "clock-browser" "value"$/,
    )
  })
})
