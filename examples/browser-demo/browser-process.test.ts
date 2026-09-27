import { test, describe } from 'node:test'
import assert from 'node:assert'
import { spawnSync } from 'node:child_process'
import { fileURLToPath, pathToFileURL } from 'node:url'

// The demo bundle's `process` global is vite-plugin-node-polyfills' shim (the
// `process` npm package's browser build; vite.config.ts aliases it), and that
// shim has no `hrtime`. src/core/clock.ts used to call
// `process.hrtime.bigint()` at module load, so the bundle threw before the
// first line of UI code and the page rendered an empty terminal (#499).
//
// This imports the modules backend.ts imports with that exact shim installed
// as the global `process`, then runs MONITOR, whose timestamps come from
// clock.ts. The global has to be swapped before the module graph is
// evaluated, and swapping it under the test runner would break the runner,
// so the check runs in a child process.
const here = (rel: string) => fileURLToPath(new URL(rel, import.meta.url))
const url = (rel: string) => pathToFileURL(here(rel)).href

// On Node 22.6 (the engines floor) a tsx-compiled CommonJS module imported
// from an ESM script exposes only a `default` export; newer versions expose
// the named exports as well. `default` is `module.exports` on all of them.
const CHILD_SCRIPT = `
const nodeProcess = globalThis.process
const unwrap = namespace => namespace.default ?? namespace
const { default: browserProcess } = await import(${JSON.stringify(
  url('./node_modules/vite-plugin-node-polyfills/shims/process/dist/index.js'),
)})
Object.defineProperty(globalThis, 'process', {
  value: browserProcess,
  configurable: true,
  writable: true,
})

const result = { hrtime: typeof globalThis.process.hrtime }
try {
  const { createInMemoryRedis } = unwrap(
    await import(${JSON.stringify(url('../../src/in-memory-client.ts'))}),
  )
  await import(${JSON.stringify(url('../../src/cluster.ts'))})

  const instance = await createInMemoryRedis()
  const monitor = instance.connect()
  const other = instance.connect()
  result.monitorReply = await monitor.command('MONITOR')
  const pushes = monitor.pushes()[Symbol.asyncIterator]()
  await other.command('SET', 'demo', 'value')
  result.monitorLine = (await pushes.next()).value
  instance.close()
} catch (error) {
  result.error = String(error && error.stack ? error.stack : error)
}
nodeProcess.stdout.write(JSON.stringify(result))
`

describe("src under the plugin's browser process shim", () => {
  test('imports without process.hrtime and MONITOR stamps lines', () => {
    const child = spawnSync(
      process.execPath,
      [
        '--import',
        'tsx',
        '--no-warnings',
        '--input-type=module',
        '-e',
        CHILD_SCRIPT,
      ],
      { cwd: here('.'), encoding: 'utf8', timeout: 30000 },
    )
    assert.strictEqual(child.status, 0, `${child.stdout}${child.stderr}`)

    const result = JSON.parse(child.stdout) as {
      hrtime: string
      error?: string
      monitorReply?: unknown
      monitorLine?: unknown
    }
    // If the plugin's shim ever grows an hrtime this test stops proving
    // anything; fail loudly rather than pass vacuously.
    assert.strictEqual(result.hrtime, 'undefined')
    assert.strictEqual(result.error, undefined, result.error)
    assert.strictEqual(result.monitorReply, 'OK')
    assert.match(
      String(result.monitorLine),
      /^\d+\.\d{6} \[0 [^\]]+\] "SET" "demo" "value"$/,
    )
  })
})
