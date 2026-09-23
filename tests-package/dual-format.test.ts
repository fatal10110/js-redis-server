import { test, describe, before } from 'node:test'
import assert from 'node:assert'
import { createRequire } from 'node:module'
import { execFileSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

// Validates the published package end-to-end: the `exports` map, the dual
// ESM + CJS builds, and the `/core` subpath under either npm name. Resolution goes
// through the package *name* (self-reference), so this exercises the real
// `exports` conditions a consumer hits — not relative dist paths.
//
// Requires `dist/` to exist; run via `npm run test:package` (which builds first).

const require = createRequire(import.meta.url)
const { name: packageName } = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
) as { name: string }
const distIndex = fileURLToPath(new URL('../dist/index.js', import.meta.url))

before(() => {
  assert.ok(
    existsSync(distIndex),
    'dist/ is missing — run `npm run build` first (or use `npm run test:package`)',
  )
})

async function assertWorkingRoot(pkg: Record<string, unknown>): Promise<void> {
  assert.strictEqual(typeof pkg.createRedisMock, 'function')
  assert.strictEqual(typeof pkg.createValkeyMock, 'function')
  assert.strictEqual(pkg.createValkeyMock, pkg.createRedisMock)
  assert.strictEqual(typeof pkg.createRedisServer, 'function')
  assert.strictEqual(typeof pkg.createRedisCluster, 'function')
  assert.strictEqual(typeof pkg.createInMemoryClient, 'function')
  assert.strictEqual(typeof pkg.InMemoryRedisClient, 'function')
  assert.strictEqual(typeof pkg.buildRedisCluster, 'function')
  assert.strictEqual(pkg.buildRedisCluster, pkg.createRedisCluster)
  assert.strictEqual(typeof pkg.RedisCommandError, 'function')
  // The executor and hand-wiring building blocks are intentionally not part of
  // the root surface — they live on `js-redis-server/core`.
  assert.strictEqual('executor' in pkg, false)
  assert.strictEqual('Resp2Server' in pkg, false)
  assert.strictEqual('RedisServerState' in pkg, false)
  assert.strictEqual('createRedisCommandExecutor' in pkg, false)

  const createInMemoryClient = pkg.createInMemoryClient as () => Promise<{
    command(...a: unknown[]): Promise<unknown>
    close(): void
  }>
  const client = await createInMemoryClient()
  assert.strictEqual(await client.command('SET', 'k', 'v'), 'OK')
  assert.strictEqual(await client.command('GET', 'k'), 'v')
  client.close()
}

function assertCore(core: Record<string, unknown>): void {
  assert.strictEqual(typeof core.defineCommand, 'function')
  assert.strictEqual(typeof core.CommandRegistry, 'function')
  assert.strictEqual(typeof core.t, 'object')
  // Hand-wiring building blocks live here, not on the root.
  assert.strictEqual(typeof core.Resp2Server, 'function')
  assert.strictEqual(typeof core.RedisServerState, 'function')
  assert.strictEqual(typeof core.createRedisCommandExecutor, 'function')
  // Facade lives at the root, not in the internals subpath.
  assert.strictEqual(core.createRedisMock, undefined)
}

describe('package CJS entry (require)', () => {
  test('root works through the require condition', async () => {
    await assertWorkingRoot(require(packageName))
  })

  test(`${packageName}/core exposes internals only`, () => {
    assertCore(require(`${packageName}/core`))
  })
})

// The node-redis facade's error classes, checked against the SHIPPED artifacts
// (the build is where a lazy-loading shim lives or dies). Each case runs in a
// fresh child process so no earlier test can have pre-loaded `redis` or warmed
// the facade's cache — the only honest way to observe import-time behaviour.
describe('node-redis facade error classes in the built package', () => {
  const repoRoot = fileURLToPath(new URL('..', import.meta.url))

  function runChild(format: 'cjs' | 'esm', body: string): string {
    const esm = format === 'esm'
    const script = esm
      ? `import { createRequire } from 'node:module'
         const require = createRequire(import.meta.url)
         const load = specifier => import(specifier)
         ${body}`
      : `const load = async specifier => require(specifier)
         ;(async () => { ${body} })().catch(err => { console.error(err); process.exit(1) })`
    return execFileSync(
      process.execPath,
      [...(esm ? ['--input-type=module'] : []), '-e', script],
      { cwd: repoRoot, encoding: 'utf8' },
    ).trim()
  }

  for (const format of ['cjs', 'esm'] as const) {
    test(`${format}: importing the package does not load \`redis\``, () => {
      // package.json declares "sideEffects": false, and ioredis-only consumers
      // import this package too.
      const out = runChild(
        format,
        `await load('${packageName}')
         await new Promise(resolve => setTimeout(resolve, 100))
         console.log(Object.keys(require.cache)
           .some(path => /[\\\\/]node_modules[\\\\/]redis[\\\\/]/.test(path)))`,
      )
      assert.strictEqual(out, 'false')
    })

    test(`${format}: a client built from exported pieces throws the real errors`, () => {
      // The reviewer's repro: a NodeRedisMockClient assembled from the public
      // '.' and './core' entries, bypassing createNodeRedisMock(), running
      // set → quit → quit with no chance to warm anything first.
      const out = runChild(
        format,
        `const { NodeRedisMockClient } = await load('${packageName}')
         const { RedisServerState, createRedisCommandExecutor } =
           await load('${packageName}/core')
         const client = new NodeRedisMockClient({
           state: new RedisServerState({ databaseCount: 1 }),
           executor: createRedisCommandExecutor(),
           ownsState: true,
         })
         await client.set('k', 'v')
         await client.quit()
         let rejected
         try { await client.quit() } catch (err) { rejected = err }
         let thrown
         try { client.destroy() } catch (err) { thrown = err }
         const { ClientClosedError } = await load('redis')
         console.log(JSON.stringify({
           quitRejectedWithReal: rejected instanceof ClientClosedError,
           destroyThrewRealSync: thrown instanceof ClientClosedError,
         }))`,
      )
      assert.deepStrictEqual(JSON.parse(out), {
        quitRejectedWithReal: true,
        destroyThrewRealSync: true,
      })
    })
  }
})

describe('package ESM entry (import)', () => {
  test('root works through the import condition', async () => {
    const pkg = (await import(packageName)) as unknown as Record<
      string,
      unknown
    >
    await assertWorkingRoot(pkg)
  })

  test(`${packageName}/core exposes internals only`, async () => {
    const core = (await import(`${packageName}/core`)) as unknown as Record<
      string,
      unknown
    >
    assertCore(core)
  })
})
