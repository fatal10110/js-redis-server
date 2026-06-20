import { test, describe, before } from 'node:test'
import assert from 'node:assert'
import { createRequire } from 'node:module'
import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

// Validates the published package end-to-end: the `exports` map, the dual
// ESM + CJS builds, and the `js-redis-server/core` subpath. Resolution goes
// through the package *name* (self-reference), so this exercises the real
// `exports` conditions a consumer hits — not relative dist paths.
//
// Requires `dist/` to exist; run via `npm run test:package` (which builds first).

const require = createRequire(import.meta.url)
const distIndex = fileURLToPath(new URL('../dist/index.js', import.meta.url))

before(() => {
  assert.ok(
    existsSync(distIndex),
    'dist/ is missing — run `npm run build` first (or use `npm run test:package`)',
  )
})

async function assertWorkingRoot(pkg: Record<string, unknown>): Promise<void> {
  assert.strictEqual(typeof pkg.createRedisMock, 'function')
  assert.strictEqual(typeof pkg.createRedisServer, 'function')
  assert.strictEqual(typeof pkg.createRedisCluster, 'function')
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

  const createRedisMock = pkg.createRedisMock as (o: unknown) => Promise<{
    client(): { command(...a: unknown[]): Promise<unknown> }
    close(): Promise<void>
  }>
  const mock = await createRedisMock({ transport: 'memory' })
  const client = mock.client()
  assert.strictEqual(await client.command('SET', 'k', 'v'), 'OK')
  assert.strictEqual(await client.command('GET', 'k'), 'v')
  await mock.close()
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
    await assertWorkingRoot(require('js-redis-server'))
  })

  test('js-redis-server/core exposes internals only', () => {
    assertCore(require('js-redis-server/core'))
  })
})

describe('package ESM entry (import)', () => {
  test('root works through the import condition', async () => {
    const pkg = (await import('js-redis-server')) as unknown as Record<
      string,
      unknown
    >
    await assertWorkingRoot(pkg)
  })

  test('js-redis-server/core exposes internals only', async () => {
    const core = (await import('js-redis-server/core')) as unknown as Record<
      string,
      unknown
    >
    assertCore(core)
  })
})
