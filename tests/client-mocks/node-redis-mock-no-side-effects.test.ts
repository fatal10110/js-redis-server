import { test } from 'node:test'
import assert from 'node:assert'
import { sep } from 'node:path'
import '../../src/client-mocks/node-redis-mock'

// Own file on purpose: `node --test` runs each file in a fresh process, so this
// observes exactly what importing the facade does and nothing else. Do NOT
// import `redis` here.

test('importing the node-redis facade does not load `redis`', async () => {
  // package.json declares `"sideEffects": false`, and ioredis-only consumers
  // import this module too, so it must not pull in the optional `redis` peer
  // (~270 ms). Give any import-time async load time to land before checking.
  await new Promise(resolve => setTimeout(resolve, 50))
  const segment = `${sep}node_modules${sep}redis${sep}`
  assert.deepStrictEqual(
    Object.keys(require.cache).filter(path => path.includes(segment)),
    [],
  )
})
