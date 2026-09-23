import { test } from 'node:test'
import assert from 'node:assert'
import { createRequire } from 'node:module'
import { createRedisCommandExecutor } from '../../src/commands'
import {
  NodeRedisMockClient,
  NodeRedisMockCluster,
} from '../../src/client-mocks/node-redis-mock'
import { RedisServerState } from '../../src/state'

// Own file on purpose: `node --test` runs each file in a fresh process, so no
// other test can have warmed the facade's error-class cache first. Clients here
// are built from the exported pieces, bypassing createNodeRedisMock(), the way
// a consumer of the public `NodeRedisMockClient` / `NodeRedisMockCluster`
// classes can. `redis` is required only AFTER the close calls under test, so it
// cannot be what warmed anything.

function newClient(): NodeRedisMockClient {
  return new NodeRedisMockClient({
    state: new RedisServerState({ databaseCount: 1 }),
    executor: createRedisCommandExecutor(),
    ownsState: true,
  })
}

/** The `redis` package, loaded synchronously — only ever after the closes. */
function realRedis(): typeof import('redis') {
  return createRequire(__filename)('redis') as typeof import('redis')
}

test('a client closed with no await in between throws the real class', () => {
  // Fully synchronous: nothing gets a tick in which to load `redis` lazily, so
  // this only passes if the class is resolved when it is first needed.
  const client = newClient()
  client.destroy()
  let thrown: unknown
  try {
    client.destroy()
  } catch (err) {
    thrown = err
  }
  assert.ok(
    thrown instanceof realRedis().ClientClosedError,
    'the documented `instanceof ClientClosedError` idiom must hold',
  )
})

test('set → quit → quit rejects with the real ClientClosedError', async () => {
  const client = newClient()
  await client.set('k', 'v')
  assert.strictEqual(await client.quit(), 'OK')
  await assert.rejects(
    () => client.quit(),
    (err: unknown) => err instanceof realRedis().ClientClosedError,
  )
})

test('a hard close flushes with the real DisconnectsClientError', async () => {
  const client = newClient()
  // Capture the outcome immediately: the command rejects on a later microtask.
  const outcome = client.set('k', 'v').then(
    () => 'resolved',
    (err: unknown) => err,
  )
  client.destroy()
  assert.ok((await outcome) instanceof realRedis().DisconnectsClientError)
})

test('a directly created cluster throws the real ClientClosedError', async () => {
  const cluster = NodeRedisMockCluster.create({ masters: 3 })
  await cluster.quit()
  await assert.rejects(
    () => cluster.get('k'),
    (err: unknown) => err instanceof realRedis().ClientClosedError,
  )
})
