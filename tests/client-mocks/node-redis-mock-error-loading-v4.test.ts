import { test } from 'node:test'
import assert from 'node:assert'
import { createRequire } from 'node:module'
import { createRedisCommandExecutor } from '../../src/commands'
import { NodeRedisMockClient } from '../../src/client-mocks/node-redis-mock'
import { RedisServerState } from '../../src/state'

// Own file on purpose: `node --test` runs each file in a fresh process, and the
// facade resolves node-redis' error classes once, on the first client — so the
// stub below must be in place before anything else can load the real `redis`.
//
// The stub stands in for `redis` ≤ 4.6.11 (@redis/client ≤ 1.5.12): exactly the
// error classes that release exports. It predates `MultiErrorReply` (added in
// @redis/client 1.5.13, i.e. redis 4.6.12) and `SimpleError` (added in v5). A
// missing class must cost only that class, never the ones that do exist (#450).

class AbortError extends Error {}
class WatchError extends Error {
  constructor() {
    super('One (or more) of the watched keys has been changed')
  }
}
class ConnectionTimeoutError extends Error {}
class ClientClosedError extends Error {
  constructor() {
    super('The client is closed')
  }
}
class ClientOfflineError extends Error {}
class DisconnectsClientError extends Error {
  constructor() {
    super('Disconnects client')
  }
}
class SocketClosedUnexpectedlyError extends Error {}
class RootNodesUnavailableError extends Error {}
class ReconnectStrategyError extends Error {}
class ErrorReply extends Error {}

const redis46 = {
  AbortError,
  WatchError,
  ConnectionTimeoutError,
  ClientClosedError,
  ClientOfflineError,
  DisconnectsClientError,
  SocketClosedUnexpectedlyError,
  RootNodesUnavailableError,
  ReconnectStrategyError,
  ErrorReply,
}

// Seed the module cache at the exact path the facade's own `require('redis')`
// resolves to, so it gets the stub instead of the installed v6 package.
const facadeRequire = createRequire(
  require.resolve('../../src/client-mocks/node-redis-mock'),
)
const redisPath = facadeRequire.resolve('redis')
facadeRequire.cache[redisPath] = {
  id: redisPath,
  filename: redisPath,
  loaded: true,
  exports: redis46,
} as NodeJS.Module

function newClient(): NodeRedisMockClient {
  return new NodeRedisMockClient({
    state: new RedisServerState({ databaseCount: 1 }),
    executor: createRedisCommandExecutor(),
    ownsState: true,
  })
}

test('the stub is what the facade loads', () => {
  assert.strictEqual(facadeRequire('redis'), redis46)
})

test('a closed client still throws the installed ClientClosedError', async () => {
  const client = newClient()
  await client.quit()
  await assert.rejects(
    () => client.get('k'),
    (err: unknown) => err instanceof ClientClosedError,
  )
})

test('a hard close still flushes with the installed DisconnectsClientError', async () => {
  const client = newClient()
  const outcome = client.set('k', 'v').then(
    () => 'resolved',
    (err: unknown) => err,
  )
  client.destroy()
  assert.ok((await outcome) instanceof DisconnectsClientError)
})

test('a watch-aborted exec still throws the installed WatchError', async () => {
  const client = newClient()
  const other = await client.duplicate()
  try {
    await client.watch('w')
    await other.set('w', 'changed')
    await assert.rejects(
      () => client.multi().set('w', 'fromTxn').exec(),
      (err: unknown) => err instanceof WatchError,
    )
  } finally {
    await other.quit()
    await client.quit()
  }
})

test('with no SimpleError, server errors are the installed ErrorReply', async () => {
  const client = newClient()
  try {
    await client.set('s', 'notAnInteger')
    await assert.rejects(
      () => client.incr('s'),
      (err: unknown) => {
        assert.strictEqual((err as Error).constructor, ErrorReply)
        assert.strictEqual(
          (err as Error).message,
          'ERR value is not an integer or out of range',
        )
        return true
      },
    )
  } finally {
    await client.quit()
  }
})

test('with no MultiErrorReply, the stand-in still extends the installed ErrorReply', async () => {
  const client = newClient()
  try {
    await client.set('s', 'notAnInteger')
    await assert.rejects(
      () => client.multi().set('ok', 'v').incr('s').exec(),
      (err: unknown) => {
        assert.ok(err instanceof ErrorReply)
        const multiErr = err as ErrorReply & {
          replies: unknown[]
          errorIndexes: number[]
        }
        assert.strictEqual(multiErr.constructor.name, 'MultiErrorReply')
        assert.deepStrictEqual(multiErr.errorIndexes, [1])
        assert.strictEqual(multiErr.replies[0], 'OK')
        assert.strictEqual(
          (multiErr.replies[1] as Error).constructor,
          ErrorReply,
        )
        return true
      },
    )
  } finally {
    await client.quit()
  }
})
