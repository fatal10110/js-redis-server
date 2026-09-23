import { test } from 'node:test'
import assert from 'node:assert'
import Module, { createRequire } from 'node:module'
import { createRedisCommandExecutor } from '../../src/commands'
import { NodeRedisMockClient } from '../../src/client-mocks/node-redis-mock'
import { RedisServerState } from '../../src/state'

// Own file on purpose: `node --test` runs each file in a fresh process, and the
// facade resolves node-redis' error classes once, on the first client — so
// `redis` must be unresolvable before anything can load it.
//
// With no `redis` at all the facade falls back to its local stand-ins, which
// model v6 (the version it tracks): same messages, `name`, `constructor.name`
// and shape as node-redis' own classes, just not `instanceof` any of them.

type ResolveFilename = (request: string, ...rest: unknown[]) => string
const moduleInternals = Module as unknown as {
  _resolveFilename: ResolveFilename
}
const resolveFilename = moduleInternals._resolveFilename
moduleInternals._resolveFilename = function (request, ...rest) {
  if (request === 'redis') {
    throw Object.assign(new Error("Cannot find module 'redis'"), {
      code: 'MODULE_NOT_FOUND',
    })
  }
  return resolveFilename.call(this, request, ...rest)
}

function newClient(): NodeRedisMockClient {
  return new NodeRedisMockClient({
    state: new RedisServerState({ databaseCount: 1 }),
    executor: createRedisCommandExecutor(),
    ownsState: true,
  })
}

// The real classes, reached past the blocked `redis` entry point, to prove the
// facade really fell back rather than loading them some other way.
const localRequire = createRequire(__filename)
const realClasses = localRequire('@redis/client') as Record<string, unknown>

/** Assert `err` mirrors a node-redis class it cannot be `instanceof`. */
function assertStandIn(err: unknown, className: string, message: string) {
  assert.ok(err instanceof Error)
  const RealClass = realClasses[className] as new () => Error
  assert.strictEqual(typeof RealClass, 'function')
  assert.ok(!(err instanceof RealClass))
  assert.strictEqual(err.constructor.name, className)
  assert.strictEqual(err.name, 'Error')
  assert.strictEqual(err.message, message)
}

test('redis is unresolvable in this process', () => {
  // Guards the premise: if the hook stopped applying, every test below would
  // be exercising the installed v6 classes instead of the stand-ins.
  assert.throws(() => localRequire('redis'), { code: 'MODULE_NOT_FOUND' })
})

test('a hard close flushes with the DisconnectsClientError stand-in', async () => {
  const client = newClient()
  const outcome = client.set('k', 'v').then(
    () => 'resolved',
    (err: unknown) => err,
  )
  client.destroy()
  assertStandIn(await outcome, 'DisconnectsClientError', 'Disconnects client')
})

test('a closed client throws the ClientClosedError stand-in', async () => {
  const client = newClient()
  await client.quit()
  await assert.rejects(
    () => client.get('k'),
    (err: unknown) => {
      assertStandIn(err, 'ClientClosedError', 'The client is closed')
      return true
    },
  )
})

test('a watch-aborted exec throws the WatchError stand-in', async () => {
  const client = newClient()
  const other = await client.duplicate()
  try {
    await client.watch('w')
    await other.set('w', 'changed')
    await assert.rejects(
      () => client.multi().set('w', 'fromTxn').exec(),
      (err: unknown) => {
        assertStandIn(
          err,
          'WatchError',
          'One (or more) of the watched keys has been changed',
        )
        return true
      },
    )
  } finally {
    await other.quit()
    await client.quit()
  }
})

test('server errors are the SimpleError stand-in, a subclass of ErrorReply', async () => {
  const client = newClient()
  try {
    await client.set('s', 'notAnInteger')
    await assert.rejects(
      () => client.incr('s'),
      (err: unknown) => {
        assertStandIn(
          err,
          'SimpleError',
          'ERR value is not an integer or out of range',
        )
        const parent = Object.getPrototypeOf((err as Error).constructor)
        assert.strictEqual(parent.name, 'ErrorReply')
        return true
      },
    )
  } finally {
    await client.quit()
  }
})

test('a failed queued command throws the MultiErrorReply stand-in', async () => {
  const client = newClient()
  try {
    await client.set('s', 'notAnInteger')
    await client.set('t', 'notAnInteger')
    await assert.rejects(
      () => client.multi().incr('s').set('ok', 'v').incr('t').exec(),
      (err: unknown) => {
        assertStandIn(
          err,
          'MultiErrorReply',
          '2 commands failed, see .replies and .errorIndexes for more information',
        )
        const multiErr = err as Error & {
          replies: unknown[]
          errorIndexes: number[]
          errors(): IterableIterator<unknown>
        }
        assert.deepStrictEqual(multiErr.errorIndexes, [0, 2])
        assert.strictEqual(multiErr.replies[1], 'OK')
        const failed = [...multiErr.errors()]
        assert.deepStrictEqual(failed, [
          multiErr.replies[0],
          multiErr.replies[2],
        ])
        for (const item of failed) {
          assertStandIn(
            item,
            'SimpleError',
            'ERR value is not an integer or out of range',
          )
        }
        return true
      },
    )
  } finally {
    await client.quit()
  }
})
