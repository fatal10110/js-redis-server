import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  RedisLuaRuntime,
  RedisServerState,
  RedisResult,
  RedisValue,
} from '../src'
import { createRedisSessionHarness as createSession } from './core-session-test-helpers'

// Issue #130: RedisLuaRuntime used to be a process-wide module-level singleton
// (`getDefaultRedisLuaRuntime`), so every EVAL/EVALSHA across logically
// independent server/cluster-node instances shared one LuaEngine + hostState.
// The runtime must instead be scoped per RedisServerState so concurrent scripts
// on separate nodes can never collide on the single-script re-entrancy guard.
describe('RedisLuaRuntime per-server isolation (#130)', () => {
  test('each RedisServerState owns a distinct Lua runtime, not a shared singleton', async () => {
    const serverA = new RedisServerState()
    const serverB = new RedisServerState()

    const runtimeA = await serverA.getLuaRuntime()
    const runtimeB = await serverB.getLuaRuntime()

    assert.ok(runtimeA instanceof RedisLuaRuntime)
    assert.ok(runtimeB instanceof RedisLuaRuntime)
    assert.notStrictEqual(
      runtimeA,
      runtimeB,
      'separate server instances must not share one Lua runtime',
    )
  })

  test('the same RedisServerState memoizes its Lua runtime', async () => {
    const server = new RedisServerState()

    const first = await server.getLuaRuntime()
    const second = await server.getLuaRuntime()

    assert.strictEqual(first, second)
  })

  test('EVAL on two independent servers runs through each server own runtime', async () => {
    const a = createSession()
    const b = createSession()
    const key = Buffer.from('shared-key-name')

    // Same key name on two independent servers must stay independent — each
    // server resolves its own runtime via ctx.server.getLuaRuntime().
    const [resA, resB] = await Promise.all([
      a.session.execute('eval', [
        Buffer.from('return redis.call("set", KEYS[1], ARGV[1])'),
        Buffer.from('1'),
        key,
        Buffer.from('from-A'),
      ]),
      b.session.execute('eval', [
        Buffer.from('return redis.call("set", KEYS[1], ARGV[1])'),
        Buffer.from('1'),
        key,
        Buffer.from('from-B'),
      ]),
    ])

    assert.deepStrictEqual(resA, RedisResult.ok())
    assert.deepStrictEqual(resB, RedisResult.ok())
    assert.deepStrictEqual(
      a.server.getDatabase(0).getString(key),
      Buffer.from('from-A'),
    )
    assert.deepStrictEqual(
      b.server.getDatabase(0).getString(key),
      Buffer.from('from-B'),
    )

    assert.deepStrictEqual(
      await a.session.execute('eval', [
        Buffer.from('return redis.call("get", KEYS[1])'),
        Buffer.from('1'),
        key,
      ]),
      RedisResult.create(RedisValue.bulkString(Buffer.from('from-A'))),
    )
  })
})
