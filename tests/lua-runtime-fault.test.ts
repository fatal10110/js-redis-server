import { describe, test } from 'node:test'
import assert from 'node:assert'
import { WasmFault } from 'lua-redis-wasm'
import {
  RedisResult,
  RedisValue,
  setLuaWasmLoadOptions,
  type RedisLuaRuntime,
} from '../src/internal'
import { createRedisSessionHarness as createSession } from './core-session-test-helpers'

// #539: since lua-redis-wasm 2.0 an exception that escapes the WASM module
// leaves the engine unusable, and every later call throws `LuaEngine is
// unusable: ...`. The server must drop that runtime and create a fresh one
// on the next script command, keeping its script cache and functions.

/** The engine a runtime wraps (private to RedisLuaRuntime). */
function engineOf(runtime: RedisLuaRuntime): Record<string, unknown> {
  return (runtime as unknown as { engine: Record<string, unknown> }).engine
}

const evalArgs = (script: string) => [Buffer.from(script), Buffer.from('0')]

function errorMessage(result: unknown): string {
  assert.ok(result instanceof RedisResult)
  assert.strictEqual(result.value.kind, 'error')
  assert.strictEqual(result.value.code, 'ERR')
  return result.value.message
}

describe('Lua runtime fault recovery (#539)', () => {
  test('a WasmFault from evalWithArgs is an ERR reply, and the next EVAL gets a fresh runtime', async () => {
    const { session, server } = createSession()
    const sha = server.scriptCache.load(Buffer.from('return 7'))
    const faulty = await server.getLuaRuntime()
    engineOf(faulty).evalWithArgs = () => {
      throw new WasmFault('injected fault')
    }

    assert.strictEqual(
      errorMessage(await session.execute('eval', evalArgs('return 1'))),
      'injected fault',
    )
    assert.strictEqual(faulty.usable, false)

    assert.deepStrictEqual(
      await session.execute('eval', evalArgs('return 2')),
      RedisResult.create(RedisValue.integer(2)),
    )
    const fresh = await server.getLuaRuntime()
    assert.notStrictEqual(fresh, faulty)
    assert.strictEqual(fresh.usable, true)

    // The script cache lives on the server, so it survives the swap.
    assert.deepStrictEqual(
      await session.execute('evalsha', [Buffer.from(sha), Buffer.from('0')]),
      RedisResult.create(RedisValue.integer(7)),
    )
  })

  test('a trap inside the WASM module leaves the engine unusable, and the server replaces it', async () => {
    const { session, server } = createSession()
    const faulty = await server.getLuaRuntime()
    // Make the module itself trap mid-evaluation: the engine marks itself
    // unusable and rethrows the trap as is (not as a WasmFault).
    const instance = (engineOf(faulty) as { instance: Record<string, unknown> })
      .instance
    instance._eval_with_args = () => {
      throw new WebAssembly.RuntimeError('unreachable')
    }

    assert.strictEqual(
      errorMessage(await session.execute('eval', evalArgs('return 1'))),
      'unreachable',
    )
    assert.strictEqual(faulty.usable, false)
    // The engine itself now refuses every call.
    assert.throws(
      () => faulty.compile(Buffer.from('return 1')),
      /LuaEngine is (unusable|disposed)/,
    )

    assert.deepStrictEqual(
      await session.execute('eval', evalArgs('return 3')),
      RedisResult.create(RedisValue.integer(3)),
    )
    assert.notStrictEqual(await server.getLuaRuntime(), faulty)
  })

  test('SCRIPT LOAD and FUNCTION LOAD recover the same way', async () => {
    const { session, server } = createSession()
    const library = Buffer.from(
      "#!lua name=faultlib\nredis.register_function('ff', function() return 'ok' end)",
    )
    assert.deepStrictEqual(
      await session.execute('function', [Buffer.from('load'), library]),
      RedisResult.create(RedisValue.bulkString(Buffer.from('faultlib'))),
    )

    const faulty = await server.getLuaRuntime()
    engineOf(faulty).compile = () => {
      throw new WasmFault('compile fault')
    }
    assert.strictEqual(
      errorMessage(
        await session.execute('script', [
          Buffer.from('load'),
          Buffer.from('return 1'),
        ]),
      ),
      'compile fault',
    )

    // A fresh runtime compiles it, and the loaded library still runs.
    assert.deepStrictEqual(
      await session.execute('script', [
        Buffer.from('load'),
        Buffer.from('return 1'),
      ]),
      RedisResult.create(
        RedisValue.bulkString(
          Buffer.from('e0e1f9fabfc9d4800c877a703b823ac0578ff8db'),
        ),
      ),
    )
    assert.deepStrictEqual(
      await session.execute('fcall', [Buffer.from('ff'), Buffer.from('0')]),
      RedisResult.create(RedisValue.bulkString(Buffer.from('ok'))),
    )

    const second = await server.getLuaRuntime()
    engineOf(second).compile = () => {
      throw new WasmFault('compile fault')
    }
    assert.strictEqual(
      errorMessage(
        await session.execute('function', [
          Buffer.from('load'),
          Buffer.from('REPLACE'),
          library,
        ]),
      ),
      'compile fault',
    )
    assert.notStrictEqual(await server.getLuaRuntime(), second)
  })

  test('an error that leaves the engine usable keeps the runtime', async () => {
    const { session, server } = createSession()
    const runtime = await server.getLuaRuntime()
    const original = engineOf(runtime).evalWithArgs
    engineOf(runtime).evalWithArgs = () => {
      // The engine's own heap-size error: it stays usable.
      throw new RangeError('script too large for the WASM heap')
    }

    assert.strictEqual(
      errorMessage(await session.execute('eval', evalArgs('return 1'))),
      'script too large for the WASM heap',
    )
    assert.strictEqual(runtime.usable, true)
    engineOf(runtime).evalWithArgs = original
    assert.deepStrictEqual(
      await session.execute('eval', evalArgs('return 4')),
      RedisResult.create(RedisValue.integer(4)),
    )
    assert.strictEqual(await server.getLuaRuntime(), runtime)
  })

  test('the re-entrancy guard is not a fault', async () => {
    const { server } = createSession()
    const runtime = await server.getLuaRuntime()
    const script = Buffer.from('return 1')
    // Simulate a script already running on this runtime.
    const hostState = (runtime as unknown as { hostState: { ctx: unknown } })
      .hostState
    hostState.ctx = {}
    try {
      assert.throws(
        () => runtime.eval(script, [], [], {} as never),
        /Lua runtime is already executing a script/,
      )
    } finally {
      hostState.ctx = null
    }
    assert.strictEqual(runtime.usable, true)
    assert.strictEqual(await server.getLuaRuntime(), runtime)
  })

  test('a failed runtime creation is retried on the next call', async () => {
    const { session, server } = createSession()
    setLuaWasmLoadOptions({ wasmPath: '/nonexistent/lua-redis.wasm' })
    try {
      await assert.rejects(server.getLuaRuntime())
    } finally {
      setLuaWasmLoadOptions({})
    }
    assert.deepStrictEqual(
      await session.execute('eval', evalArgs('return 6')),
      RedisResult.create(RedisValue.integer(6)),
    )
  })
})
