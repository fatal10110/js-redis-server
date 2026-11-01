import { test, afterEach } from 'node:test'
import assert from 'node:assert'
import {
  createRedisLuaRuntime,
  getLuaWasmLoadOptions,
  setLuaWasmLoadOptions,
} from '../src/core/lua-runtime'

/**
 * Loading the Lua engine awaits WebAssembly compilation and instantiation,
 * which V8 runs on its own worker threads: nothing in the event loop waits for
 * them. A caller with no socket open (the in-memory client, the node-redis
 * mock) then leaves the loop empty, and Node drains its platform tasks with a
 * blocking wait that can deadlock (nodejs/node#54918). The main thread stalls
 * in a futex for good: #541 and the node-redis mock stall of #454, both on the
 * first EVAL of an engine. A referenced timer keeps the loop alive until the
 * engine settles, so Node never enters that drain.
 */
function timeouts(): number {
  return process.getActiveResourcesInfo().filter(name => name === 'Timeout')
    .length
}

const defaultOptions = getLuaWasmLoadOptions()

afterEach(() => {
  setLuaWasmLoadOptions(defaultOptions)
})

test('a referenced timer holds the event loop while the engine loads', async () => {
  const before = timeouts()
  const loading = createRedisLuaRuntime()
  assert.strictEqual(timeouts(), before + 1)

  const runtime = await loading
  try {
    assert.strictEqual(timeouts(), before)
  } finally {
    runtime.dispose()
  }
})

test('the timer is released when the engine fails to load', async () => {
  setLuaWasmLoadOptions({ wasmPath: '/nonexistent/redis_lua.wasm' })
  const before = timeouts()
  const loading = createRedisLuaRuntime()
  assert.strictEqual(timeouts(), before + 1)

  await assert.rejects(loading)
  assert.strictEqual(timeouts(), before)
})
