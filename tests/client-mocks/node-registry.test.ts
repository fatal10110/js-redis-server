import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  RedisServerState,
  createRedisCommandExecutor,
  InMemoryNodeRegistry,
} from '../../src/internal'

function pipeline() {
  return {
    state: new RedisServerState(),
    executor: createRedisCommandExecutor(),
  }
}

describe('InMemoryNodeRegistry', () => {
  test('resolves a registered host:port to its pipeline', () => {
    const registry = new InMemoryNodeRegistry()
    const p = pipeline()
    registry.register('127.0.0.1', 7000, p)

    const resolved = registry.resolve('127.0.0.1', 7000)
    assert.strictEqual(resolved?.state, p.state)
    assert.strictEqual(resolved?.executor, p.executor)
  })

  test('returns undefined for an unknown address', () => {
    const registry = new InMemoryNodeRegistry()
    registry.register('127.0.0.1', 7000, pipeline())

    assert.strictEqual(registry.resolve('127.0.0.1', 7001), undefined)
    assert.strictEqual(registry.resolve('10.0.0.1', 7000), undefined)
  })

  test('nodes() lists every registered entry with its host/port', () => {
    const registry = new InMemoryNodeRegistry()
    registry.register('127.0.0.1', 7000, pipeline())
    registry.register('127.0.0.1', 7001, pipeline())

    const addresses = registry
      .nodes()
      .map(node => `${node.host}:${node.port}`)
      .sort()
    assert.deepStrictEqual(addresses, ['127.0.0.1:7000', '127.0.0.1:7001'])
  })

  test('re-registering an address overwrites the pipeline', () => {
    const registry = new InMemoryNodeRegistry()
    registry.register('127.0.0.1', 7000, pipeline())
    const replacement = pipeline()
    registry.register('127.0.0.1', 7000, replacement)

    assert.strictEqual(registry.nodes().length, 1)
    assert.strictEqual(
      registry.resolve('127.0.0.1', 7000)?.state,
      replacement.state,
    )
  })
})
