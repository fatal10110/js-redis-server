import { test, describe } from 'node:test'
import assert from 'node:assert'
import { MockTransport, createMockTransport } from './mock-transport'

describe('MockTransport', () => {
  test('should track write calls', () => {
    const transport = new MockTransport()

    transport.write('hello')
    transport.write('world', true)

    assert.strictEqual(transport.getCallCount(), 2)

    const calls = transport.getCalls()
    assert.strictEqual(calls[0].responseData, 'hello')
    assert.strictEqual(calls[0].close, undefined)
    assert.strictEqual(calls[1].responseData, 'world')
    assert.strictEqual(calls[1].close, true)
  })

  test('should track last call and response', () => {
    const transport = new MockTransport()

    transport.write('first')
    transport.write('last')

    const lastCall = transport.getLastCall()
    assert.strictEqual(lastCall?.responseData, 'last')
    assert.strictEqual(transport.getLastResponse(), 'last')
  })

  test('should track all response data', () => {
    const transport = new MockTransport()

    transport.write('a')
    transport.write('b')
    transport.write('c')

    const responses = transport.getResponseData()
    assert.deepStrictEqual(responses, ['a', 'b', 'c'])
  })

  test('should track close state', () => {
    const transport = new MockTransport()

    assert.strictEqual(transport.wasCloseCalled(), false)
    assert.strictEqual(transport.isClosed(), false)

    transport.write('data')
    assert.strictEqual(transport.wasCloseCalled(), false)
    assert.strictEqual(transport.isClosed(), false)

    transport.write('data', true)
    assert.strictEqual(transport.wasCloseCalled(), true)
    assert.strictEqual(transport.isClosed(), true)
  })

  test('should reset state', () => {
    const transport = new MockTransport()

    transport.write('data', true)
    assert.strictEqual(transport.getCallCount(), 1)
    assert.strictEqual(transport.isClosed(), true)

    transport.reset()
    assert.strictEqual(transport.getCallCount(), 0)
    assert.strictEqual(transport.isClosed(), false)
    assert.strictEqual(transport.wasCloseCalled(), false)
  })

  test('assertion helpers should work correctly', () => {
    const transport = new MockTransport()

    transport.write('test')
    transport.assertCallCount(1)
    transport.assertLastResponse('test')
    transport.assertCloseNotCalled()

    transport.write('final', true)
    transport.assertCallCount(2)
    transport.assertLastResponse('final')
    transport.assertCloseCalled()
  })

  test('assertion helpers should throw on mismatch', () => {
    const transport = new MockTransport()

    transport.write('test')

    assert.throws(
      () => transport.assertCallCount(2),
      /Expected 2 calls, but got 1/,
    )
    assert.throws(
      () => transport.assertLastResponse('wrong'),
      /Expected last response to be wrong, but got test/,
    )
    assert.throws(
      () => transport.assertCloseCalled(),
      /Expected close to be called, but it was not/,
    )

    transport.write('data', true)
    assert.throws(
      () => transport.assertCloseNotCalled(),
      /Expected close not to be called, but it was/,
    )
  })

  test('factory function should create new instance', () => {
    const transport1 = createMockTransport()
    const transport2 = createMockTransport()

    transport1.write('test1')
    transport2.write('test2')

    assert.strictEqual(transport1.getCallCount(), 1)
    assert.strictEqual(transport2.getCallCount(), 1)
    assert.strictEqual(transport1.getLastResponse(), 'test1')
    assert.strictEqual(transport2.getLastResponse(), 'test2')
  })

  test('should track timestamps', () => {
    const transport = new MockTransport()
    const before = Date.now()

    transport.write('test')

    const after = Date.now()
    const call = transport.getLastCall()

    assert.ok(call !== undefined)
    assert.ok(call.timestamp >= before)
    assert.ok(call.timestamp <= after)
  })
})
