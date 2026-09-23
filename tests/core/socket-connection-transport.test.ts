import { describe, test } from 'node:test'
import assert from 'node:assert'
import { duplexPair, type Duplex } from 'node:stream'
import { SocketConnectionTransport } from '../../src/core/transports/socket-connection-transport'

/** Bigger than the default 16 KiB high-water mark, so the write parks. */
const OVERSIZED = Buffer.alloc(64 * 1024, 1)

function pair(): { peer: Duplex; stream: Duplex } {
  const [peer, stream] = duplexPair()
  return { peer, stream }
}

async function settlesWithin(
  promise: Promise<unknown>,
  timeoutMs = 500,
): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      promise.then(
        () => true,
        () => true,
      ),
      new Promise<false>(resolve => {
        timer = setTimeout(() => resolve(false), timeoutMs)
      }),
    ])
  } finally {
    if (timer) {
      clearTimeout(timer)
    }
  }
}

describe('SocketConnectionTransport over a duplexPair', () => {
  test('a write parked by a non-draining peer settles on close()', async () => {
    const { stream } = pair()
    const transport = new SocketConnectionTransport(stream)

    // Nothing reads `peer`, so duplexPair parks the write callback. Unlike
    // net.Socket (which fails it with ECANCELED on destroy), that callback
    // would otherwise stay pending forever and stall the adapter's write chain.
    const parked = transport.write(OVERSIZED)
    assert.strictEqual(await settlesWithin(parked, 50), false)

    transport.close('test')
    assert.strictEqual(await settlesWithin(parked), true)
  })

  test('a write parked by a non-draining peer settles when the stream is destroyed', async () => {
    const { stream } = pair()
    const transport = new SocketConnectionTransport(stream)

    const parked = transport.write(OVERSIZED)
    assert.strictEqual(await settlesWithin(parked, 50), false)

    stream.destroy()
    assert.strictEqual(await settlesWithin(parked), true)
  })

  test('close() half-closes first, so the peer sees end before close', async () => {
    const { peer, stream } = pair()
    const transport = new SocketConnectionTransport(stream)

    const events: string[] = []
    peer.on('data', chunk => events.push(`data:${String(chunk)}`))
    peer.on('end', () => events.push('end'))

    const ended = new Promise(resolve => peer.on('end', resolve))
    const closed = new Promise(resolve => stream.on('close', resolve))

    await transport.write(Buffer.from('+OK\r\n'))
    transport.close('test')
    await ended
    await closed

    // Real Redis answers QUIT and then sends FIN: the peer sees the reply and
    // then 'end'. A bare destroy() would skip the 'end' entirely.
    assert.deepStrictEqual(events, ['data:+OK\r\n', 'end'])
    // …and the half-close is followed by a destroy once the writable flushed.
    assert.strictEqual(stream.destroyed, true)
  })

  test('close() with a write still in flight still delivers that write, then end', async () => {
    const { peer, stream } = pair()
    const transport = new SocketConnectionTransport(stream)

    const events: string[] = []
    peer.on('data', chunk => events.push(`data:${String(chunk)}`))
    peer.on('end', () => events.push('end'))
    const closed = new Promise(resolve => stream.on('close', resolve))

    // Not awaited: the write is still in flight when close() runs, so end()
    // defers `_final` (which hands the peer its EOF) until the write completes
    // a tick later. Destroying synchronously or on a microtask drops the 'end'.
    const inFlight = transport.write(Buffer.from('+OK\r\n'))
    transport.close('test')

    await inFlight
    await closed
    await new Promise(resolve => setImmediate(resolve))

    assert.deepStrictEqual(events, ['data:+OK\r\n', 'end'])
  })

  test('in-flight writes do not add a listener each', async () => {
    const { stream } = pair()
    const transport = new SocketConnectionTransport(stream)
    const before = {
      close: stream.listenerCount('close'),
      error: stream.listenerCount('error'),
    }

    // Nothing reads the peer, so every one of these stays parked. One listener
    // per write would trip MaxListenersExceededWarning past ten.
    const writes = Array.from({ length: 25 }, () => transport.write(OVERSIZED))

    assert.strictEqual(stream.listenerCount('close'), before.close)
    assert.strictEqual(stream.listenerCount('error'), before.error)

    transport.close('test')
    assert.strictEqual(await settlesWithin(Promise.all(writes)), true)
  })

  test('read() ends quietly when the stream is destroyed mid-iteration', async () => {
    const { peer, stream } = pair()
    const transport = new SocketConnectionTransport(stream)

    const chunks: Buffer[] = []
    const loop = (async () => {
      for await (const chunk of transport.read()) {
        chunks.push(chunk)
      }
    })()

    peer.write('hello')
    await new Promise(resolve => setTimeout(resolve, 10))
    stream.destroy()

    await loop
    assert.deepStrictEqual(chunks.map(String), ['hello'])
  })

  test('read() rethrows a genuine stream failure rather than ending as a clean EOF', async () => {
    const { stream } = pair()
    const transport = new SocketConnectionTransport(stream)

    const loop = (async () => {
      for await (const _chunk of transport.read()) {
        // drain
      }
    })()

    stream.destroy(new Error('mid-stream failure'))

    // Only ERR_STREAM_PREMATURE_CLOSE is swallowed. Anything else rethrows, so
    // the adapter sees a failure rather than a clean EOF. (It is not logged:
    // the stream's 'error' has already aborted the transport by then.)
    await assert.rejects(loop, /mid-stream failure/)
  })
})
