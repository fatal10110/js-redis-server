import { describe, test } from 'node:test'
import assert from 'node:assert'

import { Resp2CommandDecoder } from '../../src/core/transports/resp2/decoder'

/**
 * Memory retention of the RESP2 decoder (#564). None of it is visible on the
 * wire, so these read the decoder's private buffer directly.
 *
 * Real Redis bounds the same things: it shrinks an idle query buffer
 * (`clientsCronResizeQueryBuffer`, above `PROTO_RESIZE_THRESHOLD` = 32KB), and
 * reserves exactly `bulklen + 2` for a bulk of `PROTO_MBULK_BIG_ARG` (32KB) or
 * more (`sdsMakeRoomForNonGreedy`), then hands that buffer over as the
 * argument.
 */
const RETAINED = 32 * 1024
const CHUNK = 64 * 1024

function decoder(): Resp2CommandDecoder {
  return new Resp2CommandDecoder({ maxBulkLength: () => 512n * 1024n * 1024n })
}

function storageLength(d: Resp2CommandDecoder): number {
  return (d as unknown as { storage: Buffer }).storage.length
}

/** Push `bytes` in socket-sized chunks, collecting frames as they complete. */
function feed(
  d: Resp2CommandDecoder,
  bytes: Buffer,
  chunk = CHUNK,
  onPush?: () => void,
): Array<{ command: Buffer; args: Buffer[] }> {
  const frames = []
  for (let offset = 0; offset < bytes.length; offset += chunk) {
    d.push(bytes.subarray(offset, offset + chunk))
    onPush?.()
    for (let frame = d.next(); frame; frame = d.next()) {
      frames.push(frame)
    }
  }
  return frames
}

function bulkSet(key: string, value: Buffer): Buffer {
  return Buffer.concat([
    Buffer.from(`*3\r\n$3\r\nSET\r\n$${key.length}\r\n${key}\r\n`),
    Buffer.from(`$${value.length}\r\n`),
    value,
    Buffer.from('\r\n'),
  ])
}

describe('RESP2 decoder memory retention (#564)', () => {
  test('a big bulk followed by a partial command leaves a small buffer', () => {
    const value = Buffer.alloc(32 * 1024 * 1024, 0x61)
    const partial = Buffer.from('*1\r\n$3\r\nfo')
    const d = decoder()
    const frames = feed(d, Buffer.concat([bulkSet('k', value), partial]))

    assert.strictEqual(frames.length, 1)
    assert.ok(frames[0]!.args[1]!.equals(value))
    assert.ok(
      storageLength(d) <= RETAINED,
      `storage kept ${storageLength(d)} bytes for an 11-byte tail`,
    )

    d.push(Buffer.from('o\r\n'))
    assert.deepStrictEqual(d.next(), { command: Buffer.from('foo'), args: [] })
  })

  test('a big buffer pushed at once is compacted to its unread tail', () => {
    // One push holding the whole request plus the start of the next one: the
    // buffer is not a reservation, so this exercises the compaction itself.
    const value = Buffer.alloc(2 * 1024 * 1024, 0x62)
    const d = decoder()
    d.push(Buffer.concat([bulkSet('k', value), Buffer.from('*1\r\n$4\r\nPI')]))
    const frame = d.next()
    assert.ok(frame?.args[1]!.equals(value))
    assert.ok(storageLength(d) <= RETAINED, `kept ${storageLength(d)} bytes`)

    d.push(Buffer.from('NG\r\n'))
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
  })

  test('retained capacity is released once the decoder goes idle', () => {
    // About 170KB of small pipelined commands, then nothing left to parse.
    // The storage stays below 1MB, so only the 32KB idle cap can release it.
    const one = Buffer.from('*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n')
    const pipeline = Buffer.concat(new Array<Buffer>(5_000).fill(one))
    const d = decoder()
    d.push(pipeline)
    assert.ok(storageLength(d) > RETAINED)
    assert.ok(
      storageLength(d) < 1024 * 1024,
      `grew to ${storageLength(d)} bytes`,
    )
    let frames = 0
    while (d.next()) {
      frames += 1
    }
    assert.strictEqual(frames, 5_000)
    assert.ok(storageLength(d) <= RETAINED, `kept ${storageLength(d)} bytes`)
  })

  test('peak storage for one large bulk is about bulklen + 2 plus one chunk', () => {
    const value = Buffer.alloc(8 * 1024 * 1024, 0x63)
    const d = decoder()
    let peak = 0
    const frames = feed(
      d,
      Buffer.concat([bulkSet('k', value), Buffer.from('*1\r\n$4\r\nPING\r\n')]),
      CHUNK,
      () => {
        peak = Math.max(peak, storageLength(d))
      },
    )

    assert.strictEqual(frames.length, 2)
    assert.ok(frames[0]!.args[1]!.equals(value))
    assert.deepStrictEqual(frames[1], {
      command: Buffer.from('PING'),
      args: [],
    })
    assert.ok(
      peak <= value.length + 2 + CHUNK,
      `peak storage ${peak} for a ${value.length}-byte bulk`,
    )
  })

  test('a large bulk decodes the same however the stream is split', () => {
    const value = Buffer.alloc(40 * 1024)
    for (let i = 0; i < value.length; i++) {
      value[i] = i % 251
    }
    const stream = Buffer.concat([
      Buffer.from('*1\r\n$4\r\nPING\r\n'),
      bulkSet('big', value),
      bulkSet('k', Buffer.from('v')),
      Buffer.from('PING\r\n'),
    ])
    const expected = feed(decoder(), stream, stream.length)
    assert.strictEqual(expected.length, 4)
    assert.ok(expected[1]!.args[1]!.equals(value))

    for (const chunk of [1000, 4096, 16 * 1024, value.length + 1, CHUNK]) {
      assert.deepStrictEqual(
        feed(decoder(), stream, chunk),
        expected,
        `chunk ${chunk}`,
      )
    }
    // Cuts around the payload's header, its end, its terminator and the next
    // command.
    const payloadStart = stream.indexOf(value)
    const payloadEnd = payloadStart + value.length
    const cuts = [payloadStart - 8, payloadStart, payloadEnd]
    for (const around of cuts) {
      for (let cut = around - 4; cut <= around + 6; cut++) {
        const d = decoder()
        const frames = feed(d, stream.subarray(0, cut), stream.length)
        frames.push(...feed(d, stream.subarray(cut), stream.length))
        assert.deepStrictEqual(frames, expected, `split at byte ${cut}`)
      }
    }
  })

  test('an inline argument does not keep the whole request line alive', () => {
    const value = 'x'.repeat(60 * 1024)
    const d = decoder()
    d.push(Buffer.from(`SET k ${value}\r\n`))
    const frame = d.next()
    assert.deepStrictEqual(frame?.args[0], Buffer.from('k'))
    assert.ok(
      frame.args[0]!.buffer.byteLength < 60 * 1024,
      `a 1-byte argument holds a ${frame.args[0]!.buffer.byteLength}-byte buffer`,
    )
    assert.strictEqual(frame.args[1]!.toString(), value)
  })
})
