import { describe, test } from 'node:test'
import assert from 'node:assert'

import { resolveCompatibilityProfile } from '../../src/core/compatibility'
import {
  Resp2CommandDecoder,
  Resp2ParseError,
} from '../../src/core/transports/resp2/decoder'
import type { CompatibilitySpec } from '../../src/core/compatibility'

/**
 * Decoder-level pins for #441 that the raw-tcp suite cannot assert
 * deterministically: "not refused yet" is only observable as `next()`
 * returning `null`, never as bytes on a socket.
 */
function decoder(spec?: CompatibilitySpec): Resp2CommandDecoder {
  return new Resp2CommandDecoder({
    maxBulkLength: () => 512n * 1024n * 1024n,
    profile: spec === undefined ? undefined : resolveCompatibilityProfile(spec),
  })
}

function assertProtocolError(d: Resp2CommandDecoder, message: string): void {
  assert.throws(
    () => d.next(),
    (err: unknown) => err instanceof Resp2ParseError && err.message === message,
  )
}

const INVALID_MULTIBULK = 'Protocol error: invalid multibulk length'
const TOO_BIG_INLINE = 'Protocol error: too big inline request'

describe('Resp2CommandDecoder multibulk count bound', () => {
  test('6.2 refuses a count above 1024*1024', () => {
    const accepted = decoder('redis-6.2')
    accepted.push(Buffer.from('*1048576\r\n'))
    assert.strictEqual(accepted.next(), null)

    const refused = decoder('redis-6.2')
    refused.push(Buffer.from('*1048577\r\n'))
    assertProtocolError(refused, INVALID_MULTIBULK)
  })

  for (const spec of [
    'redis-7.0',
    'redis-8.0',
    { flavor: 'valkey', version: '7.2.0' },
    undefined,
  ] as const) {
    test(`${JSON.stringify(spec) ?? 'no profile'} accepts up to INT_MAX`, () => {
      const accepted = decoder(spec)
      accepted.push(Buffer.from('*2147483647\r\n'))
      assert.strictEqual(accepted.next(), null)

      const refused = decoder(spec)
      refused.push(Buffer.from('*2147483648\r\n'))
      assertProtocolError(refused, INVALID_MULTIBULK)
    })
  }

  test('a negative count is skipped, not refused', () => {
    const d = decoder()
    d.push(Buffer.from('*-5\r\n*1\r\n$4\r\nPING\r\n'))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('PING'),
      args: [],
    })
  })
})

describe('Resp2CommandDecoder length format (string2ll)', () => {
  for (const count of ['-05', '-0', '01', '00', '+1', ' 1', '1 ', '']) {
    test(`multibulk count ${JSON.stringify(count)} is refused`, () => {
      const d = decoder()
      d.push(Buffer.from(`*${count}\r\n*1\r\n$4\r\nPING\r\n`))
      assertProtocolError(d, INVALID_MULTIBULK)
    })
  }

  for (const length of ['01', '-0', '04', '+4', '-1']) {
    test(`bulk length ${JSON.stringify(length)} is refused`, () => {
      const d = decoder()
      d.push(Buffer.from(`*1\r\n$${length}\r\nPING\r\n`))
      assertProtocolError(d, 'Protocol error: invalid bulk length')
    })
  }

  test('a count outside int64 is refused; one inside it is skipped', () => {
    const outside = decoder()
    outside.push(Buffer.from('*-9223372036854775809\r\n'))
    assertProtocolError(outside, INVALID_MULTIBULK)

    const inside = decoder()
    inside.push(Buffer.from('*-9223372036854775808\r\n*1\r\n$4\r\nPING\r\n'))
    assert.deepStrictEqual(inside.next(), {
      command: Buffer.from('PING'),
      args: [],
    })
  })
})

describe('Resp2CommandDecoder bulk terminator', () => {
  test('the two bytes after a payload are skipped unchecked', () => {
    const d = decoder()
    d.push(Buffer.from('*2\r\n$4\r\nECHOXX$2\r\nhi\n\n'))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('ECHO'),
      args: [Buffer.from('hi')],
    })
  })

  test('still waits for both skipped bytes before dispatching', () => {
    const d = decoder()
    d.push(Buffer.from('*1\r\n$4\r\nPINGX'))
    assert.strictEqual(d.next(), null)
    d.push(Buffer.from('X'))
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
  })
})

describe('Resp2CommandDecoder inline cap', () => {
  test('exactly 64KB with no newline waits; one byte more is refused', () => {
    const d = decoder()
    d.push(Buffer.alloc(64 * 1024, 0x61))
    assert.strictEqual(d.next(), null)

    d.push(Buffer.from('a'))
    assertProtocolError(d, TOO_BIG_INLINE)
  })

  test('the cap counts from the start of the inline request', () => {
    const d = decoder()
    d.push(Buffer.from('PING\r\n'))
    d.push(Buffer.alloc(64 * 1024, 0x61))
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
    assert.strictEqual(d.next(), null)
  })

  test('a line over 64KB is served when its newline is already buffered', () => {
    const d = decoder()
    const value = Buffer.alloc(70000, 0x61)
    d.push(Buffer.concat([Buffer.from('ECHO '), value, Buffer.from('\r\n')]))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('ECHO'),
      args: [value],
    })
  })

  test('a bare LF ends an inline request', () => {
    const d = decoder()
    d.push(Buffer.from('ECHO hi\nPING\r\n'))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('ECHO'),
      args: [Buffer.from('hi')],
    })
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
  })
})

/** Decode one inline line on `spec`, returning its arguments as latin1 text. */
function inlineArgs(line: string, spec?: CompatibilitySpec): string[] | null {
  const d = decoder(spec)
  d.push(Buffer.from(`${line}\r\n`, 'latin1'))
  const frame = d.next()
  return frame && [frame.command, ...frame.args].map(b => b.toString('latin1'))
}

// #505 item 1. Expected splits verified byte-for-byte on redis-server 7.0.15
// (`ECHO <line>` over a raw socket); `sdssplitargs` is unchanged in sds.c from
// 6.2 through 8.0 and in Valkey before 9.0.
describe('Resp2CommandDecoder inline argument splitting (sdssplitargs)', () => {
  const cases: [string, string[]][] = [
    ['ECHO a\rb', ['ECHO', 'a', 'b']],
    // An unquoted argument ends only at space, \t, \n or \r...
    ['ECHO a\vb', ['ECHO', 'a\vb']],
    ['ECHO a\fb', ['ECHO', 'a\fb']],
    ['ECHO\fa', ['ECHO\fa']],
    // ...but leading blanks are skipped with isspace, \v and \f included.
    ['ECHO \va', ['ECHO', 'a']],
    ['\v\f PING \t', ['PING']],
    // A closing quote may be followed by any isspace byte.
    ['ECHO "a"\vb', ['ECHO', 'a', 'b']],
    ['ECHO "a"\f', ['ECHO', 'a']],
    // A quote opens a quoted section anywhere in an argument.
    ['ECHO foo"bar baz"', ['ECHO', 'foobar baz']],
    ["ECHO foo'bar baz'", ['ECHO', 'foobar baz']],
    // Escapes: all of them inside double quotes, only \' inside single ones.
    ['ECHO "\\x41\\x4"', ['ECHO', 'Ax4']],
    ['ECHO "\\n\\q\\\\"', ['ECHO', '\nq\\']],
    ["ECHO 'a\\x41'", ['ECHO', 'a\\x41']],
    ["ECHO 'a\\n'", ['ECHO', 'a\\n']],
    ["ECHO 'it\\'s'", ['ECHO', "it's"]],
    // Bytes are bytes: nothing is decoded as UTF-8.
    ['ECHO \xe9\xff', ['ECHO', '\xe9\xff']],
    ['ECHO "\xe9"', ['ECHO', '\xe9']],
    ['ECHO ""', ['ECHO', '']],
    ['"" a', ['', 'a']],
    // A CR inside the line ends an argument; only the one before LF is eaten.
    ['ECHO a\r', ['ECHO', 'a']],
  ]

  for (const [line, expected] of cases) {
    test(`${JSON.stringify(line)} splits as ${JSON.stringify(expected)}`, () => {
      assert.deepStrictEqual(inlineArgs(line), expected)
    })
  }

  for (const line of ['ECHO "a"b', "ECHO 'a'b", 'ECHO "abc', 'ECHO "a\\']) {
    test(`${JSON.stringify(line)} is unbalanced`, () => {
      const d = decoder()
      d.push(Buffer.from(`${line}\r\n`))
      assertProtocolError(d, 'Protocol error: unbalanced quotes in request')
    })
  }

  test('a line of blanks alone is skipped', () => {
    const d = decoder()
    d.push(Buffer.from('  \t \r\n\v\f\r\nPING\r\n'))
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
  })

  // Valkey 9.0's `sdsparsearg`: a closing quote ends the quoted section, not
  // the argument (sds.c, valkey 9.0.0; 8.0.0 and 8.1.0 still refuse).
  test('Valkey 9.0 joins an argument across a closing quote', () => {
    assert.deepStrictEqual(inlineArgs('ECHO "a"b', 'valkey-9.0'), [
      'ECHO',
      'ab',
    ])
    assert.deepStrictEqual(inlineArgs("ECHO 'a'b", 'valkey-9.0'), [
      'ECHO',
      'ab',
    ])
    assert.deepStrictEqual(inlineArgs('ECHO "a"\vb', 'valkey-9.0'), [
      'ECHO',
      'a\vb',
    ])
    assert.deepStrictEqual(inlineArgs('ECHO "a" b', 'valkey-9.0'), [
      'ECHO',
      'a',
      'b',
    ])

    const d = decoder('valkey-9.0')
    d.push(Buffer.from('ECHO "abc\r\n'))
    assertProtocolError(d, 'Protocol error: unbalanced quotes in request')
  })

  for (const spec of ['redis-6.2', 'redis-8.0', 'valkey-8.0'] as const) {
    test(`${spec} refuses an argument continuing past a closing quote`, () => {
      const d = decoder(spec)
      d.push(Buffer.from('ECHO "a"b\r\n'))
      assertProtocolError(d, 'Protocol error: unbalanced quotes in request')
    })
  }
})

// #505 item 2. Verified on redis-server 7.0.15; the same checks are in
// `processMultibulkBuffer` on 6.2 through 8.0 and Valkey's `parseMultibulk`.
describe('Resp2CommandDecoder header lines', () => {
  const TOO_BIG_MBULK = 'Protocol error: too big mbulk count string'
  const TOO_BIG_BULK = 'Protocol error: too big bulk count string'

  test('a count line with no CR waits up to 64KB, then is refused', () => {
    const d = decoder()
    d.push(Buffer.from(`*${'1'.repeat(64 * 1024 - 1)}`))
    assert.strictEqual(d.next(), null)
    d.push(Buffer.from('1'))
    assertProtocolError(d, TOO_BIG_MBULK)
  })

  test('a bulk length line with no CR waits up to 64KB, then is refused', () => {
    const d = decoder()
    d.push(Buffer.from(`*2\r\n$4\r\nECHO\r\n$${'1'.repeat(64 * 1024 - 1)}`))
    assert.strictEqual(d.next(), null)
    d.push(Buffer.from('1'))
    assertProtocolError(d, TOO_BIG_BULK)
  })

  test('the cap applies before the element prefix is checked', () => {
    const d = decoder()
    d.push(Buffer.from(`*1\r\n${'X'.repeat(70000)}`))
    assertProtocolError(d, TOO_BIG_BULK)
  })

  test('a 64KB header line is judged on its value once the CR arrives', () => {
    const count = decoder()
    count.push(Buffer.from(`*${'1'.repeat(64 * 1024 - 1)}`))
    assert.strictEqual(count.next(), null)
    count.push(Buffer.from('\r\n'))
    assertProtocolError(count, INVALID_MULTIBULK)
  })

  test('the element prefix is only checked once its line is complete', () => {
    const d = decoder()
    d.push(Buffer.from('*1\r\nX'))
    assert.strictEqual(d.next(), null)
    d.push(Buffer.from('\r'))
    assert.strictEqual(d.next(), null)
    d.push(Buffer.from('\n'))
    assertProtocolError(d, "Protocol error: expected '$', got 'X'")
  })

  test('a header line ends at the first CR; the next byte is skipped unchecked', () => {
    const d = decoder()
    d.push(Buffer.from('*1\rX$4\rYPING\r\n'))
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
  })

  test('a CR alone does not end a header line until one more byte arrives', () => {
    const d = decoder()
    d.push(Buffer.from('*1\r'))
    assert.strictEqual(d.next(), null)
    d.push(Buffer.from('\n$4\r'))
    assert.strictEqual(d.next(), null)
    d.push(Buffer.from('\nPING\r\n'))
    assert.deepStrictEqual(d.next(), { command: Buffer.from('PING'), args: [] })
  })

  // Redis searches its NUL-terminated query buffer with strchr, so a NUL ahead
  // of the line end hides it: the request waits, and trips the 64KB cap.
  test('a NUL before the CR hides the header line end on Redis', () => {
    const count = decoder('redis-8.0')
    count.push(Buffer.from('*1\0\r\n'))
    assert.strictEqual(count.next(), null)
    count.push(Buffer.alloc(70000, 0x61))
    assertProtocolError(count, TOO_BIG_MBULK)

    const bulk = decoder('redis-6.2')
    bulk.push(Buffer.from('*1\r\n$4\0\r\nPING\r\n'))
    assert.strictEqual(bulk.next(), null)
    bulk.push(Buffer.alloc(70000, 0x61))
    assertProtocolError(bulk, TOO_BIG_BULK)
  })

  // Valkey 8.1+ uses memchr over the buffered length instead.
  test('a NUL in a header line is just an invalid length on Valkey 9.0', () => {
    const count = decoder('valkey-9.0')
    count.push(Buffer.from('*1\0\r\n'))
    assertProtocolError(count, INVALID_MULTIBULK)

    const bulk = decoder('valkey-9.0')
    bulk.push(Buffer.from('*1\r\n$4\0\r\nPING\r\n'))
    assertProtocolError(bulk, 'Protocol error: invalid bulk length')

    const valkey80 = decoder('valkey-8.0')
    valkey80.push(Buffer.from('*1\0\r\n'))
    assert.strictEqual(valkey80.next(), null)
  })

  test('a NUL before the newline hides an inline request end on every version', () => {
    for (const spec of ['redis-7.0', 'valkey-9.0'] as const) {
      const d = decoder(spec)
      d.push(Buffer.from('PING\0\r\n'))
      assert.strictEqual(d.next(), null)
      d.push(Buffer.alloc(70000, 0x61))
      assertProtocolError(d, TOO_BIG_INLINE)
    }
  })

  test('a NUL inside a bulk payload is plain data', () => {
    const d = decoder()
    d.push(Buffer.from('*2\r\n$4\r\nECHO\r\n$3\r\na\0b\r\n'))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('ECHO'),
      args: [Buffer.from('a\0b')],
    })
  })
})

// #505 item 3.
describe('Resp2CommandDecoder incremental parsing', () => {
  const stream = Buffer.from(
    'PING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n\r\n*0\r\n*-1\r\n' +
      ' ECHO "x y" \xe9\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\na\0b\r\n' +
      '*1\rX$4\r\nPING\r\n',
    'latin1',
  )

  function drain(d: Resp2CommandDecoder): string[] {
    const frames: string[] = []
    for (let frame = d.next(); frame; frame = d.next()) {
      frames.push(
        [frame.command, ...frame.args]
          .map(b => JSON.stringify(b.toString('latin1')))
          .join(' '),
      )
    }
    return frames
  }

  const expected = [
    '"PING"',
    '"ECHO" "hi"',
    '"ECHO" "x y" "\xe9"',
    '"SET" "k" "a\\u0000b"',
    '"PING"',
  ]

  test('the reference stream decodes as expected in one push', () => {
    const d = decoder()
    d.push(stream)
    assert.deepStrictEqual(drain(d), expected)
  })

  test('every split point gives the same frames', () => {
    for (let cut = 1; cut < stream.length; cut++) {
      const d = decoder()
      d.push(stream.subarray(0, cut))
      const frames = drain(d)
      d.push(stream.subarray(cut))
      frames.push(...drain(d))
      assert.deepStrictEqual(frames, expected, `split at byte ${cut}`)
    }
  })

  test('one byte at a time gives the same frames', () => {
    const d = decoder()
    const frames: string[] = []
    for (let i = 0; i < stream.length; i++) {
      d.push(stream.subarray(i, i + 1))
      frames.push(...drain(d))
    }
    assert.deepStrictEqual(frames, expected)
  })

  test('a pushed chunk is copied, so the caller may reuse it', () => {
    const d = decoder()
    const chunk = Buffer.from('*2\r\n$4\r\nECHO\r\n$2\r\nh')
    d.push(chunk)
    assert.strictEqual(d.next(), null)
    chunk.fill(0x58)
    d.push(Buffer.from('i\r\n'))
    assert.deepStrictEqual(d.next(), {
      command: Buffer.from('ECHO'),
      args: [Buffer.from('hi')],
    })
  })

  // The old decoder re-parsed an incomplete multibulk from its first byte on
  // every chunk: 8x the elements cost ~64x the time. Incremental parsing keeps
  // it ~8x. The bound sits far from both, and the clock is this process's CPU
  // time (best of five), so other processes competing for the CPU cannot flip
  // it; see scripts/bench-resp2-decoder.ts for the full curve.
  test('a large multibulk fed in socket-sized chunks decodes in linear time', () => {
    function request(elements: number): Buffer {
      const parts = [Buffer.from(`*${elements}\r\n`)]
      const element = Buffer.from('$5\r\nvalue\r\n')
      for (let i = 0; i < elements; i++) {
        parts.push(element)
      }
      return Buffer.concat(parts)
    }

    function bestCpuMicros(bytes: Buffer): number {
      let best = Infinity
      for (let run = 0; run < 5; run++) {
        const d = decoder()
        const started = process.cpuUsage()
        let frames = 0
        for (let offset = 0; offset < bytes.length; offset += 16 * 1024) {
          d.push(bytes.subarray(offset, offset + 16 * 1024))
          if (d.next()) {
            frames += 1
          }
        }
        const used = process.cpuUsage(started)
        assert.strictEqual(frames, 1)
        best = Math.min(best, used.user + used.system)
      }
      return best
    }

    const small = request(12_500)
    const large = request(100_000)
    bestCpuMicros(small) // warm up
    const ratio = bestCpuMicros(large) / Math.max(bestCpuMicros(small), 1)
    assert.ok(ratio < 24, `8x the elements took ${ratio.toFixed(1)}x the time`)
  })
})
