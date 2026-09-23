import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

// XCLAIM / XAUTOCLAIM argument validation and XINFO CONSUMERS activity times,
// pinned against real Redis (#486 review). Real Redis checks XAUTOCLAIM's
// arguments before the key (a bad argument beats WRONGTYPE / NOGROUP), but
// XCLAIM's only after the key and group — and neither creates a consumer when
// it rejects the call.

const testRunner = new TestRunner()

describe(`XCLAIM / XAUTOCLAIM validation (${testRunner.getBackendName()})`, () => {
  let client: Redis

  before(async () => {
    client = await testRunner.setupIoredisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('XAUTOCLAIM rejects COUNT outside 1.. and creates no consumer', async () => {
    const key = await streamWithGroup()
    for (const count of [0, -1, 'abc', '01']) {
      await assert.rejects(
        () => client.xautoclaim(key, 'g', 'c', 0, '0-0', 'COUNT', count),
        errorWithMessage('ERR COUNT must be > 0'),
      )
    }
    await assert.rejects(
      () => client.call('XAUTOCLAIM', key, 'g', 'c', '0', '0-0', 'COUNT'),
      errorWithMessage('ERR syntax error'),
    )
    assert.deepStrictEqual(await client.xinfo('CONSUMERS', key, 'g'), [])
    await client.del(key)
  })

  test('XAUTOCLAIM validates its arguments before the key', async () => {
    const key = await streamWithGroup()
    const missing = randomKey()
    const text = randomKey()
    await client.set(text, 'v')

    for (const target of [key, missing, text]) {
      await assert.rejects(
        () => client.xautoclaim(target, 'g', 'c', 'abc', '0-0'),
        errorWithMessage('ERR Invalid min-idle-time argument for XAUTOCLAIM'),
      )
      await assert.rejects(
        () => client.xautoclaim(target, 'g', 'c', 0, '0-0', 'COUNT', 0),
        errorWithMessage('ERR COUNT must be > 0'),
      )
    }
    await assert.rejects(
      () =>
        client.xautoclaim(
          key,
          'g',
          'c',
          0,
          '(18446744073709551615-18446744073709551615',
        ),
      errorWithMessage('ERR invalid start ID for the interval'),
    )
    assert.deepStrictEqual(await client.xinfo('CONSUMERS', key, 'g'), [])
    await client.del(key, text)
  })

  test('XAUTOCLAIM accepts interval start ids', async () => {
    const key = await streamWithGroup()
    await client.xadd(key, '2-0', 'f', 'v')
    await client.xreadgroup('GROUP', 'g', 'a', 'STREAMS', key, '>')

    assert.deepStrictEqual(
      await client.xautoclaim(key, 'g', 'b', 0, '-', 'JUSTID'),
      ['0-0', ['1-0', '2-0'], []],
    )
    assert.deepStrictEqual(
      await client.xautoclaim(key, 'g', 'b', 0, '(1-0', 'JUSTID'),
      ['0-0', ['2-0'], []],
    )
    assert.deepStrictEqual(
      await client.xautoclaim(key, 'g', 'b', 0, '+', 'JUSTID'),
      ['0-0', [], []],
    )
    await client.del(key)
  })

  test('XCLAIM checks the key and group before its arguments', async () => {
    const missing = randomKey()
    const text = randomKey()
    await client.set(text, 'v')

    await assert.rejects(
      () => client.xclaim(missing, 'g', 'c', 'abc', 'notanid'),
      errorWithMessage(
        `NOGROUP No such key '${missing}' or consumer group 'g'`,
      ),
    )
    await assert.rejects(
      () => client.xclaim(text, 'g', 'c', 'abc', 'notanid'),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
    await client.del(text)
  })

  test('XCLAIM parses ids up to the first non-id, then options', async () => {
    const key = await streamWithGroup()
    const rejects = (args: (string | number)[], message: string) =>
      assert.rejects(
        () => client.call('XCLAIM', key, 'g', 'c', ...args),
        errorWithMessage(message),
      )

    await rejects(['0', 'notanid'], "ERR Unrecognized XCLAIM option 'notanid'")
    await rejects(
      ['0', '1-0', 'notanid'],
      "ERR Unrecognized XCLAIM option 'notanid'",
    )
    await rejects(['0', '1-0', 'IDLE'], "ERR Unrecognized XCLAIM option 'IDLE'")
    await rejects(
      ['0', '1-0', 'JUSTID', '2-0'],
      "ERR Unrecognized XCLAIM option '2-0'",
    )
    await rejects(['0', '-'], "ERR Unrecognized XCLAIM option '-'")
    await rejects(
      ['abc', '1-0'],
      'ERR Invalid min-idle-time argument for XCLAIM',
    )
    await rejects(
      ['0', '1-0', 'IDLE', 'abc'],
      'ERR Invalid IDLE option argument for XCLAIM',
    )
    await rejects(
      ['0', '1-0', 'IDLE', '01'],
      'ERR Invalid IDLE option argument for XCLAIM',
    )
    await rejects(
      ['0', '1-0', 'TIME', 'abc'],
      'ERR Invalid TIME option argument for XCLAIM',
    )
    await rejects(
      ['0', '1-0', 'RETRYCOUNT', 'abc'],
      'ERR Invalid RETRYCOUNT option argument for XCLAIM',
    )
    await rejects(
      ['0', '1-0', 'LASTID', 'abc'],
      'ERR Invalid stream ID specified as stream command argument',
    )
    // A rejected XCLAIM creates no consumer.
    assert.deepStrictEqual(await client.xinfo('CONSUMERS', key, 'g'), [])
    await client.del(key)
  })

  test('XCLAIM clamps out-of-range times instead of rejecting them', async () => {
    const key = await streamWithGroup()
    await client.xreadgroup('GROUP', 'g', 'a', 'STREAMS', key, '>')

    // Negative min-idle-time is 0; a negative IDLE or future TIME means now;
    // a negative RETRYCOUNT means "not given".
    assert.deepStrictEqual(
      await client.xclaim(key, 'g', 'b', -5, '1-0', 'IDLE', -5, 'JUSTID'),
      ['1-0'],
    )
    assert.deepStrictEqual(
      await client.xclaim(
        key,
        'g',
        'b',
        0,
        '1-0',
        'TIME',
        '99999999999999',
        'RETRYCOUNT',
        -1,
        'JUSTID',
      ),
      ['1-0'],
    )
    const [[id, owner, idle, deliveries]] = (await client.xpending(
      key,
      'g',
      '-',
      '+',
      10,
    )) as [string, string, number, number][]
    assert.deepStrictEqual([id, owner, deliveries], ['1-0', 'b', 1])
    assert.ok(idle < 1000, `idle ${idle} should be near 0`)
    await client.del(key)
  })

  test('XINFO CONSUMERS inactive stays -1 until a consumer gets entries', async () => {
    const key = await streamWithGroup()
    // Nothing new to read: seen, but never active.
    await client.xgroup('SETID', key, 'g', '$')
    await client.xreadgroup('GROUP', 'g', 'reader', 'STREAMS', key, '>')
    // Nothing claimable: seen, but never active.
    await client.xclaim(key, 'g', 'claimer', 0, '1-0')
    await client.xautoclaim(key, 'g', 'autoclaimer', 0, '0-0')
    assert.deepStrictEqual(
      (await consumers(key)).map(c => [c.name, c.inactive]),
      [
        ['autoclaimer', -1],
        ['claimer', -1],
        ['reader', -1],
      ],
    )

    await client.xadd(key, '2-0', 'f', 'v')
    await client.xreadgroup('GROUP', 'g', 'reader', 'STREAMS', key, '>')
    const reader = (await consumers(key)).find(c => c.name === 'reader')!
    assert.ok(reader.inactive >= 0 && reader.inactive < 1000)

    // A history read refreshes `idle` but not `inactive`.
    await new Promise(resolve => setTimeout(resolve, 400))
    await client.xreadgroup('GROUP', 'g', 'reader', 'STREAMS', key, '0')
    const after = (await consumers(key)).find(c => c.name === 'reader')!
    assert.ok(after.idle < 300, `idle ${after.idle} should be refreshed`)
    assert.ok(after.inactive >= 350, `inactive ${after.inactive} should not be`)
    await client.del(key)
  })

  async function streamWithGroup(): Promise<string> {
    const key = randomKey()
    await client.xadd(key, '1-0', 'f', 'v')
    await client.xgroup('CREATE', key, 'g', '0')
    return key
  }

  async function consumers(
    key: string,
  ): Promise<{ name: string; idle: number; inactive: number }[]> {
    const reply = (await client.xinfo('CONSUMERS', key, 'g')) as unknown[][]
    return reply
      .map(fields => {
        const get = (name: string) => fields[fields.indexOf(name) + 1]
        return {
          name: get('name') as string,
          idle: get('idle') as number,
          inactive: get('inactive') as number,
        }
      })
      .sort((a, b) => a.name.localeCompare(b.name))
  }
})
