import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisClientType } from 'redis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

// node-redis twin of ioredis/stream/claim-validation.test.ts: XCLAIM /
// XAUTOCLAIM argument validation and XINFO CONSUMERS activity times (#486
// review). Arguments a typed method cannot put on the wire (a non-integer
// min-idle-time, COUNT 0 — the client drops a falsy COUNT) go through
// sendCommand.

const testRunner = new TestRunner()

describe(`XCLAIM / XAUTOCLAIM validation (node-redis, ${testRunner.getBackendName()})`, () => {
  let client: RedisClientType

  before(async () => {
    client = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('XAUTOCLAIM rejects COUNT outside 1.. and creates no consumer', async () => {
    const key = await streamWithGroup()
    for (const count of ['0', '-1', 'abc', '01']) {
      await assert.rejects(
        () =>
          client.sendCommand([
            'XAUTOCLAIM',
            key,
            'g',
            'c',
            '0',
            '0-0',
            'COUNT',
            count,
          ]),
        errorWithMessage('ERR COUNT must be > 0'),
      )
    }
    await assert.rejects(
      () =>
        client.sendCommand(['XAUTOCLAIM', key, 'g', 'c', '0', '0-0', 'COUNT']),
      errorWithMessage('ERR syntax error'),
    )
    assert.deepStrictEqual(await client.xInfoConsumers(key, 'g'), [])
    await client.del(key)
  })

  test('XAUTOCLAIM validates its arguments before the key', async () => {
    const key = await streamWithGroup()
    const missing = randomKey()
    const text = randomKey()
    await client.set(text, 'v')

    for (const target of [key, missing, text]) {
      await assert.rejects(
        () =>
          client.sendCommand(['XAUTOCLAIM', target, 'g', 'c', 'abc', '0-0']),
        errorWithMessage('ERR Invalid min-idle-time argument for XAUTOCLAIM'),
      )
      await assert.rejects(
        () =>
          client.sendCommand([
            'XAUTOCLAIM',
            target,
            'g',
            'c',
            '0',
            '0-0',
            'COUNT',
            '0',
          ]),
        errorWithMessage('ERR COUNT must be > 0'),
      )
    }
    await assert.rejects(
      () =>
        client.xAutoClaim(
          key,
          'g',
          'c',
          0,
          '(18446744073709551615-18446744073709551615',
        ),
      errorWithMessage('ERR invalid start ID for the interval'),
    )
    assert.deepStrictEqual(await client.xInfoConsumers(key, 'g'), [])
    await client.del([key, text])
  })

  test('XAUTOCLAIM accepts interval start ids', async () => {
    const key = await streamWithGroup()
    await client.xAdd(key, '2-0', { f: 'v' })
    await client.xReadGroup('g', 'a', { key, id: '>' })

    const claimedIds = async (start: string) =>
      (await client.xAutoClaimJustId(key, 'g', 'b', 0, start)).messages
    assert.deepStrictEqual(await claimedIds('-'), ['1-0', '2-0'])
    assert.deepStrictEqual(await claimedIds('(1-0'), ['2-0'])
    assert.deepStrictEqual(await claimedIds('+'), [])
    await client.del(key)
  })

  test('XCLAIM checks the key and group before its arguments', async () => {
    const missing = randomKey()
    const text = randomKey()
    await client.set(text, 'v')

    await assert.rejects(
      () => client.sendCommand(['XCLAIM', missing, 'g', 'c', 'abc', 'notanid']),
      errorWithMessage(
        `NOGROUP No such key '${missing}' or consumer group 'g'`,
      ),
    )
    await assert.rejects(
      () => client.sendCommand(['XCLAIM', text, 'g', 'c', 'abc', 'notanid']),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
    await client.del(text)
  })

  test('XCLAIM parses ids up to the first non-id, then options', async () => {
    const key = await streamWithGroup()
    const rejects = (args: string[], message: string) =>
      assert.rejects(
        () => client.sendCommand(['XCLAIM', key, 'g', 'c', ...args]),
        errorWithMessage(message),
      )

    await assert.rejects(
      () => client.xClaim(key, 'g', 'c', 0, 'notanid'),
      errorWithMessage("ERR Unrecognized XCLAIM option 'notanid'"),
    )
    await assert.rejects(
      () => client.xClaim(key, 'g', 'c', 0, ['1-0', 'notanid']),
      errorWithMessage("ERR Unrecognized XCLAIM option 'notanid'"),
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
    await assert.rejects(
      () => client.xClaim(key, 'g', 'c', 0, '1-0', { LASTID: 'abc' }),
      errorWithMessage(
        'ERR Invalid stream ID specified as stream command argument',
      ),
    )
    // A rejected XCLAIM creates no consumer.
    assert.deepStrictEqual(await client.xInfoConsumers(key, 'g'), [])
    await client.del(key)
  })

  test('XCLAIM clamps out-of-range times instead of rejecting them', async () => {
    const key = await streamWithGroup()
    await client.xReadGroup('g', 'a', { key, id: '>' })

    // Negative min-idle-time is 0; a negative IDLE or future TIME means now;
    // a negative RETRYCOUNT means "not given".
    assert.deepStrictEqual(
      await client.xClaimJustId(key, 'g', 'b', -5, '1-0', { IDLE: -5 }),
      ['1-0'],
    )
    assert.deepStrictEqual(
      await client.xClaimJustId(key, 'g', 'b', 0, '1-0', {
        TIME: 99999999999999,
        RETRYCOUNT: -1,
      }),
      ['1-0'],
    )
    const [pending] = await client.xPendingRange(key, 'g', '-', '+', 10)
    assert.deepStrictEqual(
      [pending.id, pending.consumer, pending.deliveriesCounter],
      ['1-0', 'b', 1],
    )
    assert.ok(pending.millisecondsSinceLastDelivery < 1000)
    await client.del(key)
  })

  test('XINFO CONSUMERS inactive stays -1 until a consumer gets entries', async () => {
    const key = await streamWithGroup()
    // Nothing new to read: seen, but never active.
    await client.xGroupSetId(key, 'g', '$')
    await client.xReadGroup('g', 'reader', { key, id: '>' })
    // Nothing claimable: seen, but never active.
    await client.xClaim(key, 'g', 'claimer', 0, '1-0')
    await client.xAutoClaim(key, 'g', 'autoclaimer', 0, '0-0')
    assert.deepStrictEqual(
      (await consumers(key)).map(c => [c.name, c.inactive]),
      [
        ['autoclaimer', -1],
        ['claimer', -1],
        ['reader', -1],
      ],
    )

    await client.xAdd(key, '2-0', { f: 'v' })
    await client.xReadGroup('g', 'reader', { key, id: '>' })
    const reader = (await consumers(key)).find(c => c.name === 'reader')!
    assert.ok(reader.inactive >= 0 && reader.inactive < 1000)

    // A history read refreshes `idle` but not `inactive`.
    await new Promise(resolve => setTimeout(resolve, 400))
    await client.xReadGroup('g', 'reader', { key, id: '0' })
    const after = (await consumers(key)).find(c => c.name === 'reader')!
    assert.ok(after.idle < 300, `idle ${after.idle} should be refreshed`)
    assert.ok(after.inactive >= 350, `inactive ${after.inactive} should not be`)
    await client.del(key)
  })

  async function streamWithGroup(): Promise<string> {
    const key = randomKey()
    await client.xAdd(key, '1-0', { f: 'v' })
    await client.xGroupCreate(key, 'g', '0')
    return key
  }

  async function consumers(
    key: string,
  ): Promise<{ name: string; idle: number; inactive: number }[]> {
    const reply = await client.xInfoConsumers(key, 'g')
    return reply
      .map(consumer => ({
        name: String(consumer.name),
        idle: Number(consumer.idle),
        inactive: Number(consumer.inactive),
      }))
      .sort((a, b) => a.name.localeCompare(b.name))
  }
})
