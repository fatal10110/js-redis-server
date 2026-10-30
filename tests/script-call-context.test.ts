import { describe, test } from 'node:test'
import assert from 'node:assert'

import {
  ClientSession,
  RedisResult,
  RedisServerState,
  RedisValue,
  createRedisCommandExecutor,
  defineCommand,
  t,
} from '../src/internal'

// The context a script's redis.call / redis.pcall runs under (#563, #566).

function buf(...values: string[]): Buffer[] {
  return values.map(value => Buffer.from(value))
}

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

describe('script call context', () => {
  test('SELECT moves the script, never the caller (#566)', async () => {
    const server = new RedisServerState({ databaseCount: 16 })
    const executor = createRedisCommandExecutor({
      compatibility: server.profile,
    })
    const session = new ClientSession({ server, executor })

    await session.execute('select', buf('3'))
    const result = await session.execute(
      'eval',
      buf(
        "redis.call('select','5') redis.call('set',KEYS[1],'v') return redis.call('get',KEYS[1])",
        '1',
        'k',
      ),
    )
    assert.deepStrictEqual(
      result.value,
      RedisValue.bulkString(Buffer.from('v')),
    )
    assert.strictEqual(session.selectedDatabase, 3)
    assert.strictEqual(server.getDatabase(3).get(Buffer.from('k')), null)
    assert.strictEqual(
      server.getDatabase(5).getString(Buffer.from('k'))?.toString(),
      'v',
    )
  })

  test('a command that parks from a script is refused, and its promise consumed (#563)', async () => {
    const parks: string[] = []
    const parkme = defineCommand({
      name: 'parkme',
      schema: t.object({}),
      flags: ['readonly'],
      keys: () => [],
      execute: (_args, ctx) =>
        ctx
          .park({ waitFor: new Promise<true>(() => {}), signal: ctx.signal })
          .then(
            () => {
              parks.push('parked')
              return RedisResult.create(RedisValue.simpleString('OK'))
            },
            (err: unknown) => {
              parks.push('refused')
              throw err
            },
          ),
    })
    const server = new RedisServerState()
    const executor = createRedisCommandExecutor({
      compatibility: server.profile,
      extraCommands: [parkme],
    })
    const session = new ClientSession({ server, executor })

    const seen: unknown[] = []
    const onUnhandled = (reason: unknown) => seen.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      const result = await session.execute(
        'eval',
        buf("return redis.pcall('parkme')", '0'),
      )
      assert.strictEqual(result.value.kind, 'error')
      await delay(20)
      assert.deepStrictEqual(parks, ['refused'])
      assert.deepStrictEqual(seen, [])
      assert.deepStrictEqual(
        (await session.execute('ping', [])).value,
        RedisValue.simpleString('PONG'),
      )
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
  })
})
