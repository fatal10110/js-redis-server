import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  CommandExecutor,
  CommandRegistry,
  RedisCommandError,
  RedisResult,
  RedisServerState,
  RedisValue,
  createNoopParkHandler,
  defineCommand,
  isResponseStream,
  t,
} from '../src/internal'
import type { RedisExecutionContext, ResponseStream } from '../src/internal'

function createContext(executor?: CommandExecutor): RedisExecutionContext {
  const server = new RedisServerState()
  const contextExecutor =
    executor ?? new CommandExecutor({ registry: new CommandRegistry() })
  return {
    db: server.getDatabase(0),
    server,
    session: {
      id: 'session-1',
      selectedDatabase: 0,
      mode: 'normal',
      protocolVersion: 2,
      usesSubscribedReplyMode: false,
      clusterReadOnly: false,
      setProtocolVersion: () => {},
      setClusterReadOnly: () => {},
      selectDatabase: () => {},
      beginTransaction: () => {},
      queueTransaction: () => {},
      drainTransaction: () => [],
      discardTransaction: () => {},
      markTransactionDirty: () => {},
      isTransactionDirty: () => false,
      executeTransaction: async () => RedisResult.create(RedisValue.array([])),
      watch: () => {},
      unwatch: () => {},
      isWatchDirty: () => false,
    },
    executor: contextExecutor,
    signal: new AbortController().signal,
    park: createNoopParkHandler(),
  }
}

describe('new command executor core', () => {
  test('parses typed args, extracts keys, and executes command definitions', async () => {
    const setCommand = defineCommand({
      name: 'SET',
      schema: t.object({
        key: t.key(),
        value: t.bulk(),
      }),
      flags: ['write'],
      keys: args => [args.key],
      execute: args =>
        RedisResult.create(
          RedisValue.array([
            RedisValue.bulkString(args.key),
            RedisValue.bulkString(args.value),
          ]),
        ),
    })

    const registry = new CommandRegistry()
    registry.register(setCommand)
    const executor = new CommandExecutor({ registry })

    const plan = executor.plan('set', [
      Buffer.from('key'),
      Buffer.from('value'),
    ])
    assert.strictEqual(plan.definition.name, 'set')
    assert.deepStrictEqual(plan.keys, [Buffer.from('key')])

    const result = await executor.executePlan(plan, createContext())
    assert.ok(result instanceof RedisResult)
    assert.deepStrictEqual(
      result.value,
      RedisValue.array([
        RedisValue.bulkString(Buffer.from('key')),
        RedisValue.bulkString(Buffer.from('value')),
      ]),
    )
  })

  test('returns Redis error results for unknown commands and parser errors', async () => {
    const registry = new CommandRegistry()
    const executor = new CommandExecutor({ registry })

    const unknown = await executor.executeRaw(
      'MiSsInG',
      [Buffer.from('arg')],
      createContext(),
    )
    assert.ok(unknown instanceof RedisResult)
    assert.deepStrictEqual(
      unknown.value,
      RedisValue.error(
        "unknown command 'MiSsInG', with args beginning with: 'arg' ",
        'ERR',
      ),
    )

    registry.register(
      defineCommand({
        name: 'get',
        schema: t.object({
          key: t.key(),
        }),
        flags: ['readonly'],
        keys: args => [args.key],
        execute: () => RedisResult.nil(),
      }),
    )

    const wrongArity = await executor.executeRaw(
      'get',
      [Buffer.from('key'), Buffer.from('extra')],
      createContext(),
    )
    assert.ok(wrongArity instanceof RedisResult)
    assert.deepStrictEqual(
      wrongArity.value,
      RedisValue.error("wrong number of arguments for 'get' command", 'ERR'),
    )

    registry.override(
      defineCommand({
        name: 'get',
        schema: t.object({
          key: t.key(),
        }),
        flags: ['readonly'],
        keys: args => [args.key],
        execute: () => {
          throw new RedisCommandError('runtime failure')
        },
      }),
    )

    const plan = executor.plan('get', [Buffer.from('key')])
    const runtimeError = await executor.executePlan(plan, createContext())
    assert.ok(runtimeError instanceof RedisResult)
    assert.deepStrictEqual(
      runtimeError.value,
      RedisValue.error('runtime failure', 'ERR'),
    )
  })

  test('lets policies short-circuit execution', async () => {
    const registry = new CommandRegistry()
    registry.register(
      defineCommand({
        name: 'del',
        schema: t.object({
          key: t.key(),
        }),
        flags: ['write'],
        keys: args => [args.key],
        execute: () => RedisResult.create(RedisValue.integer(1)),
      }),
    )

    const executor = new CommandExecutor({
      registry,
      policies: [
        {
          name: 'readonly',
          beforeExecute: plan =>
            plan.flags.includes('write')
              ? RedisResult.error(
                  'You cannot write against a read only replica.',
                  'READONLY',
                )
              : undefined,
        },
      ],
    })

    const result = await executor.executeRaw(
      'del',
      [Buffer.from('key')],
      createContext(),
    )

    assert.ok(result instanceof RedisResult)
    assert.deepStrictEqual(
      result.value,
      RedisValue.error(
        'You cannot write against a read only replica.',
        'READONLY',
      ),
    )
  })

  test('uses monitor metadata instead of admin flags for visibility', async () => {
    const registry = new CommandRegistry()
    registry.register(
      defineCommand({
        name: 'info',
        schema: t.object({}),
        flags: ['readonly', 'admin'],
        keys: () => [],
        execute: () =>
          RedisResult.create(RedisValue.bulkString(Buffer.from(''))),
      }),
    )
    registry.register(
      defineCommand({
        name: 'config',
        schema: t.object({}),
        flags: ['admin'],
        monitor: { skip: true },
        keys: () => [],
        execute: () => RedisResult.ok(),
      }),
    )

    const executor = new CommandExecutor({ registry })
    const ctx = createContext(executor)
    const commands: string[] = []
    ctx.server.monitorFeed.subscribe(event => {
      commands.push(event.command.toString())
    })

    await executor.executeRaw('INFO', [], ctx)
    await executor.executeRaw('CONFIG', [], ctx)

    assert.deepStrictEqual(commands, ['INFO'])
  })

  test('skips monitor events for cluster pre-execution errors', async () => {
    const clusterErrorCodes = [
      'ASK',
      'CLUSTERDOWN',
      'CROSSSLOT',
      'MOVED',
      'TRYAGAIN',
    ]

    for (const code of clusterErrorCodes) {
      const registry = new CommandRegistry()
      registry.register(
        defineCommand({
          name: 'get',
          schema: t.object({
            key: t.key(),
          }),
          flags: ['readonly'],
          keys: args => [args.key],
          execute: () => assert.fail('cluster policy should short-circuit'),
        }),
      )

      const executor = new CommandExecutor({
        registry,
        policies: [
          {
            name: 'cluster',
            beforeExecute: () => {
              throw new RedisCommandError('cluster failure', code)
            },
          },
        ],
      })
      const ctx = createContext(executor)
      const commands: string[] = []
      ctx.server.monitorFeed.subscribe(event => {
        commands.push(event.command.toString())
      })

      const result = await executor.executeRaw('GET', [Buffer.from('key')], ctx)

      assert.ok(result instanceof RedisResult)
      assert.deepStrictEqual(
        result.value,
        RedisValue.error('cluster failure', code),
      )
      assert.deepStrictEqual(commands, [])
    }
  })

  test('supports open command registration and explicit overrides', () => {
    const registry = new CommandRegistry()
    const first = defineCommand({
      name: 'ping',
      schema: t.object({}),
      flags: ['readonly'],
      keys: () => [],
      execute: () => RedisResult.create(RedisValue.simpleString('PONG')),
    })
    const second = defineCommand({
      name: 'ping',
      schema: t.object({}),
      flags: ['readonly'],
      keys: () => [],
      execute: () => RedisResult.create(RedisValue.simpleString('CUSTOM')),
    })

    registry.register(first)
    assert.throws(() => registry.register(second), /already registered/)

    registry.override(second)
    assert.strictEqual(registry.get('PING'), second)
  })

  test('supports async command results', async () => {
    const registry = new CommandRegistry()
    registry.register(
      defineCommand({
        name: 'async-ping',
        schema: t.object({}),
        flags: ['readonly'],
        keys: () => [],
        execute: async () =>
          RedisResult.create(RedisValue.simpleString('PONG')),
      }),
    )

    const executor = new CommandExecutor({ registry })
    const result = await executor.executeRaw('async-ping', [], createContext())

    assert.ok(result instanceof RedisResult)
    assert.deepStrictEqual(result.value, RedisValue.simpleString('PONG'))
  })

  test('executes sync plans through sync policy hooks', () => {
    const registry = new CommandRegistry()
    registry.register(
      defineCommand({
        name: 'ping',
        schema: t.object({}),
        flags: ['readonly'],
        keys: () => [],
        execute: () => RedisResult.create(RedisValue.simpleString('PONG')),
      }),
    )

    const executor = new CommandExecutor({
      registry,
      policies: [
        {
          name: 'observer',
          afterExecute: (_plan, _ctx, result) =>
            result.value.kind === 'simple-string'
              ? RedisResult.create(RedisValue.simpleString('POLICY-PONG'))
              : result,
        },
      ],
    })

    assert.deepStrictEqual(
      executor.executePlanSync(
        executor.plan('ping', []),
        createContext(executor),
      ),
      RedisResult.create(RedisValue.simpleString('POLICY-PONG')),
    )
  })

  test('returns Redis errors for async work in sync execution', () => {
    const registry = new CommandRegistry()
    registry.register(
      defineCommand({
        name: 'async-ping',
        schema: t.object({}),
        flags: ['readonly'],
        keys: () => [],
        execute: async () =>
          RedisResult.create(RedisValue.simpleString('PONG')),
      }),
    )

    const executor = new CommandExecutor({ registry })

    assert.deepStrictEqual(
      executor.executePlanSync(
        executor.plan('async-ping', []),
        createContext(executor),
      ),
      RedisResult.error(
        'ASYNC-PING cannot run asynchronously from scripts',
        'ERR',
      ),
    )
  })

  test('does not start async command definitions in sync execution', async () => {
    let started = false
    let mutatedAfterError = false
    const registry = new CommandRegistry()
    registry.register(
      defineCommand({
        name: 'async-write',
        schema: t.object({}),
        flags: ['write'],
        keys: () => [],
        execute: async () => {
          started = true
          await Promise.resolve()
          mutatedAfterError = true
          return RedisResult.ok()
        },
      }),
    )

    const executor = new CommandExecutor({ registry })

    assert.deepStrictEqual(
      executor.executePlanSync(
        executor.plan('async-write', []),
        createContext(executor),
      ),
      RedisResult.error(
        'ASYNC-WRITE cannot run asynchronously from scripts',
        'ERR',
      ),
    )
    await Promise.resolve()
    assert.strictEqual(started, false)
    assert.strictEqual(mutatedAfterError, false)
  })

  test('supports response streams and stream policy hooks', async () => {
    const stream: ResponseStream = {
      kind: 'response-stream',
      closed: Promise.resolve(),
      frames: async function* () {
        yield RedisResult.create(RedisValue.push('message', []))
      },
      close: () => {},
    }
    const registry = new CommandRegistry()
    registry.register(
      defineCommand({
        name: 'subscribe',
        schema: t.object({
          channel: t.bulk(),
        }),
        flags: ['pubsub'],
        capabilities: { pushOnly: true },
        keys: () => [],
        execute: () => stream,
      }),
    )

    let observed = false
    const executor = new CommandExecutor({
      registry,
      policies: [
        {
          name: 'stream-observer',
          onStream: (_plan, _ctx, currentStream) => {
            observed = currentStream === stream
          },
        },
      ],
    })

    const result = await executor.executeRaw(
      'subscribe',
      [Buffer.from('updates')],
      createContext(),
    )

    assert.strictEqual(result, stream)
    assert.strictEqual(observed, true)
  })

  test('does not await thenable response streams', async () => {
    const stream: ResponseStream & { then: () => never } = {
      kind: 'response-stream',
      closed: Promise.resolve(),
      frames: async function* () {
        yield RedisResult.create(RedisValue.push('message', []))
      },
      close: () => {},
      then: () => assert.fail('ResponseStream should not be awaited'),
    }
    const registry = new CommandRegistry()
    registry.register(
      defineCommand({
        name: 'monitor',
        schema: t.object({}),
        flags: ['pubsub'],
        capabilities: { pushOnly: true },
        keys: () => [],
        execute: () => stream,
      }),
    )

    const executor = new CommandExecutor({ registry })
    const result = await executor.executeRaw('monitor', [], createContext())

    assert.strictEqual(isResponseStream(result), true)
  })

  test('parses bigint values and keeps safe integer parser strict', () => {
    const parsedBigInt = t
      .bigInteger()
      .parse([Buffer.from('9007199254740993')], 0, { commandName: 'big' })
    assert.strictEqual(parsedBigInt.value, 9007199254740993n)

    assert.throws(
      () =>
        t.integer().parse([Buffer.from('9007199254740993')], 0, {
          commandName: 'int',
        }),
      /integer or out of range/,
    )
  })

  test('park handler supports timeout and abort', async () => {
    const park = createNoopParkHandler()
    const timeoutResult = await park({
      waitFor: new Promise<null>(() => {}),
      timeoutMs: 1,
      signal: new AbortController().signal,
    })

    assert.strictEqual(timeoutResult, null)

    const controller = new AbortController()
    const aborted = park({
      waitFor: new Promise<null>(() => {}),
      signal: controller.signal,
    })
    controller.abort()

    await assert.rejects(aborted, { name: 'AbortError' })
  })
})
