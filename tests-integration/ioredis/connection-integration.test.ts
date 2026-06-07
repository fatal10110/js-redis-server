import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createConnection, type Socket } from 'node:net'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import {
  commandFrame,
  connectToSlotOwner,
  errorWithMessage,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe(`Connection commands integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined
  let directClient: Redis | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('connection-integration')
    directClient = await connectToSlotOwner(
      redisClient,
      `{connection:${randomKey()}}:probe`,
    )
  })

  after(async () => {
    directClient?.disconnect()
    await testRunner.cleanup()
  })

  test('PING and INFO expose Redis-compatible connection metadata', async () => {
    assert.strictEqual(await directClient?.ping(), 'PONG')
    assert.strictEqual(await directClient?.call('PING', 'hello'), 'hello')

    const info = (await directClient?.info()) ?? ''
    assert.match(info, /loading:0/)
    assert.match(info, /redis_mode:cluster/)
    assert.match(info, /cluster_enabled:1/)
  })

  test('CLIENT name, id, info, and list are connection-local', async () => {
    const name = `client-${randomKey()}`

    assert.strictEqual(
      await directClient?.call('CLIENT', 'SETNAME', name),
      'OK',
    )
    assert.strictEqual(await directClient?.call('CLIENT', 'GETNAME'), name)

    const id = await directClient?.call('CLIENT', 'ID')
    assert.strictEqual(typeof id, 'number')
    assert.ok((id as number) > 0)

    const info = (await directClient?.call('CLIENT', 'INFO')) as string
    assert.match(info, new RegExp(`name=${name}`))
    assert.match(info, /db=0/)

    const list = (await directClient?.call('CLIENT', 'LIST')) as string
    assert.match(list, new RegExp(`name=${name}`))
  })

  test('HELLO can set the connection name and reports cluster mode', async () => {
    const name = `hello-${randomKey()}`
    const hello = (await directClient?.call(
      'HELLO',
      '2',
      'SETNAME',
      name,
    )) as unknown[]

    assertHelloEntry(hello, 'server', 'redis')
    assertHelloEntry(hello, 'proto', 2)
    assertHelloEntry(hello, 'mode', 'cluster')
    assert.strictEqual(await directClient?.call('CLIENT', 'GETNAME'), name)
  })

  test('HELLO 3 switches the connection to RESP3 replies', async () => {
    const port = testRunner.getClusterPorts()[0]
    assert.notStrictEqual(port, undefined)
    const connection = await RawRedisConnection.connect('127.0.0.1', port!)

    try {
      connection.write(commandFrame('HELLO', '3'))
      const hello = await connection.readFrame()

      assert.ok(hello instanceof Map)
      assert.strictEqual(respText(respMapGet(hello, 'server')), 'redis')
      assert.strictEqual(respNumber(respMapGet(hello, 'proto')), 3)
      assert.strictEqual(respText(respMapGet(hello, 'mode')), 'cluster')

      connection.write(commandFrame('CLIENT', 'GETNAME'))
      assert.strictEqual(await connection.readFrame(), null)
    } finally {
      connection.close()
    }
  })

  test('AUTH without configured password returns the Redis error', async () => {
    await assert.rejects(
      () => directClient?.auth('secret'),
      errorWithMessage(
        'ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?',
      ),
    )
  })

  test('RESET clears connection-local state', async () => {
    assert.strictEqual(
      await directClient?.call('CLIENT', 'SETNAME', 'reset-name'),
      'OK',
    )
    assert.strictEqual(await directClient?.call('RESET'), 'RESET')
    assert.strictEqual(await directClient?.call('CLIENT', 'GETNAME'), null)
  })

  test('SELECT is rejected in cluster mode', async () => {
    await assert.rejects(
      () => directClient?.select(1),
      errorWithMessage('ERR SELECT is not allowed in cluster mode'),
    )
  })
})

function assertHelloEntry(
  reply: unknown[],
  key: string,
  expected: string | number,
): void {
  const index = reply.indexOf(key)
  assert.notStrictEqual(index, -1)
  assert.strictEqual(reply[index + 1], expected)
}

type RespWireValue =
  | string
  | Buffer
  | number
  | boolean
  | null
  | RespWireValue[]
  | Map<unknown, RespWireValue>

type ParsedFrame =
  | { complete: true; value: RespWireValue; nextIndex: number }
  | { complete: false }

class RawRedisConnection {
  private buffered = Buffer.alloc(0)
  private readonly waiters: Array<() => void> = []
  private closed = false
  private error: Error | null = null

  private constructor(private readonly socket: Socket) {
    socket.on('data', chunk => {
      this.buffered = Buffer.concat([this.buffered, chunk])
      this.wakeWaiters()
    })
    socket.on('error', error => {
      this.error = error
      this.wakeWaiters()
    })
    socket.on('close', () => {
      this.closed = true
      this.wakeWaiters()
    })
  }

  static async connect(
    host: string,
    port: number,
  ): Promise<RawRedisConnection> {
    const socket = createConnection({ host, port })
    await new Promise<void>((resolve, reject) => {
      const onConnect = () => {
        socket.off('error', onError)
        resolve()
      }
      const onError = (error: Error) => {
        socket.off('connect', onConnect)
        reject(error)
      }

      socket.once('connect', onConnect)
      socket.once('error', onError)
    })
    return new RawRedisConnection(socket)
  }

  write(frame: Buffer): void {
    this.socket.write(frame)
  }

  async readFrame(): Promise<RespWireValue> {
    while (true) {
      const parsed = parseRespFrame(this.buffered, 0)
      if (parsed.complete) {
        this.buffered = this.buffered.subarray(parsed.nextIndex)
        return parsed.value
      }

      if (this.error) {
        throw this.error
      }

      if (this.closed) {
        throw new Error(
          'Redis connection closed before a complete frame arrived',
        )
      }

      await new Promise<void>(resolve => {
        this.waiters.push(resolve)
      })
    }
  }

  close(): void {
    this.socket.destroy()
  }

  private wakeWaiters(): void {
    const waiters = this.waiters.splice(0)
    for (const waiter of waiters) {
      waiter()
    }
  }
}

function parseRespFrame(buffer: Buffer, index: number): ParsedFrame {
  const prefix = buffer[index]
  if (prefix === undefined) {
    return { complete: false }
  }

  if (prefix === PLUS || prefix === MINUS) {
    return parseLineString(buffer, index + 1)
  }

  if (prefix === COLON) {
    return parseNumberFrame(buffer, index + 1)
  }

  if (prefix === COMMA) {
    return parseDoubleFrame(buffer, index + 1)
  }

  if (prefix === HASH) {
    return parseBooleanFrame(buffer, index + 1)
  }

  if (prefix === UNDERSCORE) {
    if (!hasCrlf(buffer, index + 1)) {
      return { complete: false }
    }
    return { complete: true, value: null, nextIndex: index + 3 }
  }

  if (prefix === DOLLAR || prefix === EQUALS) {
    return parseBlobFrame(buffer, index + 1)
  }

  if (prefix === ASTERISK || prefix === TILDE || prefix === GREATER_THAN) {
    return parseArrayFrame(buffer, index + 1)
  }

  if (prefix === PERCENT) {
    return parseMapFrame(buffer, index + 1)
  }

  return { complete: false }
}

function parseLineString(buffer: Buffer, index: number): ParsedFrame {
  const line = readLine(buffer, index)
  if (!line) {
    return { complete: false }
  }
  return {
    complete: true,
    value: line.value.toString(),
    nextIndex: line.nextIndex,
  }
}

function parseNumberFrame(buffer: Buffer, index: number): ParsedFrame {
  const line = readLine(buffer, index)
  if (!line) {
    return { complete: false }
  }
  return {
    complete: true,
    value: Number(line.value.toString()),
    nextIndex: line.nextIndex,
  }
}

function parseDoubleFrame(buffer: Buffer, index: number): ParsedFrame {
  const line = readLine(buffer, index)
  if (!line) {
    return { complete: false }
  }
  return {
    complete: true,
    value: Number(line.value.toString()),
    nextIndex: line.nextIndex,
  }
}

function parseBooleanFrame(buffer: Buffer, index: number): ParsedFrame {
  if (!hasCrlf(buffer, index + 1)) {
    return { complete: false }
  }
  return {
    complete: true,
    value: buffer[index] === LOWER_T,
    nextIndex: index + 3,
  }
}

function parseBlobFrame(buffer: Buffer, index: number): ParsedFrame {
  const header = readLine(buffer, index)
  if (!header) {
    return { complete: false }
  }

  const length = Number(header.value.toString())
  if (length < 0) {
    return { complete: true, value: null, nextIndex: header.nextIndex }
  }

  const valueStart = header.nextIndex
  const valueEnd = valueStart + length
  const nextIndex = valueEnd + 2
  if (buffer.length < nextIndex) {
    return { complete: false }
  }

  return {
    complete: true,
    value: Buffer.from(buffer.subarray(valueStart, valueEnd)),
    nextIndex,
  }
}

function parseArrayFrame(buffer: Buffer, index: number): ParsedFrame {
  const header = readLine(buffer, index)
  if (!header) {
    return { complete: false }
  }

  const count = Number(header.value.toString())
  if (count < 0) {
    return { complete: true, value: null, nextIndex: header.nextIndex }
  }

  const values: RespWireValue[] = []
  let cursor = header.nextIndex
  for (let i = 0; i < count; i++) {
    const parsed = parseRespFrame(buffer, cursor)
    if (!parsed.complete) {
      return { complete: false }
    }
    values.push(parsed.value)
    cursor = parsed.nextIndex
  }

  return { complete: true, value: values, nextIndex: cursor }
}

function parseMapFrame(buffer: Buffer, index: number): ParsedFrame {
  const header = readLine(buffer, index)
  if (!header) {
    return { complete: false }
  }

  const count = Number(header.value.toString())
  const values = new Map<unknown, RespWireValue>()
  let cursor = header.nextIndex

  for (let i = 0; i < count; i++) {
    const key = parseRespFrame(buffer, cursor)
    if (!key.complete) {
      return { complete: false }
    }
    cursor = key.nextIndex

    const value = parseRespFrame(buffer, cursor)
    if (!value.complete) {
      return { complete: false }
    }
    cursor = value.nextIndex
    values.set(key.value, value.value)
  }

  return { complete: true, value: values, nextIndex: cursor }
}

function readLine(
  buffer: Buffer,
  index: number,
): { value: Buffer; nextIndex: number } | null {
  const end = buffer.indexOf('\r\n', index)
  if (end === -1) {
    return null
  }

  return {
    value: buffer.subarray(index, end),
    nextIndex: end + 2,
  }
}

function hasCrlf(buffer: Buffer, index: number): boolean {
  return buffer[index] === CR && buffer[index + 1] === LF
}

function respMapGet(
  value: Map<unknown, RespWireValue>,
  key: string,
): RespWireValue {
  for (const [entryKey, entryValue] of value.entries()) {
    if (respText(entryKey) === key) {
      return entryValue
    }
  }

  assert.fail(`Missing RESP map key ${key}`)
}

function respText(value: unknown): string {
  if (Buffer.isBuffer(value)) {
    return value.toString()
  }
  assert.strictEqual(typeof value, 'string')
  return value
}

function respNumber(value: unknown): number {
  assert.strictEqual(typeof value, 'number')
  return value
}

const ASTERISK = '*'.charCodeAt(0)
const COLON = ':'.charCodeAt(0)
const COMMA = ','.charCodeAt(0)
const CR = '\r'.charCodeAt(0)
const DOLLAR = '$'.charCodeAt(0)
const EQUALS = '='.charCodeAt(0)
const GREATER_THAN = '>'.charCodeAt(0)
const HASH = '#'.charCodeAt(0)
const LF = '\n'.charCodeAt(0)
const LOWER_T = 't'.charCodeAt(0)
const MINUS = '-'.charCodeAt(0)
const PERCENT = '%'.charCodeAt(0)
const PLUS = '+'.charCodeAt(0)
const TILDE = '~'.charCodeAt(0)
const UNDERSCORE = '_'.charCodeAt(0)
