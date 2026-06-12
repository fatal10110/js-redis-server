import assert from 'node:assert'
import { createConnection, type Socket } from 'node:net'

/**
 * Raw TCP harness for the integration suite.
 *
 * Most integration tests drive the server through a real client (ioredis /
 * node-redis). Those clients can only ever speak well-formed RESP — they will
 * never emit an inline command, a malformed frame, or hand you the exact wire
 * bytes a server replied with. This harness opens a bare TCP socket so tests
 * can write arbitrary bytes and assert on the raw RESP response, against either
 * backend (the in-process mock server or a real redis-server) since both listen
 * on a normal TCP port.
 */

export type RespWireValue =
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

export class RawRedisConnection {
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
    socket.setNoDelay(true)
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

  write(frame: Buffer | string): void {
    this.socket.write(typeof frame === 'string' ? Buffer.from(frame) : frame)
  }

  /** Read and decode the next complete RESP frame. */
  async readFrame(): Promise<RespWireValue> {
    return (await this.readParsedFrame()).value
  }

  /** Read the next complete RESP frame and return its exact wire bytes. */
  async readRawFrame(): Promise<Buffer> {
    return (await this.readParsedFrame()).raw
  }

  /**
   * Wait until the server closes the connection, then return everything it sent
   * but that has not yet been consumed by readFrame/readRawFrame. Used to assert
   * "server replied X, then hung up" (e.g. a protocol error after a bad frame).
   */
  async readUntilClose(): Promise<Buffer> {
    while (!this.closed) {
      if (this.error) {
        throw this.error
      }
      await new Promise<void>(resolve => {
        this.waiters.push(resolve)
      })
    }
    return Buffer.from(this.buffered)
  }

  private async readParsedFrame(): Promise<{
    raw: Buffer
    value: RespWireValue
  }> {
    while (true) {
      const parsed = parseRespFrame(this.buffered, 0)
      if (parsed.complete) {
        const raw = Buffer.from(this.buffered.subarray(0, parsed.nextIndex))
        this.buffered = this.buffered.subarray(parsed.nextIndex)
        return { raw, value: parsed.value }
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

export function respMapGet(value: unknown, key: string): RespWireValue {
  assert.ok(value instanceof Map, 'expected a RESP map reply')
  for (const [entryKey, entryValue] of value.entries()) {
    if (respText(entryKey) === key) {
      return entryValue
    }
  }

  assert.fail(`Missing RESP map key ${key}`)
}

export function respText(value: unknown): string {
  if (Buffer.isBuffer(value)) {
    return value.toString()
  }
  assert.strictEqual(typeof value, 'string')
  return value as string
}

export function respNumber(value: unknown): number {
  assert.strictEqual(typeof value, 'number')
  return value as number
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
