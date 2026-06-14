import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import type { RedisExecutionContext } from '../core/redis-context'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { ResponseStream } from '../core/response-stream'
import type { RedisMonitorCommandEvent, Unsubscribe } from '../state'
import { commandDocs } from './introspection'

export const monitorCommand = defineCommand({
  name: 'monitor',
  schema: t.object({}),
  flags: ['admin', 'noscript'],
  introspection: {
    arity: 1,
    flags: ['admin', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
    keySpecs: [],
    docs: commandDocs(
      'Listen for all requests received by the server in real time',
      'server',
      [],
      { since: '1.0.0', complexity: 'O(N)' },
    ),
  },
  keys: () => [],
  execute: (_args, ctx) => createMonitorStream(ctx),
})

export const monitorCommands = [monitorCommand]

function createMonitorStream(ctx: RedisExecutionContext): ResponseStream {
  const frames: RedisResult[] = [RedisResult.ok()]
  const waiters = new Set<() => void>()
  let closed = false
  let unsubscribe: Unsubscribe | undefined
  let unregisterResponseStreamCleanup: Unsubscribe | undefined
  let resolveClosed!: () => void
  const closedPromise = new Promise<void>(resolve => {
    resolveClosed = resolve
  })

  const wakeWaiters = () => {
    for (const waiter of Array.from(waiters)) {
      waiter()
    }
  }

  const close = () => {
    if (closed) {
      return
    }

    closed = true
    unregisterResponseStreamCleanup?.()
    unregisterResponseStreamCleanup = undefined
    unsubscribe?.()
    unsubscribe = undefined
    wakeWaiters()
    resolveClosed()
  }

  unsubscribe = ctx.server.monitorFeed.subscribe(event => {
    if (event.clientId === ctx.session.id) {
      return
    }

    frames.push(
      RedisResult.create(
        RedisValue.simpleString(formatMonitorCommandEvent(event)),
      ),
    )
    wakeWaiters()
  })
  unregisterResponseStreamCleanup =
    ctx.session.registerResponseStreamCleanup(close)

  return {
    kind: 'response-stream',
    closed: closedPromise,
    frames: async function* (signal: AbortSignal) {
      const onAbort = () => close()
      signal.addEventListener('abort', onAbort, { once: true })
      ctx.signal.addEventListener('abort', onAbort, { once: true })

      try {
        while (!closed && !signal.aborted && !ctx.signal.aborted) {
          const frame = frames.shift()
          if (frame) {
            yield frame
            continue
          }

          await waitForFrame(signal, ctx.signal, waiters)
        }
      } finally {
        signal.removeEventListener('abort', onAbort)
        ctx.signal.removeEventListener('abort', onAbort)
        close()
      }
    },
    close,
  }
}

function formatMonitorCommandEvent(event: RedisMonitorCommandEvent): string {
  const timestamp = (event.timestampMs / 1000).toFixed(6)
  const source = event.clientAddress ?? event.clientId
  const argv = [event.command, ...event.args]
    .map(formatMonitorArgument)
    .join(' ')

  return `${timestamp} [${event.database} ${source}] ${argv}`
}

function formatMonitorArgument(value: Buffer): string {
  let result = '"'

  for (const byte of value) {
    if (byte === DOUBLE_QUOTE || byte === BACKSLASH) {
      result += `\\${String.fromCharCode(byte)}`
      continue
    }

    if (byte === CARRIAGE_RETURN) {
      result += '\\r'
      continue
    }

    if (byte === LINE_FEED) {
      result += '\\n'
      continue
    }

    if (byte === TAB) {
      result += '\\t'
      continue
    }

    if (byte >= 0x20 && byte <= 0x7e) {
      result += String.fromCharCode(byte)
      continue
    }

    result += `\\x${byte.toString(16).padStart(2, '0')}`
  }

  return `${result}"`
}

function waitForFrame(
  signal: AbortSignal,
  sessionSignal: AbortSignal,
  waiters: Set<() => void>,
): Promise<void> {
  if (signal.aborted || sessionSignal.aborted) {
    return Promise.resolve()
  }

  return new Promise(resolve => {
    const cleanup = () => {
      waiters.delete(waiter)
      signal.removeEventListener('abort', waiter)
      sessionSignal.removeEventListener('abort', waiter)
    }
    const waiter = () => {
      cleanup()
      resolve()
    }

    waiters.add(waiter)
    signal.addEventListener('abort', waiter, { once: true })
    sessionSignal.addEventListener('abort', waiter, { once: true })
  })
}

const BACKSLASH = '\\'.charCodeAt(0)
const CARRIAGE_RETURN = '\r'.charCodeAt(0)
const DOUBLE_QUOTE = '"'.charCodeAt(0)
const LINE_FEED = '\n'.charCodeAt(0)
const TAB = '\t'.charCodeAt(0)
