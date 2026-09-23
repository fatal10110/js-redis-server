import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import { RedisCommandError } from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import { formatMonitorTimestamp } from '../core/clock'
import type { RedisMonitorCommandEvent } from '../state'
import { commandDocs } from './introspection'

export const monitorCommand = defineCommand({
  name: 'monitor',
  schema: t.object({}),
  flags: ['admin', 'noscript'],
  monitor: {
    skip: true,
  },
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
  execute: (_args, ctx) => {
    // Redis runs EXEC as a DENY BLOCKING client, which MONITOR refuses.
    if (ctx.transactionReplay) {
      throw new RedisCommandError(
        "MONITOR isn't allowed for DENY BLOCKING client",
      )
    }

    // Redis ignores MONITOR on a connection that is already monitoring: no
    // reply at all.
    if (ctx.session.monitoring) {
      return RedisResult.create(RedisValue.null(), { omitReply: true })
    }

    // Feed lines are session pushes. Hold them until +OK is on the wire so a
    // command another client runs meanwhile cannot overtake it.
    const flushPushes = ctx.session.deferPushesUntilAfterReply()
    ctx.session.startMonitor(event =>
      RedisResult.create(
        RedisValue.simpleString(formatMonitorCommandEvent(event)),
      ),
    )
    return RedisResult.create(RedisValue.simpleString('OK'), {
      afterReply: flushPushes,
    })
  },
})

export const monitorCommands = [monitorCommand]

function formatMonitorCommandEvent(event: RedisMonitorCommandEvent): string {
  const timestamp = formatMonitorTimestamp(event.timestampMicros)
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

    if (byte === BELL) {
      result += '\\a'
      continue
    }

    if (byte === BACKSPACE) {
      result += '\\b'
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

const BACKSPACE = '\b'.charCodeAt(0)
const BACKSLASH = '\\'.charCodeAt(0)
const BELL = '\u0007'.charCodeAt(0)
const CARRIAGE_RETURN = '\r'.charCodeAt(0)
const DOUBLE_QUOTE = '"'.charCodeAt(0)
const LINE_FEED = '\n'.charCodeAt(0)
const TAB = '\t'.charCodeAt(0)
