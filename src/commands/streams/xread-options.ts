import { equalsAscii } from '../../core/ascii-case'
import type { ParseContext } from '../../core/command-schema'
import { isIntegerToken } from '../../core/command-schema'
import { RedisCommandError, errors } from '../../core/redis-error'

export type XreadOptions = {
  /** `null` when absent; Redis treats a COUNT of 0 or below as no limit. */
  count: number | null
  blockMs: number | null
  noack: boolean
  group: Buffer | null
  consumer: Buffer | null
  /** Index in `input` of the first stream key. */
  streamsStart: number
  /** Number of streams (keys; the IDs follow them). */
  streamCount: number
}

/**
 * The option loop of Redis's `xreadCommand`, shared by XREAD and XREADGROUP,
 * with its errors in its order: options are read left to right until
 * `STREAMS`; `COUNT` takes an integer (`value is not an integer or out of
 * range`), `BLOCK` a non-negative integer timeout; `GROUP` and `NOACK` belong
 * to XREADGROUP; an odd number of arguments after `STREAMS` is the
 * `Unbalanced ... list of streams` error; anything else, or no `STREAMS`, is
 * `syntax error`. Verified against redis-server 6.2.24, 7.0.15, 7.2, 8.0.6
 * and valkey 8.0 / 9.0.
 */
export function parseXreadOptions(
  input: readonly Buffer[],
  index: number,
  ctx: ParseContext,
  xreadgroup: boolean,
): XreadOptions {
  const options: XreadOptions = {
    count: null,
    blockMs: null,
    noack: false,
    group: null,
    consumer: null,
    streamsStart: -1,
    streamCount: 0,
  }

  for (let i = index; i < input.length; i++) {
    const token = input[i]
    const moreArgs = input.length - i - 1
    if (equalsAscii(token, 'block') && moreArgs > 0) {
      i++
      options.blockMs = parseBlockTimeout(input[i])
    } else if (equalsAscii(token, 'count') && moreArgs > 0) {
      i++
      const raw = input[i].toString()
      if (!isIntegerToken(raw)) {
        throw errors.expectedInteger()
      }
      const count = Number(raw)
      options.count = count > 0 ? count : null
    } else if (equalsAscii(token, 'streams') && moreArgs > 0) {
      options.streamsStart = i + 1
      if (moreArgs % 2 !== 0) {
        throw unbalancedStreams(ctx, xreadgroup)
      }
      options.streamCount = moreArgs / 2
      break
    } else if (equalsAscii(token, 'group') && moreArgs >= 2) {
      if (!xreadgroup) {
        throw new RedisCommandError(
          'The GROUP option is only supported by XREADGROUP. You called XREAD instead.',
        )
      }
      options.group = input[i + 1]
      options.consumer = input[i + 2]
      i += 2
    } else if (equalsAscii(token, 'noack')) {
      if (!xreadgroup) {
        throw new RedisCommandError(
          'The NOACK option is only supported by XREADGROUP. You called XREAD instead.',
        )
      }
      options.noack = true
    } else {
      throw errors.syntax()
    }
  }

  if (options.streamsStart === -1) {
    throw errors.syntax()
  }
  if (xreadgroup && options.group === null) {
    throw new RedisCommandError('Missing GROUP option for XREADGROUP')
  }
  return options
}

/** `getTimeoutFromObjectOrReply` in milliseconds. */
function parseBlockTimeout(raw: Buffer): number {
  const text = raw.toString()
  if (!isIntegerToken(text) || !Number.isSafeInteger(Number(text))) {
    throw new RedisCommandError('timeout is not an integer or out of range')
  }
  const ms = Number(text)
  if (ms < 0) {
    throw errors.timeoutNegative()
  }
  return ms
}

/**
 * Redis 6.2 / 7.0 name XREAD for both commands; 7.2 names the command and
 * gives XREADGROUP its `>`; Redis 8.0 adds XREAD's `+` to the list (7.4
 * accepts `+` but does not mention it).
 */
function unbalancedStreams(
  ctx: ParseContext,
  xreadgroup: boolean,
): RedisCommandError {
  if (!ctx.profile.has('stream.xread-unbalanced-wording')) {
    return new RedisCommandError(
      "Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.",
    )
  }
  if (xreadgroup) {
    return new RedisCommandError(
      "Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.",
    )
  }
  return new RedisCommandError(
    ctx.profile.has('stream.xread-unbalanced-plus-wording')
      ? "Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified."
      : "Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
  )
}
