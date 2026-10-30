import type { RedisClientSession } from './redis-context'
import { RedisResult } from './redis-result'

/**
 * CLIENT REPLY state of one connection, as real Redis' client flags:
 * `CLIENT_REPLY_OFF`, `CLIENT_REPLY_SKIP` (this command's reply is dropped)
 * and `CLIENT_REPLY_SKIP_NEXT` (set by CLIENT REPLY SKIP, armed for the next
 * command when this one is done).
 */
type ReplyState = { off: boolean; skip: boolean; skipNext: boolean }

const replyStates = new WeakMap<RedisClientSession, ReplyState>()

function stateOf(session: RedisClientSession): ReplyState {
  let state = replyStates.get(session)
  if (!state) {
    state = { off: false, skip: false, skipNext: false }
    replyStates.set(session, state)
  }
  return state
}

/**
 * `CLIENT REPLY ON|OFF|SKIP` (networking.c `clientCommand`). ON clears OFF
 * and the pending skip before it answers; SKIP does nothing while OFF.
 */
export function setClientReplyMode(
  session: RedisClientSession,
  mode: 'on' | 'off' | 'skip',
): void {
  const state = stateOf(session)
  if (mode === 'on') {
    state.off = false
    state.skip = false
  } else if (mode === 'off') {
    state.off = true
  } else if (!state.off) {
    state.skipNext = true
  }
}

/**
 * RESET (`clearClientConnectionState`) clears OFF and a SKIP still to be
 * armed, but not the skip of the command running now: a RESET right after
 * CLIENT REPLY SKIP is not answered.
 */
export function resetClientReplyMode(session: RedisClientSession): void {
  const state = replyStates.get(session)
  if (state) {
    state.off = false
    state.skipNext = false
  }
}

/**
 * Called once per processed command with its result (`resetClient` after
 * every command): drops the reply while OFF or skipping, then arms a skip
 * CLIENT REPLY SKIP asked for. Pushes are delivered regardless
 * (`CLIENT_PUSHING`): the (un)subscribe confirmations, marked `push`, still
 * use up a skip; the others never come through here.
 */
export function applyClientReplyMode(
  session: RedisClientSession,
  result: RedisResult,
): RedisResult {
  const state = replyStates.get(session)
  if (!state) {
    return result
  }

  const suppress = state.off || state.skip
  state.skip = state.skipNext
  state.skipNext = false
  if (!suppress || result.options?.omitReply || result.options?.push) {
    return result
  }
  return new RedisResult(
    result.value,
    { ...result.options, omitReply: true },
    result.encoded,
  )
}
