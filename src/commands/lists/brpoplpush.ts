import { defineCommand } from '../../core/command-definition'
import { t } from '../../core/command-schema'
import { WrongNumberOfArgumentsError } from '../../core/redis-error'
import { parseBlockingTimeout } from '../blocking'
import { blockingListMove } from './blmove'
import { tryListMove } from './move'

type BrpoplpushArgs = {
  source: Buffer
  destination: Buffer
  timeout: number
}

const BRPOPLPUSH_LAYOUT = { min: 3, max: 3, keys: [0, 1] }

// BRPOPLPUSH source destination timeout: the deprecated form of
// `BLMOVE source destination RIGHT LEFT timeout`. Real Redis runs both through
// blmoveGenericCommand, so the replies, keyspace events, wake order and
// blocked-client bookkeeping are BLMOVE's. Unlike BLMOVE it exists on every
// profile (since 2.2.0).
export const brpoplpushCommand = defineCommand({
  name: 'brpoplpush',
  schema: t.custom<BrpoplpushArgs>(BRPOPLPUSH_LAYOUT, (input, index, ctx) => {
    const source = input[index]
    const destination = input[index + 1]
    const timeoutToken = input[index + 2]
    if (!source || !destination || !timeoutToken)
      throw new WrongNumberOfArgumentsError(ctx.commandName)
    const timeout = parseBlockingTimeout(timeoutToken, ctx.profile)
    return {
      value: { source, destination, timeout },
      nextIndex: index + 3,
    }
  }),
  flags: ['write', 'denyoom', 'blocking'],
  keys: args => [args.source, args.destination],
  execute: (args, ctx) =>
    tryListMove(args.source, args.destination, 'right', 'left', ctx.db) ??
    blockingListMove(
      args.source,
      args.destination,
      'right',
      'left',
      args.timeout,
      ctx,
    ),
})
