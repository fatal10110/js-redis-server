import { asciiLowerCase } from '../core/ascii-case'
import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import { WrongNumberOfArgumentsError } from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { RedisExecutionContext } from '../core/redis-context'
import { encodeRedisValue } from '../core/resp-encoder'
import {
  array,
  integer,
  subcommandSyntaxError,
  unknownSubcommandError,
} from './helpers'
import { commandSubcommandInfo } from './introspection'

type PubSubArgs = {
  subcommand: Buffer
  args: Buffer[]
}

export const subscribeCommand = defineCommand({
  name: 'subscribe',
  schema: t.object({
    channels: t.variadic(t.bulk(), { min: 1 }),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    categories: ['@pubsub', '@slow'],
  },
  keys: () => [],
  execute: (args, ctx) =>
    confirmations(ctx, ctx.session.pubsubSubscribe('channel', args.channels)),
})

export const unsubscribeCommand = defineCommand({
  name: 'unsubscribe',
  schema: t.object({
    channels: t.variadic(t.bulk()),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    categories: ['@pubsub', '@slow'],
  },
  keys: () => [],
  execute: (args, ctx) =>
    confirmations(ctx, ctx.session.pubsubUnsubscribe('channel', args.channels)),
})

export const ssubscribeCommand = defineCommand({
  name: 'ssubscribe',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: t.object({
    channels: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    categories: ['@pubsub', '@slow'],
  },
  keys: args => args.channels,
  execute: (args, ctx) =>
    confirmations(ctx, ctx.session.pubsubSubscribe('shard', args.channels)),
})

export const sunsubscribeCommand = defineCommand({
  name: 'sunsubscribe',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: t.object({
    channels: t.variadic(t.key()),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    categories: ['@pubsub', '@slow'],
  },
  keys: args => args.channels,
  execute: (args, ctx) =>
    confirmations(ctx, ctx.session.pubsubUnsubscribe('shard', args.channels)),
})

export const psubscribeCommand = defineCommand({
  name: 'psubscribe',
  schema: t.object({
    patterns: t.variadic(t.bulk(), { min: 1 }),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    categories: ['@pubsub', '@slow'],
  },
  keys: () => [],
  execute: (args, ctx) =>
    confirmations(ctx, ctx.session.pubsubSubscribe('pattern', args.patterns)),
})

export const punsubscribeCommand = defineCommand({
  name: 'punsubscribe',
  schema: t.object({
    patterns: t.variadic(t.bulk()),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    categories: ['@pubsub', '@slow'],
  },
  keys: () => [],
  execute: (args, ctx) =>
    confirmations(ctx, ctx.session.pubsubUnsubscribe('pattern', args.patterns)),
})

export const publishCommand = defineCommand({
  name: 'publish',
  schema: t.object({
    channel: t.bulk(),
    message: t.bulk(),
  }),
  flags: ['pubsub', 'fast'],
  introspection: {
    flags: ['pubsub', 'loading', 'stale', 'fast'],
    categories: ['@pubsub', '@fast'],
  },
  keys: () => [],
  execute: (args, ctx) => {
    if (
      ctx.session.mode !== 'subscribed' ||
      ctx.session.protocolVersion !== 3 ||
      !ctx.server.profile.has('pubsub.resp3-publish-reply-first')
    ) {
      return integer(
        ctx.server.pubsubBroker.publish(args.channel, args.message),
      )
    }

    const flushPushes = ctx.session.deferPushesUntilAfterReply()
    const delivered = ctx.server.pubsubBroker.publish(
      args.channel,
      args.message,
    )
    return RedisResult.create(RedisValue.integer(delivered), {
      afterReply: flushPushes,
    })
  },
})

export const spublishCommand = defineCommand({
  name: 'spublish',
  since: { redis: '7.0.0', valkey: '7.2.0' },
  schema: t.object({
    channel: t.key(),
    message: t.bulk(),
  }),
  flags: ['pubsub', 'fast'],
  introspection: {
    flags: ['pubsub', 'loading', 'stale', 'fast'],
    categories: ['@pubsub', '@fast'],
  },
  keys: args => [args.channel],
  execute: (args, ctx) =>
    integer(ctx.server.pubsubBroker.spublish(args.channel, args.message)),
})

export const pubsubCommand = defineCommand({
  name: 'pubsub',
  schema: t.object({
    // Raw bytes, not `t.string()`: the unknown-subcommand reply echoes the
    // name the client sent, and a UTF-8 decode here would lose its bytes.
    subcommand: t.bulk(),
    args: t.variadic(t.bulk()),
  }),
  flags: ['readonly', 'pubsub', 'fast'],
  introspection: {
    flags: ['pubsub', 'loading', 'stale', 'fast'],
    categories: ['@pubsub', '@slow'],
    subcommands: [
      commandSubcommandInfo('pubsub|channels', -2, {
        categories: ['@pubsub', '@slow'],
      }),
      commandSubcommandInfo('pubsub|numsub', -2, {
        categories: ['@pubsub', '@slow'],
      }),
      commandSubcommandInfo('pubsub|numpat', 2, {
        categories: ['@pubsub', '@slow'],
      }),
      commandSubcommandInfo('pubsub|shardchannels', -2, {
        categories: ['@pubsub', '@slow'],
      }),
      commandSubcommandInfo('pubsub|shardnumsub', -2, {
        categories: ['@pubsub', '@slow'],
      }),
      commandSubcommandInfo('pubsub|help', 2, {
        categories: ['@pubsub', '@slow'],
      }),
    ],
  },
  keys: () => [],
  execute: (args, ctx) => {
    const subcommand = asciiLowerCase(args.subcommand.toString())

    if (subcommand === 'channels') {
      return pubsubChannels(args, ctx)
    }

    if (subcommand === 'numsub') {
      return pubsubNumsub(args, ctx)
    }

    if (subcommand === 'numpat') {
      expectArgCount('pubsub|numpat', args.args, 0)
      return integer(ctx.server.pubsubBroker.patternSubscriptionCount())
    }

    if (subcommand === 'shardchannels') {
      if (!ctx.server.profile.has('pubsub.sharded')) {
        throw unknownSubcommandError(
          'PUBSUB',
          args.subcommand,
          ctx.server.profile,
        )
      }
      expectPubSubSubcommandMaxArgCount(args, 1, ctx)
      const channels = ctx.server.pubsubBroker.shardChannelsMatching(
        args.args[0],
      )
      return array(channels.map(channel => RedisValue.bulkString(channel)))
    }

    if (subcommand === 'shardnumsub') {
      if (!ctx.server.profile.has('pubsub.sharded')) {
        throw unknownSubcommandError(
          'PUBSUB',
          args.subcommand,
          ctx.server.profile,
        )
      }
      return RedisResult.create(
        RedisValue.array(
          args.args.flatMap(channel => [
            RedisValue.bulkString(Buffer.from(channel)),
            RedisValue.integer(
              ctx.server.pubsubBroker.shardSubscriberCount(channel),
            ),
          ]),
        ),
      )
    }

    if (subcommand === 'help') {
      expectArgCount('pubsub|help', args.args, 0)
      return pubsubHelp(ctx)
    }

    throw unknownSubcommandError('PUBSUB', args.subcommand, ctx.server.profile)
  },
})

export const pubsubCommands = [
  subscribeCommand,
  unsubscribeCommand,
  ssubscribeCommand,
  sunsubscribeCommand,
  psubscribeCommand,
  punsubscribeCommand,
  publishCommand,
  spublishCommand,
  pubsubCommand,
]

function pubsubChannels(args: PubSubArgs, ctx: RedisExecutionContext) {
  expectPubSubSubcommandMaxArgCount(args, 1, ctx)
  const channels = ctx.server.pubsubBroker.channelsMatching(args.args[0])
  return RedisResult.create(
    RedisValue.array(channels.map(channel => RedisValue.bulkString(channel))),
  )
}

function pubsubNumsub(args: PubSubArgs, ctx: RedisExecutionContext) {
  return RedisResult.create(
    RedisValue.array(
      args.args.flatMap(channel => [
        RedisValue.bulkString(Buffer.from(channel)),
        RedisValue.integer(ctx.server.pubsubBroker.subscriberCount(channel)),
      ]),
    ),
  )
}

function pubsubHelp(ctx: RedisExecutionContext): RedisResult {
  const lines = [
    'PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
    'CHANNELS [<pattern>]',
    '    Return the currently active channels matching a pattern.',
    'NUMSUB [<channel> ...]',
    '    Return the number of subscribers for the specified channels.',
    'NUMPAT',
    '    Return the number of pattern subscriptions.',
  ]

  if (ctx.server.profile.has('pubsub.sharded')) {
    lines.push(
      'SHARDCHANNELS [<pattern>]',
      '    Return active shard channels matching a pattern.',
      'SHARDNUMSUB [<channel> ...]',
      '    Return the number of shard subscribers for the specified channels.',
    )
  }

  lines.push('HELP', '    Prints this help.')

  return RedisResult.create(
    RedisValue.array(
      lines.map(line => RedisValue.bulkString(Buffer.from(line))),
    ),
  )
}

/**
 * A (UN)SUBSCRIBE-family reply: one confirmation frame per target, sent back to
 * back as a single reply so nothing pipelined behind the command can land
 * between them (#455). Inside EXEC the same bytes are embedded in the array, as
 * Redis does. Front ends that read values get the first frame as `value` and
 * the rest as `trailingFrames`.
 */
function confirmations(
  ctx: RedisExecutionContext,
  frames: RedisResult[],
): RedisResult {
  if (frames.length === 1) {
    return frames[0]
  }

  const version = ctx.session.protocolVersion
  return RedisResult.preEncoded(
    frames[0].value,
    Buffer.concat(
      frames.map(frame =>
        encodeRedisValue(frame.value, {
          version,
          profile: ctx.server.profile,
        }),
      ),
    ),
    { trailingFrames: frames.slice(1).map(frame => frame.value) },
  )
}

function expectArgCount(
  commandName: string,
  args: readonly Buffer[],
  count: number,
): void {
  if (args.length !== count) {
    throw new WrongNumberOfArgumentsError(commandName)
  }
}

/**
 * PUBSUB's variadic subcommands police their own argument count, so an excess
 * argument is real Redis' `addReplySubcommandSyntaxError` rather than the
 * dispatch-level unknown-subcommand reply or an arity error.
 */
function expectPubSubSubcommandMaxArgCount(
  args: PubSubArgs,
  count: number,
  ctx: RedisExecutionContext,
): void {
  if (args.args.length > count) {
    throw subcommandSyntaxError('PUBSUB', args.subcommand, ctx.server.profile)
  }
}
