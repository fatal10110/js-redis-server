import { defineCommand } from '../core/command-definition'
import { t } from '../core/command-schema'
import {
  RedisCommandError,
  WrongNumberOfArgumentsError,
} from '../core/redis-error'
import { RedisResult } from '../core/redis-result'
import { RedisValue } from '../core/redis-value'
import type { RedisExecutionContext } from '../core/redis-context'
import type { ResponseStream } from '../core/response-stream'
import { array, integer } from './helpers'
import { commandSubcommandInfo } from './introspection'

type PubSubArgs = {
  subcommand: string
  args: Buffer[]
}

export const subscribeCommand = defineCommand({
  name: 'subscribe',
  schema: t.object({
    channels: t.variadic(t.bulk(), { min: 1 }),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    arity: -2,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  keys: () => [],
  execute: (args, ctx) =>
    framesResult(ctx.session.subscribePubSubChannels(args.channels)),
})

export const unsubscribeCommand = defineCommand({
  name: 'unsubscribe',
  schema: t.object({
    channels: t.variadic(t.bulk()),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    arity: -1,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  keys: () => [],
  execute: (args, ctx) =>
    framesResult(ctx.session.unsubscribePubSubChannels(args.channels)),
})

export const ssubscribeCommand = defineCommand({
  name: 'ssubscribe',
  schema: t.object({
    channels: t.variadic(t.key(), { min: 1 }),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    arity: -2,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@pubsub', '@slow'],
  },
  keys: args => args.channels,
  execute: (args, ctx) =>
    framesResult(ctx.session.subscribePubSubShardChannels(args.channels)),
})

export const sunsubscribeCommand = defineCommand({
  name: 'sunsubscribe',
  schema: t.object({
    channels: t.variadic(t.key()),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    arity: -1,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@pubsub', '@slow'],
  },
  keys: args => args.channels,
  execute: (args, ctx) =>
    framesResult(ctx.session.unsubscribePubSubShardChannels(args.channels)),
})

export const psubscribeCommand = defineCommand({
  name: 'psubscribe',
  schema: t.object({
    patterns: t.variadic(t.bulk(), { min: 1 }),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    arity: -2,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  keys: () => [],
  execute: (args, ctx) =>
    framesResult(ctx.session.subscribePubSubPatterns(args.patterns)),
})

export const punsubscribeCommand = defineCommand({
  name: 'punsubscribe',
  schema: t.object({
    patterns: t.variadic(t.bulk()),
  }),
  flags: ['pubsub', 'noscript', 'subscribed'],
  introspection: {
    arity: -1,
    flags: ['pubsub', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@slow'],
  },
  keys: () => [],
  execute: (args, ctx) =>
    framesResult(ctx.session.unsubscribePubSubPatterns(args.patterns)),
})

export const publishCommand = defineCommand({
  name: 'publish',
  schema: t.object({
    channel: t.bulk(),
    message: t.bulk(),
  }),
  flags: ['pubsub', 'fast'],
  introspection: {
    arity: 3,
    flags: ['pubsub', 'loading', 'stale', 'fast'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@pubsub', '@fast'],
  },
  keys: () => [],
  execute: (args, ctx) =>
    integer(ctx.server.pubsubBroker.publish(args.channel, args.message)),
})

export const spublishCommand = defineCommand({
  name: 'spublish',
  schema: t.object({
    channel: t.key(),
    message: t.bulk(),
  }),
  flags: ['pubsub', 'fast'],
  introspection: {
    arity: 3,
    flags: ['pubsub', 'loading', 'stale', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@pubsub', '@fast'],
  },
  keys: args => [args.channel],
  execute: (args, ctx) =>
    integer(ctx.server.pubsubBroker.spublish(args.channel, args.message)),
})

export const pubsubCommand = defineCommand({
  name: 'pubsub',
  schema: t.object({
    subcommand: t.string(),
    args: t.variadic(t.bulk()),
  }),
  flags: ['readonly', 'pubsub', 'fast'],
  introspection: {
    arity: -2,
    flags: ['pubsub', 'loading', 'stale', 'fast'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
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
  keys: args => {
    if (args.subcommand.toLowerCase() === 'shardnumsub') {
      return args.args
    }

    return []
  },
  execute: (args, ctx) => {
    const subcommand = args.subcommand.toLowerCase()

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
      expectPubSubSubcommandMaxArgCount(args.subcommand, args.args, 1)
      const channels = ctx.server.pubsubBroker.shardChannelsMatching(
        args.args[0],
      )
      return array(channels.map(channel => RedisValue.bulkString(channel)))
    }

    if (subcommand === 'shardnumsub') {
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
      return pubsubHelp()
    }

    throw new RedisCommandError(
      `unknown subcommand '${args.subcommand}'. Try PUBSUB HELP.`,
    )
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
  expectPubSubSubcommandMaxArgCount(args.subcommand, args.args, 1)
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

function pubsubHelp(): RedisResult {
  return RedisResult.create(
    RedisValue.array(
      [
        'PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
        'CHANNELS [<pattern>]',
        '    Return the currently active channels matching a pattern.',
        'NUMSUB [<channel> ...]',
        '    Return the number of subscribers for the specified channels.',
        'NUMPAT',
        '    Return the number of pattern subscriptions.',
        'SHARDCHANNELS [<pattern>]',
        '    Return active shard channels matching a pattern.',
        'SHARDNUMSUB [<channel> ...]',
        '    Return the number of shard subscribers for the specified channels.',
        'HELP',
        '    Prints this help.',
      ].map(line => RedisValue.bulkString(Buffer.from(line))),
    ),
  )
}

function framesResult(frames: RedisResult[]): RedisResult | ResponseStream {
  if (frames.length === 1) {
    return frames[0]
  }

  return {
    kind: 'response-stream',
    closed: Promise.resolve(),
    frames: async function* () {
      for (const frame of frames) {
        yield frame
      }
    },
    close: () => {},
  }
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

function expectPubSubSubcommandMaxArgCount(
  subcommand: string,
  args: readonly Buffer[],
  count: number,
): void {
  if (args.length > count) {
    throw pubsubSubcommandError(subcommand)
  }
}

function pubsubSubcommandError(subcommand: string): RedisCommandError {
  return new RedisCommandError(
    `unknown subcommand or wrong number of arguments for '${subcommand}'. Try PUBSUB HELP.`,
  )
}
