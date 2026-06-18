import { redisGlobMatch } from '../core/glob'
import type { Unsubscribe } from './mutation-events'

export type RedisPubSubMessage = {
  channel: Buffer
  message: Buffer
}

export type RedisPubSubPatternMessage = RedisPubSubMessage & {
  pattern: Buffer
}

export type RedisPubSubMessageListener = (message: RedisPubSubMessage) => void

export type RedisPubSubPatternMessageListener = (
  message: RedisPubSubPatternMessage,
) => void

type ChannelSubscriber = {
  channel: Buffer
  listener: RedisPubSubMessageListener
}

type PatternSubscriber = {
  pattern: Buffer
  listener: RedisPubSubPatternMessageListener
}

export class RedisPubSubBroker {
  private readonly channels = new Map<string, Map<symbol, ChannelSubscriber>>()
  private readonly shardChannels = new Map<
    string,
    Map<symbol, ChannelSubscriber>
  >()
  private readonly patterns = new Map<string, Map<symbol, PatternSubscriber>>()

  subscribe(
    channel: Buffer,
    listener: RedisPubSubMessageListener,
  ): Unsubscribe {
    const id = Symbol('channel-subscriber')
    const key = pubsubKey(channel)
    let subscribers = this.channels.get(key)
    if (!subscribers) {
      subscribers = new Map()
      this.channels.set(key, subscribers)
    }

    subscribers.set(id, { channel: Buffer.from(channel), listener })
    return () => {
      subscribers?.delete(id)
      if (subscribers?.size === 0) {
        this.channels.delete(key)
      }
    }
  }

  psubscribe(
    pattern: Buffer,
    listener: RedisPubSubPatternMessageListener,
  ): Unsubscribe {
    const id = Symbol('pattern-subscriber')
    const key = pubsubKey(pattern)
    let subscribers = this.patterns.get(key)
    if (!subscribers) {
      subscribers = new Map()
      this.patterns.set(key, subscribers)
    }

    subscribers.set(id, { pattern: Buffer.from(pattern), listener })
    return () => {
      subscribers?.delete(id)
      if (subscribers?.size === 0) {
        this.patterns.delete(key)
      }
    }
  }

  ssubscribe(
    channel: Buffer,
    listener: RedisPubSubMessageListener,
  ): Unsubscribe {
    const id = Symbol('shard-channel-subscriber')
    const key = pubsubKey(channel)
    let subscribers = this.shardChannels.get(key)
    if (!subscribers) {
      subscribers = new Map()
      this.shardChannels.set(key, subscribers)
    }

    subscribers.set(id, { channel: Buffer.from(channel), listener })
    return () => {
      subscribers?.delete(id)
      if (subscribers?.size === 0) {
        this.shardChannels.delete(key)
      }
    }
  }

  publish(channel: Buffer, message: Buffer): number {
    let delivered = 0
    const channelSubscribers = this.channels.get(pubsubKey(channel))

    if (channelSubscribers) {
      for (const subscriber of Array.from(channelSubscribers.values())) {
        subscriber.listener({
          channel: Buffer.from(channel),
          message: Buffer.from(message),
        })
        delivered++
      }
    }

    for (const subscribers of Array.from(this.patterns.values())) {
      for (const subscriber of Array.from(subscribers.values())) {
        if (!redisGlobMatch(subscriber.pattern, channel)) {
          continue
        }

        subscriber.listener({
          pattern: Buffer.from(subscriber.pattern),
          channel: Buffer.from(channel),
          message: Buffer.from(message),
        })
        delivered++
      }
    }

    return delivered
  }

  spublish(channel: Buffer, message: Buffer): number {
    let delivered = 0
    const channelSubscribers = this.shardChannels.get(pubsubKey(channel))

    if (!channelSubscribers) {
      return delivered
    }

    for (const subscriber of Array.from(channelSubscribers.values())) {
      subscriber.listener({
        channel: Buffer.from(channel),
        message: Buffer.from(message),
      })
      delivered++
    }

    return delivered
  }

  channelsMatching(pattern?: Buffer): Buffer[] {
    const channels: Buffer[] = []

    for (const subscribers of this.channels.values()) {
      const first = subscribers.values().next().value
      if (!first) {
        continue
      }

      if (pattern && !redisGlobMatch(pattern, first.channel)) {
        continue
      }

      channels.push(Buffer.from(first.channel))
    }

    return channels
  }

  shardChannelsMatching(pattern?: Buffer): Buffer[] {
    const channels: Buffer[] = []

    for (const subscribers of this.shardChannels.values()) {
      const first = subscribers.values().next().value
      if (!first) {
        continue
      }

      if (pattern && !redisGlobMatch(pattern, first.channel)) {
        continue
      }

      channels.push(Buffer.from(first.channel))
    }

    return channels
  }

  subscriberCount(channel: Buffer): number {
    return this.channels.get(pubsubKey(channel))?.size ?? 0
  }

  shardSubscriberCount(channel: Buffer): number {
    return this.shardChannels.get(pubsubKey(channel))?.size ?? 0
  }

  patternSubscriptionCount(): number {
    let count = 0
    for (const subscribers of this.patterns.values()) {
      count += subscribers.size
    }
    return count
  }
}

function pubsubKey(value: Buffer): string {
  return value.toString('hex')
}
