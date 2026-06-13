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
        if (!redisGlobMatch(subscriber.pattern, 0, channel, 0)) {
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

  channelsMatching(pattern?: Buffer): Buffer[] {
    const channels: Buffer[] = []

    for (const subscribers of this.channels.values()) {
      const first = subscribers.values().next().value
      if (!first) {
        continue
      }

      if (pattern && !redisGlobMatch(pattern, 0, first.channel, 0)) {
        continue
      }

      channels.push(Buffer.from(first.channel))
    }

    return channels
  }

  subscriberCount(channel: Buffer): number {
    return this.channels.get(pubsubKey(channel))?.size ?? 0
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

function redisGlobMatch(
  pattern: Buffer,
  patternIndex: number,
  value: Buffer,
  valueIndex: number,
): boolean {
  let patternCursor = patternIndex
  let valueCursor = valueIndex

  while (patternCursor < pattern.length && valueCursor < value.length) {
    const token = pattern[patternCursor]

    if (token === STAR) {
      while (pattern[patternCursor + 1] === STAR) {
        patternCursor++
      }

      if (patternCursor + 1 === pattern.length) {
        return true
      }

      for (
        let nextValueCursor = valueCursor;
        nextValueCursor <= value.length;
        nextValueCursor++
      ) {
        if (
          redisGlobMatch(pattern, patternCursor + 1, value, nextValueCursor)
        ) {
          return true
        }
      }

      return false
    }

    if (token === QUESTION_MARK) {
      patternCursor++
      valueCursor++
      continue
    }

    if (token === OPEN_BRACKET) {
      const characterClass = matchCharacterClass(
        pattern,
        patternCursor,
        value[valueCursor],
      )

      if (!characterClass.matches) {
        return false
      }

      patternCursor = characterClass.nextPatternIndex
      valueCursor++
      continue
    }

    if (token === BACKSLASH && patternCursor + 1 < pattern.length) {
      patternCursor++
    }

    if (pattern[patternCursor] !== value[valueCursor]) {
      return false
    }

    patternCursor++
    valueCursor++
  }

  while (pattern[patternCursor] === STAR) {
    patternCursor++
  }

  return patternCursor === pattern.length && valueCursor === value.length
}

function matchCharacterClass(
  pattern: Buffer,
  openBracketIndex: number,
  value: number,
): { matches: boolean; nextPatternIndex: number } {
  let patternCursor = openBracketIndex + 1
  let negated = false

  if (pattern[patternCursor] === CARET) {
    negated = true
    patternCursor++
  }

  let matches = false

  while (true) {
    if (pattern[patternCursor] === CLOSE_BRACKET) {
      break
    }

    if (patternCursor >= pattern.length) {
      patternCursor--
      break
    }

    if (
      pattern[patternCursor] === BACKSLASH &&
      patternCursor + 1 < pattern.length
    ) {
      patternCursor++
      if (pattern[patternCursor] === value) {
        matches = true
      }
    } else if (
      patternCursor + 2 < pattern.length &&
      pattern[patternCursor + 1] === DASH
    ) {
      let start = pattern[patternCursor]
      let end = pattern[patternCursor + 2]

      if (start > end) {
        const previousStart = start
        start = end
        end = previousStart
      }

      if (value >= start && value <= end) {
        matches = true
      }

      patternCursor += 2
    } else if (pattern[patternCursor] === value) {
      matches = true
    }

    patternCursor++
  }

  return {
    matches: negated ? !matches : matches,
    nextPatternIndex: patternCursor + 1,
  }
}

const BACKSLASH = '\\'.charCodeAt(0)
const CARET = '^'.charCodeAt(0)
const CLOSE_BRACKET = ']'.charCodeAt(0)
const DASH = '-'.charCodeAt(0)
const OPEN_BRACKET = '['.charCodeAt(0)
const QUESTION_MARK = '?'.charCodeAt(0)
const STAR = '*'.charCodeAt(0)
