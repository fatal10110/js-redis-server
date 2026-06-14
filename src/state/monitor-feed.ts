import type { Unsubscribe } from './mutation-events'

export type RedisMonitorCommandEvent = {
  timestampMs: number
  database: number
  clientId: string
  clientAddress?: string
  command: Buffer
  args: readonly Buffer[]
}

export type RedisMonitorCommandListener = (
  event: RedisMonitorCommandEvent,
) => void

export class RedisMonitorFeed {
  private readonly listeners = new Set<RedisMonitorCommandListener>()

  get subscriberCount(): number {
    return this.listeners.size
  }

  subscribe(listener: RedisMonitorCommandListener): Unsubscribe {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  publish(event: RedisMonitorCommandEvent): void {
    for (const listener of Array.from(this.listeners)) {
      listener(cloneMonitorCommandEvent(event))
    }
  }
}

function cloneMonitorCommandEvent(
  event: RedisMonitorCommandEvent,
): RedisMonitorCommandEvent {
  return {
    ...event,
    command: Buffer.from(event.command),
    args: event.args.map(arg => Buffer.from(arg)),
  }
}
