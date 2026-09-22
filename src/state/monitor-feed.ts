import type { Unsubscribe } from './mutation-events'

export type RedisMonitorCommandEvent = {
  /**
   * Microseconds since the Unix epoch, matching the resolution real Redis
   * prints on every `MONITOR` line (`<unix-seconds>.<6 digits>`, stamped from
   * `gettimeofday()`). Produced by {@link monitorTimestampMicros} — do not use
   * `Date.now() * 1000` here, which looks the same but quantizes every line to
   * a whole millisecond (#388).
   *
   * A plain `number` is exact for this: microsecond epoch values stay below
   * `Number.MAX_SAFE_INTEGER` until the year 2255.
   */
  timestampMicros: number
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
