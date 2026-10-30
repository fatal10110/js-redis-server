/** What a CLIENT PAUSE holds back: may-replicate commands, or every command. */
export type ClientPauseMode = 'write' | 'all'

/** `setTimeout` takes at most a signed 32-bit delay. */
const MAX_TIMER_DELAY_MS = 2 ** 31 - 1

/**
 * The server-wide CLIENT PAUSE state (`pauseClientsByClient` /
 * `pauseActions` in networking.c). A new pause keeps the later end time and
 * the stricter mode (a WRITE pause never downgrades an ALL one); CLIENT
 * UNPAUSE, or the end time passing, lifts it and lets every postponed command
 * run, in the order they were postponed.
 */
export class ClientPauseState {
  private endMs = 0
  private pausedMode: ClientPauseMode | null = null
  private timer: ReturnType<typeof setTimeout> | undefined
  private unpaused: { promise: Promise<void>; resolve: () => void } | undefined

  /** The active pause's mode, or `null` when nothing is paused. */
  get mode(): ClientPauseMode | null {
    if (this.pausedMode !== null && Date.now() >= this.endMs) {
      this.unpause()
    }
    return this.pausedMode
  }

  /**
   * Pause until the absolute time `endMs` (an end in the past, such as
   * `CLIENT PAUSE 0`'s, only takes part in the max of the end times).
   */
  pause(endMs: number, mode: ClientPauseMode): void {
    const current = this.mode
    this.pausedMode = current === 'all' ? 'all' : mode
    this.endMs = Math.max(current === null ? 0 : this.endMs, endMs)
    if (this.mode !== null) {
      this.armTimer()
    }
  }

  /** CLIENT UNPAUSE: lift the pause now. */
  unpause(): void {
    this.pausedMode = null
    this.endMs = 0
    clearTimeout(this.timer)
    this.timer = undefined
    const unpaused = this.unpaused
    this.unpaused = undefined
    unpaused?.resolve()
  }

  /** Settles once the current pause is lifted. */
  whenUnpaused(): Promise<void> {
    if (!this.unpaused) {
      let resolve!: () => void
      const promise = new Promise<void>(done => {
        resolve = done
      })
      this.unpaused = { promise, resolve }
    }
    // A postponed command keeps the process alive until the pause ends,
    // like any pending reply; an idle pause does not.
    this.timer?.ref?.()
    return this.unpaused.promise
  }

  private armTimer(): void {
    clearTimeout(this.timer)
    const delay = Math.min(
      Math.max(this.endMs - Date.now(), 0),
      MAX_TIMER_DELAY_MS,
    )
    this.timer = setTimeout(() => {
      this.timer = undefined
      // Reading `mode` lifts an expired pause; a longer one re-arms.
      if (this.mode !== null) {
        this.armTimer()
      }
    }, delay)
    if (!this.unpaused) {
      this.timer.unref?.()
    }
  }
}
