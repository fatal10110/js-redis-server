export interface RedisTurnHandle {
  release(): void
  /**
   * Release the turn until `waitFor` settles, then re-acquire one ahead of
   * every queued command. `bindResume` hands the caller a `resume` it may call
   * synchronously the moment it knows the wait is over, to take that place in
   * line before `waitFor`'s promise chain has run.
   */
  suspend(
    waitFor: Promise<unknown>,
    bindResume?: (resume: () => void) => void,
  ): Promise<RedisTurnHandle>
}

type TurnResolver = () => void

export class SerialTurnQueue {
  private readonly queue: TurnResolver[] = []
  /**
   * Turns re-requested by suspended (parked) commands whose wait is over. They
   * run before every command in `queue`, but FIFO among themselves: waiters
   * woken by one write resume in the order they were woken, which is the order
   * they blocked. Real Redis serves blocked clients in that order too. An
   * `unshift` onto `queue` would reverse it.
   */
  private readonly resumed: TurnResolver[] = []
  private locked = false

  waitTurn(): Promise<RedisTurnHandle> {
    return this.waitTurnInternal(false)
  }

  private waitTurnInternal(priority: boolean): Promise<RedisTurnHandle> {
    return new Promise(resolve => {
      const grantTurn = () => {
        this.locked = true
        let active = true

        const release = () => {
          if (!active) return
          active = false
          this.locked = false
          this.scheduleNext()
        }

        const suspend = async (
          waitFor: Promise<unknown>,
          bindResume?: (resume: () => void) => void,
        ): Promise<RedisTurnHandle> => {
          if (!active) {
            throw new Error('Turn already released')
          }

          active = false
          this.locked = false
          this.scheduleNext()

          let settled = false
          let resumed: Promise<RedisTurnHandle> | undefined
          const resume = () => {
            if (settled) return
            resumed ??= this.waitTurnInternal(true)
          }
          bindResume?.(resume)

          try {
            await waitFor
          } catch (err) {
            settled = true
            // A resume queued before the wait failed would wedge the queue if
            // left unclaimed; take that turn and hand it straight back.
            if (resumed) (await resumed).release()
            throw err
          }
          resume()
          settled = true
          return resumed!
        }

        resolve({ release, suspend })
      }

      if (priority) {
        this.resumed.push(grantTurn)
      } else {
        this.queue.push(grantTurn)
      }

      this.scheduleNext()
    })
  }

  private scheduleNext(): void {
    if (this.locked) return

    const next = this.resumed.shift() ?? this.queue.shift()
    next?.()
  }
}
