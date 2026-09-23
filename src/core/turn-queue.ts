export interface RedisTurnHandle {
  release(): void
  suspend(waitFor: Promise<unknown>): Promise<RedisTurnHandle>
}

type TurnResolver = () => void

export class SerialTurnQueue {
  private readonly queue: TurnResolver[] = []
  /**
   * Turns re-requested by suspended (parked) commands whose wait settled. They
   * run ahead of `queue` so a woken blocking command re-checks its key before
   * any command that arrived after it — but among themselves they stay FIFO:
   * waiters woken by the same write resume in the order they were woken
   * (= the order they blocked), matching real Redis' fair service of blocked
   * clients. A single `unshift` onto `queue` would reverse that order.
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
        ): Promise<RedisTurnHandle> => {
          if (!active) {
            throw new Error('Turn already released')
          }

          active = false
          this.locked = false
          this.scheduleNext()
          await waitFor
          return this.waitTurnInternal(true)
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
