export interface RedisTurnHandle {
  release(): void
  suspend(waitFor: Promise<unknown>): Promise<RedisTurnHandle>
}

export interface RedisTurnQueue {
  waitTurn(): Promise<RedisTurnHandle>
}

type TurnResolver = () => void

export class SerialTurnQueue implements RedisTurnQueue {
  private readonly queue: TurnResolver[] = []
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
        this.queue.unshift(grantTurn)
      } else {
        this.queue.push(grantTurn)
      }

      this.scheduleNext()
    })
  }

  private scheduleNext(): void {
    if (this.locked) return

    const next = this.queue.shift()
    next?.()
  }
}
