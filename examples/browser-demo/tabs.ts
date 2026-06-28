// xterm-based terminal tabs. Each tab owns a Terminal and a DemoConnection over
// the shared keyspace, with a small line editor (history, Ctrl-C) and a streaming
// loop for push-mode commands (MONITOR / SUBSCRIBE).

import { Terminal } from '@xterm/xterm'
import type { DemoBackend, DemoConnection } from './backend'
import { tokenize, formatReply, type Reply } from './format'

const DIM = (s: string) => `\x1b[2m${s}\x1b[0m`
const RED = (s: string) => `\x1b[31m${s}\x1b[0m`
const GREEN = (s: string) => `\x1b[32m${s}\x1b[0m`
const CYAN = (s: string) => `\x1b[36m${s}\x1b[0m`

interface TabOptions {
  title: string
  autoRun?: string // a command to run on open (e.g. "MONITOR")
}

class Tab {
  readonly term: Terminal
  readonly element: HTMLDivElement
  private readonly conn: DemoConnection
  private line = ''
  private cursor = 0
  private readonly history: string[] = []
  private histPos = 0
  private streamAbort: AbortController | null = null

  constructor(
    readonly title: string,
    private readonly backend: DemoBackend,
    private readonly onActivity: () => void,
  ) {
    this.conn = backend.openConnection()
    this.term = new Terminal({
      convertEol: true,
      cursorBlink: true,
      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
      fontSize: 13,
      theme: {
        background: '#0b0e14',
        foreground: '#c7d1e0',
        cursor: '#39bae6',
      },
    })
    this.element = document.createElement('div')
    this.element.className = 'terminal-host'
    this.term.onData(d => this.onData(d))
  }

  open(): void {
    this.term.open(this.element)
    this.term.writeln(
      DIM(
        `js-redis-server — ${this.backend.mode} mode. Type Redis commands; ` +
          `try SET/GET, HSET, EVAL, SUBSCRIBE, MONITOR, BLPOP.`,
      ),
    )
  }

  async runAuto(command: string): Promise<void> {
    this.term.writeln(this.promptText() + command)
    await this.execute(command)
  }

  private promptText(): string {
    return this.backend.mode === 'cluster'
      ? CYAN('redis(cluster)> ')
      : CYAN('redis> ')
  }

  private prompt(): void {
    this.line = ''
    this.cursor = 0
    this.term.write('\r\n' + this.promptText())
  }

  private async onData(data: string): Promise<void> {
    if (this.streamAbort) {
      if (data === '\x03') {
        this.stopStreaming()
      }
      return
    }

    switch (data) {
      case '\r':
        this.term.write('\r\n')
        await this.submit()
        return
      case '\x7f': // backspace
        if (this.cursor > 0) {
          this.line = this.line.slice(0, -1)
          this.cursor--
          this.term.write('\b \b')
        }
        return
      case '\x03': // Ctrl-C
        this.term.write('^C')
        this.prompt()
        return
      case '\x1b[A': // up
        this.recall(-1)
        return
      case '\x1b[B': // down
        this.recall(1)
        return
      default:
        if (data >= ' ') {
          this.line += data
          this.cursor += data.length
          this.term.write(data)
        }
    }
  }

  private recall(dir: number): void {
    if (this.history.length === 0) {
      return
    }
    this.histPos = Math.max(
      0,
      Math.min(this.history.length, this.histPos + dir),
    )
    const next = this.history[this.histPos] ?? ''
    // clear current line
    this.term.write('\r' + this.promptText() + '\x1b[K' + next)
    this.line = next
    this.cursor = next.length
  }

  private async submit(): Promise<void> {
    const input = this.line.trim()
    if (input) {
      this.history.push(input)
      this.histPos = this.history.length
    }
    await this.execute(input)
  }

  private async execute(input: string): Promise<void> {
    const args = tokenize(input)
    if (args.length === 0) {
      this.prompt()
      return
    }
    const [name, ...rest] = args
    const res = await this.conn.send(name, rest)
    this.onActivity()

    if (res.route && this.backend.mode === 'cluster') {
      this.term.writeln(DIM(res.route))
    }
    if (!res.ok) {
      this.term.writeln(RED('(error) ' + res.error))
    } else if (res.streaming) {
      this.startStreaming()
      return
    } else {
      this.term.writeln(formatReply(res.reply as Reply))
    }
    this.prompt()
  }

  private startStreaming(): void {
    this.streamAbort = new AbortController()
    this.term.writeln(GREEN('(streaming — press Ctrl-C to stop)'))
    const signal = this.streamAbort.signal
    void (async () => {
      try {
        for await (const line of this.conn.pushes(signal)) {
          this.term.writeln(line)
        }
      } catch {
        /* aborted */
      }
    })()
  }

  private stopStreaming(): void {
    this.streamAbort?.abort()
    this.streamAbort = null
    this.term.write('^C')
    this.prompt()
  }

  dispose(): void {
    this.streamAbort?.abort()
    this.conn.close()
    this.term.dispose()
    this.element.remove()
  }
}

export class TabManager {
  private readonly tabs: Tab[] = []
  private active = -1

  constructor(
    private readonly backend: DemoBackend,
    private readonly bar: HTMLElement,
    private readonly host: HTMLElement,
    private readonly onActivity: () => void,
  ) {}

  async boot(): Promise<void> {
    this.addTab({ title: 'commands' })
    await this.addTab({ title: 'monitor', autoRun: 'MONITOR' })
    this.select(0)
  }

  async addTab(options: TabOptions): Promise<void> {
    const tab = new Tab(options.title, this.backend, this.onActivity)
    this.tabs.push(tab)
    this.host.appendChild(tab.element)
    this.renderBar()
    const index = this.tabs.length - 1
    this.select(index)
    tab.open()
    if (options.autoRun) {
      await tab.runAuto(options.autoRun)
    }
  }

  private select(index: number): void {
    this.active = index
    this.tabs.forEach((tab, i) => {
      tab.element.style.display = i === index ? 'block' : 'none'
    })
    this.renderBar()
    this.tabs[index]?.term.focus()
  }

  private renderBar(): void {
    this.bar.replaceChildren()
    this.tabs.forEach((tab, i) => {
      const btn = document.createElement('button')
      btn.className = 'tab' + (i === this.active ? ' active' : '')
      btn.textContent = tab.title
      btn.onclick = () => this.select(i)
      this.bar.appendChild(btn)
    })
    const add = document.createElement('button')
    add.className = 'tab add'
    add.textContent = '+'
    add.title = 'New tab'
    add.onclick = () =>
      void this.addTab({ title: `tab ${this.tabs.length + 1}` })
    this.bar.appendChild(add)
  }

  dispose(): void {
    for (const tab of this.tabs) {
      tab.dispose()
    }
    this.tabs.length = 0
    this.bar.replaceChildren()
    this.host.replaceChildren()
  }
}
