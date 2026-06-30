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
const HINT = (s: string) => `\x1b[90m${s}\x1b[0m` // grey, like redis-cli

// Argument syntax shown dimly after the cursor once a command is recognised,
// mirroring redis-cli. Demo-scoped — covers the commands the Try panel suggests.
// ponytail: static map, not COMMAND DOCS — the mock's docs metadata omits most
// optional args, so it can't produce these. Extend the map as the demo grows.
const HINTS: Record<string, string> = {
  set: 'key value [NX|XX] [GET] [EX seconds|PX ms|EXAT ts|PXAT ts|KEEPTTL]',
  get: 'key',
  getset: 'key value',
  mset: 'key value [key value ...]',
  mget: 'key [key ...]',
  append: 'key value',
  incr: 'key',
  decr: 'key',
  incrby: 'key increment',
  del: 'key [key ...]',
  exists: 'key [key ...]',
  expire: 'key seconds [NX|XX|GT|LT]',
  ttl: 'key',
  type: 'key',
  hset: 'key field value [field value ...]',
  hget: 'key field',
  hgetall: 'key',
  lpush: 'key element [element ...]',
  rpush: 'key element [element ...]',
  blpop: 'key [key ...] timeout',
  subscribe: 'channel [channel ...]',
  publish: 'channel message',
  eval: 'script numkeys [key ...] [arg ...]',
}

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
    this.term.write(this.promptText())
  }

  async runAuto(command: string): Promise<void> {
    // open() has already drawn the prompt; echo the command onto it.
    this.term.writeln(command)
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
        // Redraw without the inline hint (\x1b[K erases it), then commit.
        this.term.write('\r' + this.promptText() + this.line + '\x1b[K\r\n')
        await this.submit()
        return
      case '\x7f': // backspace
        if (this.cursor > 0) {
          this.line =
            this.line.slice(0, this.cursor - 1) + this.line.slice(this.cursor)
          this.cursor--
          this.render()
        }
        return
      case '\x03': // Ctrl-C
        this.term.write('\r' + this.promptText() + this.line + '\x1b[K^C')
        this.prompt()
        return
      case '\x1b[A': // up
        this.recall(-1)
        return
      case '\x1b[B': // down
        this.recall(1)
        return
      case '\x1b[C': // right
        if (this.cursor < this.line.length) {
          this.cursor++
          this.term.write('\x1b[C')
        }
        return
      case '\x1b[D': // left
        if (this.cursor > 0) {
          this.cursor--
          this.term.write('\x1b[D')
        }
        return
      default:
        if (data >= ' ') {
          this.line =
            this.line.slice(0, this.cursor) +
            data +
            this.line.slice(this.cursor)
          this.cursor += data.length
          this.render()
        }
    }
  }

  // ponytail: full-line redraw per keystroke, O(line length) — fine for a REPL.
  private render(): void {
    // Hint only when the cursor sits at the end of the line, like redis-cli.
    const hint = this.cursor === this.line.length ? this.hintFor(this.line) : ''
    this.term.write(
      '\r' + this.promptText() + '\x1b[K' + this.line + (hint && HINT(hint)),
    )
    const back = this.line.length - this.cursor + hint.length
    if (back > 0) {
      this.term.write(`\x1b[${back}D`)
    }
  }

  private hintFor(line: string): string {
    const cmd = /^\s*(\S+)/.exec(line)?.[1].toLowerCase()
    const syntax = cmd && HINTS[cmd]
    return syntax ? ` ${syntax}` : ''
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
    this.line = next
    this.cursor = next.length
    this.render()
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
      // Print the subscribe/psubscribe confirmation reply first, like redis-cli,
      // then begin streaming pushes.
      this.term.writeln(formatReply(res.reply as Reply))
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
    await this.addTab({ title: 'commands' })
    this.select(0)
  }

  // Show this manager's tabs again after another server was active: redraw the
  // shared tab bar and re-focus the current tab.
  activate(): void {
    this.select(this.active < 0 ? 0 : this.active)
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
