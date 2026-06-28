import '@xterm/xterm/css/xterm.css'
import { setLuaWasmLoadOptions } from '../../src/core/lua-runtime'
import {
  createSingleBackend,
  createClusterBackend,
  type DemoBackend,
} from './backend'
import { TabManager } from './tabs'

// Load the Lua WASM + Emscripten glue from jsDelivr instead of bundling the
// ~260 kB of assets into the GitHub Pages deploy. lua-redis-wasm's browser
// loader fetches these URLs directly (EVAL works on first use).
const LUA_WASM_VERSION = '1.4.0'
const cdn = (file: string) =>
  `https://cdn.jsdelivr.net/npm/lua-redis-wasm@${LUA_WASM_VERSION}/dist/${file}`
setLuaWasmLoadOptions({
  modulePath: cdn('redis_lua.mjs'),
  wasmPath: cdn('redis_lua.wasm'),
})

const $ = (id: string) => document.getElementById(id) as HTMLElement
const tabbar = $('tabbar')
const terminals = $('terminals')
const panel = $('panel')

let backend: DemoBackend
let tabs: TabManager
let mode: 'single' | 'cluster' = 'single'

async function buildBackend(next: 'single' | 'cluster'): Promise<DemoBackend> {
  return next === 'single' ? createSingleBackend() : createClusterBackend(3)
}

async function setMode(next: 'single' | 'cluster'): Promise<void> {
  tabs?.dispose()
  backend?.close()
  mode = next
  backend = await buildBackend(next)
  tabs = new TabManager(backend, tabbar, terminals, renderPanel)
  renderPanel()
  await tabs.boot()
}

function renderPanel(): void {
  panel.replaceChildren()

  const modeBox = document.createElement('div')
  modeBox.className = 'panel-section'
  modeBox.appendChild(heading('Mode'))
  const toggle = document.createElement('div')
  toggle.className = 'toggle'
  for (const m of ['single', 'cluster'] as const) {
    const btn = document.createElement('button')
    btn.textContent = m
    btn.className = m === mode ? 'active' : ''
    btn.onclick = () => {
      if (m !== mode) void setMode(m)
    }
    toggle.appendChild(btn)
  }
  modeBox.appendChild(toggle)
  panel.appendChild(modeBox)

  if (backend.mode === 'cluster') {
    const nodesBox = document.createElement('div')
    nodesBox.className = 'panel-section'
    nodesBox.appendChild(heading('Nodes'))
    const served = backend.lastServedNode()
    for (const node of backend.topology()) {
      const row = document.createElement('div')
      row.className = 'node' + (node.id === served ? ' served' : '')
      const slots = node.slots.map(([a, b]) => `${a}-${b}`).join(',')
      row.innerHTML =
        `<span class="node-id">${node.id}</span>` +
        `<span class="node-role">${node.role}</span>` +
        `<span class="node-slots">${slots || '—'}</span>`
      nodesBox.appendChild(row)
    }
    panel.appendChild(nodesBox)
  }

  const hintsBox = document.createElement('div')
  hintsBox.className = 'panel-section'
  hintsBox.appendChild(heading('Try'))
  const hints =
    backend.mode === 'cluster'
      ? [
          'SET foo bar',
          'SET hello world',
          'GET hello',
          'EVAL "return redis.call(\'GET\',KEYS[1])" 1 hello',
          'MONITOR  (in another tab)',
        ]
      : [
          'SET hello world',
          'HSET h a 1 b 2',
          'HGETALL h',
          'SUBSCRIBE news   /   PUBLISH news hi',
          'BLPOP q 0   /   LPUSH q job',
          'EVAL "return 1+1" 0',
        ]
  const list = document.createElement('ul')
  list.className = 'hints'
  for (const h of hints) {
    const li = document.createElement('li')
    li.textContent = h
    list.appendChild(li)
  }
  hintsBox.appendChild(list)
  panel.appendChild(hintsBox)
}

function heading(text: string): HTMLElement {
  const h = document.createElement('h2')
  h.textContent = text
  return h
}

void setMode('single')
