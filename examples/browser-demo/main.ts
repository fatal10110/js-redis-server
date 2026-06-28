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
// loader fetches these URLs directly (EVAL works on first use). The version is
// injected by vite.config from the installed package, so the CDN assets always
// match the bundled loader.
declare const __LUA_WASM_VERSION__: string
const cdn = (file: string) =>
  `https://cdn.jsdelivr.net/npm/lua-redis-wasm@${__LUA_WASM_VERSION__}/dist/${file}`
setLuaWasmLoadOptions({
  modulePath: cdn('redis_lua.mjs'),
  wasmPath: cdn('redis_lua.wasm'),
})

const $ = (id: string) => document.getElementById(id) as HTMLElement
const tabbar = $('tabbar')
const terminals = $('terminals')
const panel = $('panel')

// Supported compatibility presets (see src/core/compatibility). Default first.
const PROFILES = [
  'redis-8.0',
  'redis-7.4',
  'redis-7.2',
  'redis-7.0',
  'redis-6.2',
  'valkey-9.0',
  'valkey-8.0',
] as const
type Profile = (typeof PROFILES)[number]

interface Server {
  name: string
  profile: Profile
  backend: DemoBackend
  tabs: TabManager
  host: HTMLDivElement
}

const servers: Server[] = []
let active = -1
let newProfile: Profile = 'redis-8.0'
const counts = { single: 0, cluster: 0 }

async function addServer(
  kind: 'single' | 'cluster',
  profile: Profile,
): Promise<void> {
  const backend =
    kind === 'single'
      ? await createSingleBackend(profile)
      : createClusterBackend(3, profile)
  const host = document.createElement('div')
  host.className = 'terminal-host'
  terminals.appendChild(host)
  const name = `${kind} ${++counts[kind]} · ${profile}`
  const tabs = new TabManager(backend, tabbar, host, renderPanel)
  servers.push({ name, profile, backend, tabs, host })
  await tabs.boot()
  selectServer(servers.length - 1)
}

function selectServer(index: number): void {
  active = index
  servers.forEach((s, i) => {
    s.host.style.display = i === index ? 'block' : 'none'
  })
  servers[index]?.tabs.activate()
  renderPanel()
}

function renderPanel(): void {
  panel.replaceChildren()
  const backend = servers[active]?.backend
  if (!backend) {
    return
  }

  // Active-server switcher.
  const serverBox = document.createElement('div')
  serverBox.className = 'panel-section'
  serverBox.appendChild(heading('Active server'))
  const select = document.createElement('select')
  select.className = 'server-select'
  servers.forEach((s, i) => {
    const opt = document.createElement('option')
    opt.value = String(i)
    opt.textContent = s.name
    opt.selected = i === active
    select.appendChild(opt)
  })
  select.onchange = () => selectServer(Number(select.value))
  serverBox.appendChild(select)
  panel.appendChild(serverBox)

  // Create a new server — profile applies to whichever button is clicked.
  const newBox = document.createElement('div')
  newBox.className = 'panel-section'
  newBox.appendChild(heading('New server'))
  const profileSelect = document.createElement('select')
  profileSelect.className = 'server-select'
  profileSelect.setAttribute(
    'aria-label',
    'Compatibility profile for new server',
  )
  for (const p of PROFILES) {
    const opt = document.createElement('option')
    opt.value = p
    opt.textContent = p
    opt.selected = p === newProfile
    profileSelect.appendChild(opt)
  }
  profileSelect.onchange = () => {
    newProfile = profileSelect.value as Profile
  }
  newBox.appendChild(fieldLabel('Compatibility'))
  newBox.appendChild(profileSelect)

  const addRow = document.createElement('div')
  addRow.className = 'toggle'
  for (const kind of ['single', 'cluster'] as const) {
    const btn = document.createElement('button')
    btn.textContent = `+ ${kind === 'single' ? 'instance' : 'cluster'}`
    btn.onclick = () => void addServer(kind, newProfile)
    addRow.appendChild(btn)
  }
  newBox.appendChild(addRow)
  panel.appendChild(newBox)

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

function fieldLabel(text: string): HTMLElement {
  const l = document.createElement('div')
  l.className = 'field-label'
  l.textContent = text
  return l
}

void addServer('single', 'redis-8.0')
