// Mode-agnostic command routing for the demo. A `DemoBackend` owns one shared
// keyspace (single instance, or a TCP-free cluster) and hands out `DemoConnection`s
// — one per terminal tab — that all see the same data. Streaming (MONITOR/SUBSCRIBE)
// and blocking (BLPOP/…) commands work because connections share the keyspace.

import {
  createInMemoryRedis,
  InMemoryRedisClient,
  type RedisNativeReply,
} from '../../src/in-memory-client'
import { buildClusterNodes } from '../../src/cluster'
import type { CompatibilitySpec } from '../../src/core/compatibility'
import { formatReply, type Reply } from './format'

export interface NodeInfo {
  id: string
  role: 'master' | 'replica'
  host: string
  port: number
  slots: [number, number][]
}

export type SendResult = {
  /** True for a normal reply, false for an error reply (printed verbatim). */
  ok: boolean
  reply?: Reply
  error?: string
  /** Human-readable routing trace in cluster mode (e.g. `→ master-0 -MOVED→ master-2`). */
  route?: string
  /** The command put the connection into push mode — caller should drain `pushes()`. */
  streaming: boolean
}

export interface DemoConnection {
  send(name: string, args: string[]): Promise<SendResult>
  /** Pre-formatted server-initiated lines (pub/sub messages, MONITOR feed). */
  pushes(signal: AbortSignal): AsyncIterable<string>
  close(): void
}

export interface DemoBackend {
  readonly mode: 'single' | 'cluster'
  topology(): NodeInfo[]
  openConnection(): DemoConnection
  lastServedNode(): string | undefined
  close(): void
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

// --- single ----------------------------------------------------------------

class SingleConnection implements DemoConnection {
  constructor(private readonly conn: InMemoryRedisClient) {}

  async send(name: string, args: string[]): Promise<SendResult> {
    try {
      const reply = await this.conn.command(name, ...args)
      return { ok: true, reply, streaming: this.conn.streaming }
    } catch (err) {
      return { ok: false, error: errorMessage(err), streaming: false }
    }
  }

  async *pushes(signal: AbortSignal): AsyncIterable<string> {
    for await (const frame of this.conn.pushes(signal)) {
      yield formatReply(frame as Reply)
    }
  }

  close(): void {
    this.conn.close()
  }
}

// --- cluster ----------------------------------------------------------------

const MOVED = /^MOVED (\d+) (\S+):(\d+)/

class ClusterConnection implements DemoConnection {
  private streamingNodes: { id: string; conn: InMemoryRedisClient }[] = []
  // Sticky current node, like `redis-cli -c`: stay on whatever node last served
  // a command and only redirect (and print MOVED) when the key lives elsewhere.
  private currentNode: string

  constructor(
    private readonly conns: Map<string, InMemoryRedisClient>,
    private readonly byAddr: Map<string, string>,
    defaultNode: string,
    private readonly onServed: (nodeId: string) => void,
  ) {
    this.currentNode = defaultNode
  }

  async send(name: string, args: string[]): Promise<SendResult> {
    const hops: string[] = []
    let nodeId = this.currentNode

    for (let i = 0; i < 6; i++) {
      const conn = this.conns.get(nodeId)!
      try {
        const reply = await conn.command(name, ...args)

        if (conn.streaming) {
          // A streaming command (no keys → no MOVED). Fan it out to every node
          // so a MONITOR/SUBSCRIBE tab sees cluster-wide traffic.
          await this.fanOutStreaming(name, args, nodeId)
          return { ok: true, reply, route: '→ all nodes', streaming: true }
        }

        this.currentNode = nodeId
        this.onServed(nodeId)
        const route = [...hops, nodeId].join(' ')
        return { ok: true, reply, route: `→ ${route}`, streaming: false }
      } catch (err) {
        const moved = MOVED.exec(errorMessage(err))
        if (moved) {
          hops.push(`${nodeId} -MOVED ${moved[1]}→`)
          nodeId = this.byAddr.get(`${moved[2]}:${moved[3]}`) ?? nodeId
          continue
        }
        return { ok: false, error: errorMessage(err), streaming: false }
      }
    }
    return { ok: false, error: 'too many MOVED redirects', streaming: false }
  }

  private async fanOutStreaming(
    name: string,
    args: string[],
    alreadyOn: string,
  ): Promise<void> {
    this.streamingNodes = []
    for (const [id, conn] of this.conns) {
      if (id !== alreadyOn) {
        await conn.command(name, ...args)
      }
      this.streamingNodes.push({ id, conn })
    }
  }

  async *pushes(signal: AbortSignal): AsyncIterable<string> {
    // Merge every streaming node's feed, tagging each line with its node id.
    const queue: string[] = []
    let finished = 0
    let wake: (() => void) | null = null
    const ping = () => {
      wake?.()
      wake = null
    }

    for (const { id, conn } of this.streamingNodes) {
      void (async () => {
        try {
          for await (const frame of conn.pushes(signal)) {
            queue.push(`[${id}] ${formatReply(frame as Reply)}`)
            ping()
          }
        } finally {
          finished++
          ping()
        }
      })()
    }

    while (!signal.aborted) {
      const line = queue.shift()
      if (line !== undefined) {
        yield line
        continue
      }
      if (finished === this.streamingNodes.length) {
        return
      }
      await new Promise<void>(resolve => {
        wake = resolve
        signal.addEventListener('abort', () => resolve(), { once: true })
      })
    }
  }

  close(): void {
    for (const conn of this.conns.values()) {
      conn.close()
    }
  }
}

// --- factories --------------------------------------------------------------

export async function createSingleBackend(
  compatibility?: CompatibilitySpec,
): Promise<DemoBackend> {
  const instance = await createInMemoryRedis({
    databaseCount: 16,
    compatibility,
  })
  return {
    mode: 'single',
    topology: () => [],
    openConnection: () => new SingleConnection(instance.connect()),
    lastServedNode: () => undefined,
    close: () => instance.close(),
  }
}

export function createClusterBackend(
  masters = 3,
  compatibility?: CompatibilitySpec,
): DemoBackend {
  const { topology, nodes } = buildClusterNodes({
    masters,
    basePort: 7000,
    compatibility,
  })
  const byAddr = new Map<string, string>()
  for (const node of nodes) {
    byAddr.set(`${node.host}:${node.port}`, node.id)
  }
  const defaultNode = nodes[0].id
  let lastServed: string | undefined

  const nodeInfo: NodeInfo[] = topology.nodes.map(n => ({
    id: n.id,
    role: n.role,
    host: n.host,
    port: n.port,
    slots: n.slots.map(([start, end]) => [start, end] as [number, number]),
  }))

  return {
    mode: 'cluster',
    topology: () => nodeInfo,
    openConnection: () => {
      const conns = new Map<string, InMemoryRedisClient>()
      for (const node of nodes) {
        conns.set(
          node.id,
          new InMemoryRedisClient({
            server: node.state,
            executor: node.executor,
          }),
        )
      }
      return new ClusterConnection(conns, byAddr, defaultNode, id => {
        lastServed = id
      })
    },
    lastServedNode: () => lastServed,
    close: () => {
      for (const node of nodes) {
        node.state.close()
      }
    },
  }
}

export type { RedisNativeReply }
