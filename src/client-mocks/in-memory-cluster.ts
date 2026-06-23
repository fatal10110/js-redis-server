import { buildClusterNodes, type ClusterNodePipeline } from '../cluster'
import { ClientSession } from '../core/client-session'
import type { RedisValue } from '../core/redis-value'
import { isResponseStream, type ResponseStream } from '../core/response-stream'
import { RedisClusterTopology } from '../state'

export type InMemoryClusterOptions = {
  masters: number
  replicasPerMaster?: number
}

export type CommandArgument = string | Buffer

/**
 * Client-agnostic in-memory Redis cluster.
 *
 * Builds TCP-free node pipelines (via {@link buildClusterNodes}) and routes each
 * command to the slot owner's {@link ClientSession} *up front* — the slot is
 * computed client-side from the command's keys, so the node never has to answer
 * with a `MOVED` redirect that a caller would then have to follow. That makes
 * this the socket-free cluster path (the redirect-following alternative needs a
 * fake-connection layer; this one sidesteps it entirely).
 *
 * Routing keys are extracted with a single-key (first-argument) heuristic, which
 * covers single-key commands and the common case. Genuinely multi-key
 * cross-slot commands are out of scope — they would misroute and the owning
 * node would return its normal `-CROSSSLOT`/`-MOVED` error, surfaced to the
 * caller unchanged.
 */
export class InMemoryCluster {
  private readonly topology: RedisClusterTopology
  private readonly masters: readonly ClusterNodePipeline[]
  private readonly replicationLinks: readonly { close(): void }[]
  private readonly sessions = new Map<string, ClientSession>()
  private closed = false

  private constructor(
    topology: RedisClusterTopology,
    masters: readonly ClusterNodePipeline[],
    replicationLinks: readonly { close(): void }[],
  ) {
    this.topology = topology
    this.masters = masters
    this.replicationLinks = replicationLinks
  }

  static create(options: InMemoryClusterOptions): InMemoryCluster {
    const { topology, nodes, replicationLinks } = buildClusterNodes({
      masters: options.masters,
      replicasPerMaster: options.replicasPerMaster ?? 0,
      basePort: 0,
    })
    const masters = nodes.filter(node => node.role === 'master')
    return new InMemoryCluster(topology, masters, replicationLinks)
  }

  /** The session on the master that owns the slot for this command's keys. */
  route(commandArgs: readonly CommandArgument[]): ClientSession {
    const keys = extractRoutingKeys(commandArgs)
    const slot =
      keys.length > 0 ? this.topology.calculateSlotForKeys(keys) : null
    const owner =
      slot === null
        ? this.masters[0]
        : (this.topology.getSlotOwner(slot) ?? this.masters[0])
    return this.sessionFor(owner.id)
  }

  /** Route then execute, returning the raw {@link RedisValue} (callers decode). */
  async execute(commandArgs: readonly CommandArgument[]): Promise<RedisValue> {
    if (this.closed) {
      throw new Error('in-memory cluster is closed')
    }
    if (commandArgs.length === 0) {
      throw new Error('command requires at least a name')
    }

    const session = this.route(commandArgs)
    const [name, ...rest] = commandArgs
    const result = await session.execute(toBuffer(name), rest.map(toBuffer))

    if (isResponseStream(result)) {
      return drainStream(result)
    }
    return result.value
  }

  close(): void {
    if (this.closed) {
      return
    }
    this.closed = true
    for (const session of this.sessions.values()) {
      session.close()
    }
    this.sessions.clear()
    for (const link of this.replicationLinks) {
      link.close()
    }
  }

  private sessionFor(nodeId: string): ClientSession {
    const existing = this.sessions.get(nodeId)
    if (existing) {
      return existing
    }
    const node = this.masters.find(master => master.id === nodeId)
    if (!node) {
      throw new Error(`No master pipeline for cluster node ${nodeId}`)
    }
    const session = new ClientSession({
      server: node.state,
      executor: node.executor,
      nodeRole: node.role,
    })
    this.sessions.set(nodeId, session)
    return session
  }
}

export function createInMemoryCluster(
  options: InMemoryClusterOptions,
): InMemoryCluster {
  return InMemoryCluster.create(options)
}

function extractRoutingKeys(args: readonly CommandArgument[]): Buffer[] {
  // Every routed command places its key in the first argument slot — covers
  // single-key commands and the common case. Multi-key cross-slot commands are
  // out of scope (see class doc).
  if (args.length < 2) {
    return []
  }
  return [toBuffer(args[1])]
}

/**
 * Consume a streaming reply (e.g. multi-channel SUBSCRIBE confirmations) to
 * completion and return the final frame's value. Out-of-band pushes are not
 * delivered here.
 */
async function drainStream(stream: ResponseStream): Promise<RedisValue> {
  let last: RedisValue = { kind: 'simple-string', value: 'OK' }
  const abort = new AbortController()
  for await (const frame of stream.frames(abort.signal)) {
    last = frame.value
  }
  return last
}

function toBuffer(value: CommandArgument): Buffer {
  return Buffer.isBuffer(value) ? value : Buffer.from(value)
}
