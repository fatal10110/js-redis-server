export type RedisClusterNodeRole = 'master' | 'replica'

export type RedisClusterNode = {
  id: string
  role: RedisClusterNodeRole
  host: string
  port: number
  masterId?: string
  slots: Array<[number, number]>
}

export const REDIS_CLUSTER_SLOT_COUNT = 16384

/**
 * CRC16 lookup table for the XMODEM variant Redis Cluster uses (`crc16.c`):
 * polynomial 0x1021, initial value 0, no input/output reflection, no final
 * XOR. `crc16('123456789')` is 0x31c3.
 */
const CRC16_TABLE: readonly number[] = (() => {
  const table = new Array<number>(256)
  for (let byte = 0; byte < 256; byte++) {
    let crc = byte << 8
    for (let bit = 0; bit < 8; bit++) {
      crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff
    }
    table[byte] = crc
  }
  return table
})()

function crc16(buf: Buffer, start: number, end: number): number {
  let crc = 0
  for (let i = start; i < end; i++) {
    crc = ((crc << 8) & 0xffff) ^ CRC16_TABLE[((crc >> 8) ^ buf[i]) & 0xff]
  }
  return crc
}

/**
 * Port of `keyHashSlot()` from Redis `cluster.c` (`cluster.h` from 8.0; the
 * same code on every supported Redis and Valkey version).
 *
 * The hash tag is what lies between the first `{` and the first `}` after
 * it. When there is no `{`, no `}` after it, or nothing between the two, the
 * whole key is hashed; a later `{...}` never becomes the tag. So `{}{foo}`
 * hashes as the whole key, slot 2263.
 *
 * This is inlined rather than taken from the `cluster-key-slot` package. That
 * package keeps scanning after an empty `{}` and hashes `{}{foo}` as `{foo`
 * (slot 13308), a slot real Redis never uses for this key (#88). ioredis and
 * node-redis route with that package, so for such keys they reach the wrong
 * node and get a `-MOVED`, exactly as against a real cluster.
 */
export function keyHashSlot(key: Buffer): number {
  const start = key.indexOf(0x7b) // '{'
  if (start === -1) {
    return crc16(key, 0, key.length) & 0x3fff
  }

  const end = key.indexOf(0x7d, start + 1) // '}'
  if (end === -1 || end === start + 1) {
    return crc16(key, 0, key.length) & 0x3fff
  }

  return crc16(key, start + 1, end) & 0x3fff
}

export class RedisClusterTopology {
  /**
   * Each node's own `migrating_slots_to` / `importing_slots_from`: real
   * nodes keep these per node, and only the node a `CLUSTER SETSLOT`
   * reaches changes its own. Slot ownership itself is shared, so a
   * `SETSLOT NODE` is seen by every node at once (gossip made instant).
   */
  private readonly migratingSlots = new Map<string, Map<number, string>>()
  private readonly importingSlots = new Map<string, Map<number, string>>()

  constructor(public readonly nodes: readonly RedisClusterNode[] = []) {
    validateTopology(nodes)
  }

  calculateSlot(key: Buffer): number {
    return keyHashSlot(key)
  }

  /**
   * The slot every key hashes to, `null` for no keys, or `-1` when the keys
   * span more than one slot.
   */
  calculateSlotForKeys(keys: readonly Buffer[]): number | null {
    if (keys.length === 0) {
      return null
    }

    const slot = keyHashSlot(keys[0])
    for (let i = 1; i < keys.length; i++) {
      if (keyHashSlot(keys[i]) !== slot) {
        return -1
      }
    }

    return slot
  }

  getNode(id: string): RedisClusterNode | undefined {
    for (const node of this.nodes) {
      if (node.id === id) {
        return node
      }
    }

    return undefined
  }

  /**
   * Returns the master node owning the slot. Replicas are never returned —
   * MOVED must always direct the client to a master. Returns undefined if
   * the slot is unassigned.
   */
  getSlotOwner(slot: number): RedisClusterNode | undefined {
    for (const node of this.nodes) {
      if (node.role !== 'master') {
        continue
      }

      if (nodeOwnsSlot(node, slot)) {
        return node
      }
    }

    return undefined
  }

  /**
   * Returns whether the node is the master serving the slot. Replicas never
   * own slots for routing purposes — keyed commands sent directly to a
   * replica must redirect to the master via MOVED.
   */
  nodeOwnsSlot(nodeId: string, slot: number): boolean {
    const node = this.getNode(nodeId)
    return node && node.role === 'master' ? nodeOwnsSlot(node, slot) : false
  }

  /** The node `nodeId` migrates `slot` to, if it has the slot MIGRATING. */
  migratingTarget(nodeId: string, slot: number): RedisClusterNode | undefined {
    const target = this.migratingSlots.get(nodeId)?.get(slot)
    return target === undefined ? undefined : this.getNode(target)
  }

  /** The node `nodeId` imports `slot` from, if it has the slot IMPORTING. */
  importingSource(nodeId: string, slot: number): RedisClusterNode | undefined {
    const source = this.importingSlots.get(nodeId)?.get(slot)
    return source === undefined ? undefined : this.getNode(source)
  }

  /** Open (`otherId`) or close (`null`) a MIGRATING slot on `nodeId`. */
  setMigrating(nodeId: string, slot: number, otherId: string | null): void {
    setOpenSlot(this.migratingSlots, nodeId, slot, otherId)
  }

  /** Open (`otherId`) or close (`null`) an IMPORTING slot on `nodeId`. */
  setImporting(nodeId: string, slot: number, otherId: string | null): void {
    setOpenSlot(this.importingSlots, nodeId, slot, otherId)
  }

  /**
   * The slots `nodeId` has open, by ascending slot; a slot both MIGRATING
   * and IMPORTING lists MIGRATING first, as CLUSTER NODES prints them.
   */
  openSlots(nodeId: string): RedisClusterOpenSlot[] {
    const open: RedisClusterOpenSlot[] = []
    for (const [slot, other] of this.migratingSlots.get(nodeId) ?? []) {
      open.push({ slot, state: 'migrating', nodeId: other })
    }
    for (const [slot, other] of this.importingSlots.get(nodeId) ?? []) {
      open.push({ slot, state: 'importing', nodeId: other })
    }
    return open.sort(
      (a, b) =>
        a.slot - b.slot ||
        (a.state === b.state ? 0 : a.state === 'migrating' ? -1 : 1),
    )
  }

  /**
   * `CLUSTER SETSLOT <slot> NODE <id>`'s `clusterDelSlot` + `clusterAddSlot`:
   * the slot leaves its current owner's ranges and joins the master
   * `nodeId`'s. Ranges are edited in place, so a replica that shares its
   * master's `slots` array follows.
   */
  assignSlot(slot: number, nodeId: string): void {
    const target = this.getNode(nodeId)
    if (!target || target.role !== 'master') {
      throw new Error(`Cannot assign slot ${slot} to ${nodeId}`)
    }
    for (const node of this.nodes) {
      if (node.role === 'master' && node !== target) {
        if (nodeOwnsSlot(node, slot)) {
          replaceRanges(node.slots, removeSlot(node.slots, slot))
        }
      }
    }
    if (!nodeOwnsSlot(target, slot)) {
      replaceRanges(target.slots, addSlot(target.slots, slot))
    }
  }

  /**
   * Returns whether a node may serve a readonly command for the slot after the
   * client enabled Redis Cluster READONLY mode. Masters may serve their own
   * slots; replicas may serve slots owned by their configured master.
   */
  nodeCanServeReadonlySlot(nodeId: string, slot: number): boolean {
    const node = this.getNode(nodeId)
    if (!node) {
      return false
    }

    if (node.role === 'master') {
      return nodeOwnsSlot(node, slot)
    }

    if (!node.masterId) {
      return false
    }

    const master = this.getNode(node.masterId)
    return master && master.role === 'master'
      ? nodeOwnsSlot(master, slot)
      : false
  }
}

/** A slot this node has open for resharding (`CLUSTER SETSLOT`). */
export type RedisClusterOpenSlot = {
  slot: number
  /** `migrating`: keys move out to `nodeId`; `importing`: in from it. */
  state: 'migrating' | 'importing'
  nodeId: string
}

function validateTopology(nodes: readonly RedisClusterNode[]): void {
  const ids = new Set<string>()
  for (const node of nodes) {
    if (ids.has(node.id)) {
      throw new Error(`Duplicate cluster node id ${node.id}`)
    }
    ids.add(node.id)

    for (const [min, max] of node.slots) {
      if (
        !Number.isInteger(min) ||
        !Number.isInteger(max) ||
        min < 0 ||
        max >= REDIS_CLUSTER_SLOT_COUNT ||
        min > max
      ) {
        throw new Error(
          `Invalid slot range [${min}, ${max}] on node ${node.id}`,
        )
      }
    }

    if (node.role === 'replica' && !node.masterId) {
      throw new Error(`Replica node ${node.id} is missing masterId`)
    }
  }
}

function nodeOwnsSlot(node: RedisClusterNode, slot: number): boolean {
  for (const [min, max] of node.slots) {
    if (slot >= min && slot <= max) {
      return true
    }
  }

  return false
}

function setOpenSlot(
  table: Map<string, Map<number, string>>,
  nodeId: string,
  slot: number,
  otherId: string | null,
): void {
  let slots = table.get(nodeId)
  if (otherId === null) {
    slots?.delete(slot)
    return
  }
  if (!slots) {
    slots = new Map()
    table.set(nodeId, slots)
  }
  slots.set(slot, otherId)
}

function replaceRanges(
  ranges: Array<[number, number]>,
  next: Array<[number, number]>,
): void {
  ranges.splice(0, ranges.length, ...next)
}

function removeSlot(
  ranges: readonly [number, number][],
  slot: number,
): Array<[number, number]> {
  const next: Array<[number, number]> = []
  for (const [min, max] of ranges) {
    if (slot < min || slot > max) {
      next.push([min, max])
      continue
    }
    if (min < slot) next.push([min, slot - 1])
    if (slot < max) next.push([slot + 1, max])
  }
  return next
}

/** The ranges plus `slot`, sorted, with touching ranges merged. */
function addSlot(
  ranges: readonly [number, number][],
  slot: number,
): Array<[number, number]> {
  const sorted = [...ranges, [slot, slot] as [number, number]].sort(
    (a, b) => a[0] - b[0],
  )
  const next: Array<[number, number]> = []
  for (const [min, max] of sorted) {
    const last = next[next.length - 1]
    if (last && min <= last[1] + 1) {
      last[1] = Math.max(last[1], max)
    } else {
      next.push([min, max])
    }
  }
  return next
}
