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
