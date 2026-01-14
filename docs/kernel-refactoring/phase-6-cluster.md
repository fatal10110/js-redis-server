# Phase 6: Cluster & Routing

## Objective

Implement a generic, schema-driven Cluster Router. This replaces manual cross-slot checks inside individual commands with a centralized routing layer that leverages the Schema Registry.

## The Problem

Currently, cluster logic (slot calculation, MOVED errors, cross-slot validation) is often coupled with command implementations or requires manual argument parsing. This leads to code duplication and maintenance burden.

## The Solution: Schema-Driven Router

We introduce a **Router** layer between the `Session` and the `Kernel`.

### 6.1 The Architecture

```
[ Session ] -> [ Router ] -> [ Kernel ]
```

### 6.2 The Router Logic

The Router uses the **Command Registry** (Phase 2) to inspect incoming commands without knowing their specific implementation.

```typescript
class ClusterRouter {
  constructor(
    private kernel: RedisKernel,
    private registry: CommandRegistry,
    private clusterState: ClusterState,
  ) {}

  async route(command: string, args: unknown[]): Promise<any> {
    // 1. Get Schema
    const schema = this.registry.get(command)

    // 2. Extract Keys (Generic extraction based on Schema)
    // This solves the "command args check" problem generically
    const keys = this.extractKeys(schema, args)

    // 3. Calculate Slots
    const slots = new Set(keys.map(k => this.calculateSlot(k)))

    // 4. Cross-Slot Validation
    // NOTE: Redis requires all keys to map to the EXACT SAME SLOT.
    // It is NOT sufficient for them to be on the same node.
    if (slots.size > 1) {
      throw new Error("CROSSSLOT Keys in request don't hash to the same slot")
    }

    // 5. Topology Check
    const slot = slots.values().next().value
    if (slot !== undefined && !this.clusterState.isLocal(slot)) {
      const owner = this.clusterState.getOwner(slot)
      throw new Error(`MOVED ${slot} ${owner.ip}:${owner.port}`)
    }

    // 6. Forward to Kernel
    return this.kernel.execute({ command, args })
  }

  /**
   * Helper to validate a single command against a specific slot constraint.
   * Used by TransactionState to ensure all queued commands match the pinned slot.
   */
  validateSlot(
    command: string,
    args: unknown[],
    requiredSlot?: number,
  ): number | null {
    const schema = this.registry.get(command)
    const keys = this.extractKeys(schema, args)

    if (keys.length === 0) return null

    const slots = new Set(keys.map(k => this.calculateSlot(k)))
    if (slots.size > 1) {
      throw new Error("CROSSSLOT Keys in request don't hash to the same slot")
    }

    const slot = slots.values().next().value

    // Check against pinned slot (for Transactions)
    if (requiredSlot !== undefined && slot !== requiredSlot) {
      throw new Error("CROSSSLOT Keys in request don't hash to the same slot")
    }

    // Check topology
    if (!this.clusterState.isLocal(slot)) {
      const owner = this.clusterState.getOwner(slot)
      throw new Error(`MOVED ${slot} ${owner.ip}:${owner.port}`)
    }

    return slot
  }
}
```

### 6.4 Handling Transactions (MULTI/EXEC)

In Cluster mode, Redis requires that **all keys in a transaction belong to the same hash slot**.

The `TransactionState` (from Phase 5) must be enhanced to support this. When Cluster Mode is enabled, the Session should use a `ClusterTransactionState` (or inject the validation logic).

1.  **Slot Pinning:** The first command with keys "pins" the transaction to a specific slot.
2.  **Validation:** Every subsequent command must be checked against this pinned slot using `router.validateSlot`.

```typescript
class TransactionState {
  private pinnedSlot: number | undefined

  async handle(command: string, args: unknown[]) {
    // ... EXEC/DISCARD handling ...

    // Validate Slot before buffering
    // If pinnedSlot is undefined, this call will define it.
    // If pinnedSlot is defined, this call will enforce it.
    const slot = this.router.validateSlot(command, args, this.pinnedSlot)

    if (this.pinnedSlot === undefined && slot !== null) {
      this.pinnedSlot = slot
    }

    this.buffer.push({ command, args })
    return 'QUEUED'
  }
}
```

```

### 6.3 Benefits

1.  **Zero Boilerplate:** New commands support Cluster automatically if their schema defines keys correctly.
2.  **Centralized Logic:** MOVED/ASK/CROSSSLOT logic exists in one place.
3.  **Performance:** Non-cluster mode simply bypasses the slot check steps while keeping the same pipeline.
```
