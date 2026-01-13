# Phase 3: Reactive Data Store

## Objective

Make the data store "Reactive" using the Observer pattern. This allows complex Redis features like `WATCH` (Optimistic Locking) and `BLPOP` (Blocking Operations) to be implemented cleanly without polling or complex state machines.

## The Problem

Currently, `WATCH` requires checking a version number or flag on every write. `BLPOP` requires a timeout loop or a callback hook inserted into the `LPUSH` command logic.

## The Solution: Event-Driven Store

The `DataStore` extends `EventEmitter`. Every write operation emits an event.

### 3.1 The Store Interface

```typescript
type StoreEvent =
  | { type: 'set'; key: string; value: any }
  | { type: 'del'; key: string }
  | { type: 'push'; key: string; value: any }

class ReactiveStore extends EventEmitter {
  private data = new Map<string, any>()

  set(key: string, value: any) {
    this.data.set(key, value)
    this.emit('change', { type: 'set', key, value })
    this.emit(`key:${key}`, { type: 'set', value })
  }
}
```

### 3.2 Implementing WATCH

`WATCH` simply subscribes to the key's event.

```typescript
// Inside Transaction Context
watch(key: string) {
  this.store.once(`key:${key}`, () => {
    this.transaction.abort();
  });
}
```

### 3.3 Implementing BLPOP

`BLPOP` suspends the `CommandJob` (doesn't resolve the promise) and waits for an event.

_Note: Since the Kernel is single-threaded, "suspending" means we must park the job and allow the Kernel to process other jobs. This requires a slight modification to the Kernel to support "Async/Suspended" jobs._
