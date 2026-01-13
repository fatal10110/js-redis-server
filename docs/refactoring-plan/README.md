# Redis Mock Server Refactoring Plan

## Overview

This refactoring transforms js-redis-server into a proper Redis-compatible mock server with true single-threaded execution semantics and extensible architecture.

## Goals

1. **True single-threaded execution** - Simulate Redis's single-threaded model correctly
2. **Extensible command registration** - Plugin-like architecture with minimal boilerplate
3. **Proper atomic execution** - Lua scripts and transactions execute atomically
4. **Production-ready testing tool** - Client-agnostic Redis server mock

## Current State Analysis

Based on comprehensive codebase exploration:

- **84 commands implemented** across strings, hashes, lists, sets, sorted sets, keys
- **MULTI/EXEC transactions** working with mutex-based atomicity
- **Lua scripting** functional but has race condition (acquires/releases lock per redis.call)
- **Cluster mode** basic support with slot routing
- **Command registration** manual with 5-8 touch points per command
- **No command introspection** - COMMAND is stub
- **Incomplete single-threaded semantics** - commands can interleave

## Critical Issues Identified

### 1. Lua Script Race Condition (HIGH PRIORITY)
**File:** [src/commanders/custom/commands/redis/eval.ts:67](src/commanders/custom/commands/redis/eval.ts#L67)

Each `redis.call()` from Lua acquires and releases the mutex separately, allowing other clients to execute commands between Lua calls. Real Redis holds lock for entire script.

### 2. Command Registration Boilerplate (HIGH PRIORITY)
**File:** [src/commanders/custom/commands/redis/index.ts](src/commanders/custom/commands/redis/index.ts)

Adding a command requires:
- Creating command file
- Adding to category index
- Importing in main index
- Adding to command registry
- Manually updating 3 filter Sets (readonly/multi/lua)

### 3. No Command Metadata (MEDIUM PRIORITY)
**File:** [src/commanders/custom/commands/redis/command/index.ts](src/commanders/custom/commands/redis/command/index.ts)

COMMAND implementation is stub. No centralized metadata for arity, flags, key positions.

### 4. Incomplete Single-Threaded Semantics (MEDIUM PRIORITY)
**File:** [src/core/transports/resp2/index.ts](src/core/transports/resp2/index.ts)

- No command serialization per connection
- No backpressure handling
- Commands from same connection can interleave if they await

## Implementation Phases

### Phase 1: Command Metadata System (Foundation)
Create declarative command registration with metadata schema and registry.

**Deliverable:** Metadata types, CommandRegistry class, updated Command interface

**Files:** 2 new, 1 modified, ~400 lines

**Details:** [phase-1-metadata-system.md](phase-1-metadata-system.md)

---

### Phase 2: Migrate Commands to Metadata System
Convert existing 85 commands to use new metadata-based registration.

**Deliverable:** All commands have metadata, reduced registration boilerplate

**Files:** ~85 modified command files

**Details:** [phase-2-command-migration.md](phase-2-command-migration.md)

---

### Phase 3: Fix Lua Script Atomicity
Ensure Lua scripts hold lock for entire execution, not per redis.call.

**Deliverable:** Atomic Lua script execution

**Files:** 3 modified, ~100 lines

**Details:** [phase-3-lua-atomicity.md](phase-3-lua-atomicity.md)

---

### Phase 4: Implement COMMAND Command
Build full COMMAND implementation using metadata registry.

**Deliverable:** Working COMMAND, COMMAND INFO, COMMAND COUNT

**Files:** 1 modified, ~150 lines

**Details:** [phase-4-command-introspection.md](phase-4-command-introspection.md)

---

### Phase 5: Improve Single-Threaded Semantics (Optional)
Add per-connection command queue and backpressure handling.

**Deliverable:** Proper command serialization per connection

**Files:** 1 modified, ~50 lines

**Details:** [phase-5-transport-improvements.md](phase-5-transport-improvements.md)

---

## Implementation Order

Phases should be executed sequentially:
1. Phase 1 (Foundation) - Required for all other phases
2. Phase 2 (Migration) - Can be done incrementally
3. Phase 3 (Lua Fix) - Critical bug fix
4. Phase 4 (COMMAND) - Depends on Phase 1
5. Phase 5 (Transport) - Optional enhancement

## Testing Strategy

### Per-Phase Testing
Each phase includes:
- Unit tests for new functionality
- Integration tests for affected commands
- Regression test verification

### Final Verification
- [ ] All existing tests pass: `npm test`
- [ ] Integration tests pass: `npm run test:integration:mock`
- [ ] Real Redis integration: `npm run test:integration:real`
- [ ] Lua atomicity verified with concurrency test
- [ ] COMMAND returns correct metadata for all commands
- [ ] No performance regression

## Benefits

| Benefit | Current | After Refactor |
|---------|---------|----------------|
| Command registration touch points | 5-8 | 1 |
| Lua script atomicity | ❌ Race condition | ✅ Atomic |
| Command introspection | ❌ Stub | ✅ Full COMMAND |
| Filter maintenance | Manual (3 Sets) | Automatic (metadata) |
| Single-threaded semantics | Partial | ✅ Complete |
| Extensibility | Low | High |

## Success Criteria

1. ✅ All 85 commands converted to metadata system
2. ✅ Lua scripts execute atomically (concurrent client test)
3. ✅ COMMAND returns correct Redis-compatible metadata
4. ✅ Adding new command requires single touch point
5. ✅ All existing tests pass
6. ✅ No performance degradation
7. ✅ Cluster mode continues working

## Timeline Estimate

- Phase 1: Core foundation
- Phase 2: Incremental (can parallelize command conversion)
- Phase 3: Critical fix
- Phase 4: Feature implementation
- Phase 5: Optional enhancement

Total: Can be done incrementally with Phase 1+3 being highest priority.
