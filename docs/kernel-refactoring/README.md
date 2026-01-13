# Redis Kernel Architecture Plan

## Overview

This architectural plan proposes a fundamental shift from the current "Mutex-per-command" model to a **"Redis Kernel"** model. This design mimics the real Redis event loop in Node.js, ensuring strict serialization of commands, atomicity without locks, and complete decoupling of the transport layer (RESP) from the execution logic.

## Core Concepts

1.  **The Kernel (Sequencer):** A single-threaded job queue that executes commands one by one. No race conditions are possible by design.
2.  **Schema-First Design:** Commands are defined by abstract schemas, not code. This allows validation to be decoupled from the wire protocol (RESP).
3.  **Reactive Store:** The data store emits events, enabling `WATCH`, `MULTI`, and Client-Side Caching to work via the Observer pattern.
4.  **Transport Agnostic:** The core server does not know about RESP. It accepts `CommandJobs` and returns results. RESP is just one of many possible adapters.

## Roadmap

### Phase 1: The Kernel & Sequencer

**Goal:** Establish the single-threaded execution loop.

- Implement `RedisKernel` class.
- Implement `CommandJob` interface.
- Remove all `Mutex` usage.

### Phase 2: Protocol-Agnostic Schema Registry

**Goal:** Define commands using abstract types, decoupling validation from RESP.

- Create a Zod-like schema builder for Redis arguments.
- Implement `CommandRegistry`.
- Ensure schemas support different input parsers (RESP, JSON, etc.).

### Phase 3: Reactive Data Store

**Goal:** Support transactions and blocking operations via events.

- Refactor `DB` to extend `EventEmitter`.
- Implement `WATCH` using event listeners.
- Implement `BLPOP` using event listeners.

### Phase 4: Transport Adapters

**Goal:** Plug the existing RESP parser into the new Kernel.

- Create `Transport` interface.
- Implement `RespAdapter`.
- Implement `ConnectionState` management.

## Benefits vs Current Architecture

| Feature         | Current (Refactoring Plan)    | Kernel Architecture       |
| :-------------- | :---------------------------- | :------------------------ |
| **Concurrency** | Patchy `Mutex` locks          | **Guaranteed Serialized** |
| **Atomicity**   | Developer discipline required | **By Design**             |
| **Protocol**    | Tightly coupled to RESP       | **Protocol Agnostic**     |
| **Validation**  | Manual parsing in commands    | **Declarative Schema**    |
| **Testing**     | Requires Mock Sockets         | **Test Kernel directly**  |
