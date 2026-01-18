import { EventEmitter } from 'node:events'
import {
  ReactiveDB,
  ReactiveStoreEvent,
} from '../src/core/transports/reactive-db'

/**
 * Mock implementation of ReactiveDB for testing.
 * Provides a simple event emitter without actual data storage.
 */
export class MockDB extends EventEmitter implements ReactiveDB {
  constructor() {
    super()
  }

  // EventEmitter already provides on, once, and off methods
  // TypeScript will validate that they match the ReactiveDB interface
}

/**
 * Factory function to create a MockDB instance for tests
 */
export function createMockDB(): ReactiveDB {
  return new MockDB()
}
