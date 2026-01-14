import { test, describe } from 'node:test'
import assert from 'node:assert'
import {
  ClusterRouter,
  ClusterState,
  createClusterState,
} from '../src/commanders/custom/cluster-router'
import { SlotValidator } from '../src/commanders/custom/slot-validation'
import {
  ClusterNormalState,
  ClusterTransactionState,
  ClusterCommandValidator,
} from '../src/core/transports/cluster-session-state'
import { RegistryCommandValidator } from '../src/core/transports/command-validator'
import { CorssSlot, MovedError } from '../src/core/errors'
import type {
  Command,
  CommandResult,
  DiscoveryNode,
  DiscoveryService,
} from '../src/types'
import type { CommandMetadata } from '../src/commanders/custom/commands/metadata'

// Mock transport for testing
class MockTransport {
  public readonly responses: unknown[] = []

  write(data: unknown): void {
    this.responses.push(data)
  }
}

// Mock command class for testing
class MockCommand implements Command {
  constructor(
    public readonly metadata: CommandMetadata,
    private readonly keyPositions: number[] = [],
  ) {}

  getKeys(_rawCmd: Buffer, args: Buffer[]): Buffer[] {
    return this.keyPositions.map(i => args[i]).filter(Boolean)
  }

  run(
    _rawCmd: Buffer,
    _args: Buffer[],
    _signal: AbortSignal,
  ): Promise<CommandResult> {
    return Promise.resolve({ response: 'OK' })
  }
}

// Create mock discovery service
function createMockDiscoveryService(
  localSlots: Array<[number, number]>,
  nodeMap: Map<number, { host: string; port: number }>,
): DiscoveryService {
  const localNode: DiscoveryNode = {
    id: 'local-node',
    host: '127.0.0.1',
    port: 7000,
    slots: localSlots,
  }

  return {
    getAll: () => [localNode],
    isMaster: () => true,
    getMaster: () => localNode,
    getById: () => localNode,
    getBySlot: (slot: number) => {
      const node = nodeMap.get(slot)
      if (node) {
        return { id: 'remote', host: node.host, port: node.port, slots: [] }
      }
      return localNode
    },
  }
}

describe('ClusterRouter', () => {
  describe('createClusterState', () => {
    test('should correctly identify local slots', () => {
      const localSlots: Array<[number, number]> = [
        [0, 5460],
        [10923, 16383],
      ]
      const nodeMap = new Map<number, { host: string; port: number }>()
      const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
      const myself = discoveryService.getById('local-node')

      const clusterState = createClusterState(discoveryService, myself)

      // Slot 0 is local
      assert.strictEqual(clusterState.isLocal(0), true)
      // Slot 5460 is local
      assert.strictEqual(clusterState.isLocal(5460), true)
      // Slot 5461 is not local (between ranges)
      assert.strictEqual(clusterState.isLocal(5461), false)
      // Slot 10923 is local
      assert.strictEqual(clusterState.isLocal(10923), true)
      // Slot 16383 is local
      assert.strictEqual(clusterState.isLocal(16383), true)
    })
  })

  describe('validateSlot', () => {
    test('should return null for commands with no keys', () => {
      const localSlots: Array<[number, number]> = [[0, 16383]]
      const nodeMap = new Map<number, { host: string; port: number }>()
      const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
      const myself = discoveryService.getById('local-node')

      const clusterState = createClusterState(discoveryService, myself)
      const router = new ClusterRouter(null as any, clusterState)

      const pingCommand = new MockCommand(
        {
          name: 'ping',
          arity: 1,
          flags: {},
          firstKey: -1,
          lastKey: -1,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [], // No key positions
      )

      const slot = router.validateSlot(pingCommand, Buffer.from('PING'), [])

      assert.strictEqual(slot, null)
    })

    test('should return slot for single-key command', () => {
      const localSlots: Array<[number, number]> = [[0, 16383]]
      const nodeMap = new Map<number, { host: string; port: number }>()
      const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
      const myself = discoveryService.getById('local-node')

      const clusterState = createClusterState(discoveryService, myself)
      const router = new ClusterRouter(null as any, clusterState)

      const getCommand = new MockCommand(
        {
          name: 'get',
          arity: 2,
          flags: {},
          firstKey: 0,
          lastKey: 0,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [0], // Key at position 0
      )

      const slot = router.validateSlot(getCommand, Buffer.from('GET'), [
        Buffer.from('mykey'),
      ])

      assert.strictEqual(typeof slot, 'number')
      assert.ok(slot! >= 0 && slot! <= 16383)
    })

    test('should throw CorssSlot for keys in different slots', () => {
      const localSlots: Array<[number, number]> = [[0, 16383]]
      const nodeMap = new Map<number, { host: string; port: number }>()
      const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
      const myself = discoveryService.getById('local-node')

      const clusterState = createClusterState(discoveryService, myself)
      const router = new ClusterRouter(null as any, clusterState)

      const mgetCommand = new MockCommand(
        {
          name: 'mget',
          arity: -2,
          flags: {},
          firstKey: 0,
          lastKey: -1,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [0, 1], // Keys at positions 0 and 1
      )

      // These keys hash to different slots
      assert.throws(
        () =>
          router.validateSlot(mgetCommand, Buffer.from('MGET'), [
            Buffer.from('key1'),
            Buffer.from('key2'),
          ]),
        CorssSlot,
      )
    })

    test('should accept keys in same slot using hash tags', () => {
      const localSlots: Array<[number, number]> = [[0, 16383]]
      const nodeMap = new Map<number, { host: string; port: number }>()
      const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
      const myself = discoveryService.getById('local-node')

      const clusterState = createClusterState(discoveryService, myself)
      const router = new ClusterRouter(null as any, clusterState)

      const mgetCommand = new MockCommand(
        {
          name: 'mget',
          arity: -2,
          flags: {},
          firstKey: 0,
          lastKey: -1,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [0, 1],
      )

      // Using hash tags to force same slot
      const slot = router.validateSlot(mgetCommand, Buffer.from('MGET'), [
        Buffer.from('{user}:name'),
        Buffer.from('{user}:email'),
      ])

      assert.strictEqual(typeof slot, 'number')
    })

    test('should throw MovedError for non-local slot', () => {
      const localSlots: Array<[number, number]> = [[0, 5460]]
      const nodeMap = new Map<number, { host: string; port: number }>()
      // Slot 12539 (where "foo" hashes) maps to remote node
      nodeMap.set(12182, { host: '192.168.1.2', port: 7001 })

      const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
      const myself = discoveryService.getById('local-node')

      const clusterState = createClusterState(discoveryService, myself)
      const router = new ClusterRouter(null as any, clusterState)

      const getCommand = new MockCommand(
        {
          name: 'get',
          arity: 2,
          flags: {},
          firstKey: 0,
          lastKey: 0,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [0],
      )

      // "foo" hashes to slot 12182 which is not in our range
      assert.throws(
        () =>
          router.validateSlot(getCommand, Buffer.from('GET'), [
            Buffer.from('foo'),
          ]),
        MovedError,
      )
    })

    test('should enforce required slot constraint', () => {
      const localSlots: Array<[number, number]> = [[0, 16383]]
      const nodeMap = new Map<number, { host: string; port: number }>()
      const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
      const myself = discoveryService.getById('local-node')

      const clusterState = createClusterState(discoveryService, myself)
      const router = new ClusterRouter(null as any, clusterState)

      const getCommand = new MockCommand(
        {
          name: 'get',
          arity: 2,
          flags: {},
          firstKey: 0,
          lastKey: 0,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [0],
      )

      // First call establishes a slot
      const slot1 = router.validateSlot(getCommand, Buffer.from('GET'), [
        Buffer.from('{test}:key1'),
      ])

      // Second call with same slot constraint should pass
      const slot2 = router.validateSlot(
        getCommand,
        Buffer.from('GET'),
        [Buffer.from('{test}:key2')],
        slot1!,
      )

      assert.strictEqual(slot1, slot2)

      // Call with different key violating constraint should throw
      assert.throws(
        () =>
          router.validateSlot(
            getCommand,
            Buffer.from('GET'),
            [Buffer.from('differentkey')], // Different slot
            slot1!,
          ),
        CorssSlot,
      )
    })
  })
})

describe('SlotValidator', () => {
  test('should validate command using underlying router', () => {
    const localSlots: Array<[number, number]> = [[0, 16383]]
    const nodeMap = new Map<number, { host: string; port: number }>()
    const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
    const myself = discoveryService.getById('local-node')

    const validator = new SlotValidator(discoveryService, myself)

    const getCommand = new MockCommand(
      {
        name: 'get',
        arity: 2,
        flags: {},
        firstKey: 0,
        lastKey: 0,
        keyStep: 1,
        categories: [],
      },
      [0],
    )

    // Should not throw for local slot
    assert.doesNotThrow(() =>
      validator.validate(getCommand, Buffer.from('GET'), [
        Buffer.from('mykey'),
      ]),
    )
  })

  test('should expose validateSlot method for transactions', () => {
    const localSlots: Array<[number, number]> = [[0, 16383]]
    const nodeMap = new Map<number, { host: string; port: number }>()
    const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
    const myself = discoveryService.getById('local-node')

    const validator = new SlotValidator(discoveryService, myself)

    const getCommand = new MockCommand(
      {
        name: 'get',
        arity: 2,
        flags: {},
        firstKey: 0,
        lastKey: 0,
        keyStep: 1,
        categories: [],
      },
      [0],
    )

    const slot = validator.validateSlot(getCommand, Buffer.from('GET'), [
      Buffer.from('mykey'),
    ])

    assert.strictEqual(typeof slot, 'number')
  })
})

describe('ClusterTransactionState', () => {
  // Helper to create cluster-aware state machine
  function createClusterStateMachine(
    localSlots: Array<[number, number]> = [[0, 16383]],
  ) {
    const nodeMap = new Map<number, { host: string; port: number }>()
    const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
    const myself = discoveryService.getById('local-node')

    const commands: Record<string, Command> = {
      get: new MockCommand(
        {
          name: 'get',
          arity: 2,
          flags: {},
          firstKey: 0,
          lastKey: 0,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [0],
      ),
      set: new MockCommand(
        {
          name: 'set',
          arity: -3,
          flags: {},
          firstKey: 0,
          lastKey: 0,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [0],
      ),
      mget: new MockCommand(
        {
          name: 'mget',
          arity: -2,
          flags: {},
          firstKey: 0,
          lastKey: -1,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [0, 1],
      ),
      ping: new MockCommand(
        {
          name: 'ping',
          arity: 1,
          flags: {},
          firstKey: -1,
          lastKey: -1,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [],
      ),
    }

    const baseValidator = new RegistryCommandValidator(commands)
    const slotValidator = new SlotValidator(discoveryService, myself)
    const clusterValidator = new ClusterCommandValidator(
      baseValidator,
      commands,
      slotValidator,
    )

    const normalState = new ClusterNormalState(baseValidator, clusterValidator)

    return { normalState, commands }
  }

  test('MULTI should transition to ClusterTransactionState', () => {
    const { normalState } = createClusterStateMachine()
    const transport = new MockTransport()

    const transition = normalState.handle(
      transport as any,
      Buffer.from('MULTI'),
      [],
    )

    assert.strictEqual(transport.responses[0], 'OK')
    assert.ok(transition.nextState instanceof ClusterTransactionState)
    assert.strictEqual(transition.executeCommand, undefined)
  })

  test('should buffer commands in transaction', () => {
    const { normalState } = createClusterStateMachine()
    const transport = new MockTransport()

    // Enter transaction
    let transition = normalState.handle(
      transport as any,
      Buffer.from('MULTI'),
      [],
    )

    const transactionState = transition.nextState

    // Queue a command
    transition = transactionState.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('{test}:key'),
      Buffer.from('value'),
    ])

    assert.strictEqual(transport.responses[1], 'QUEUED')
    assert.strictEqual(transition.nextState, transactionState)
    assert.strictEqual(transition.executeCommand, undefined)
  })

  test('EXEC should return buffered commands', () => {
    const { normalState } = createClusterStateMachine()
    const transport = new MockTransport()

    // Enter transaction
    let transition = normalState.handle(
      transport as any,
      Buffer.from('MULTI'),
      [],
    )

    let state = transition.nextState

    // Queue commands
    transition = state.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('{test}:key'),
      Buffer.from('value'),
    ])
    state = transition.nextState

    transition = state.handle(transport as any, Buffer.from('GET'), [
      Buffer.from('{test}:key'),
    ])
    state = transition.nextState

    // Execute
    transition = state.handle(transport as any, Buffer.from('EXEC'), [])

    assert.ok(transition.executeBatch)
    assert.strictEqual(transition.executeBatch.length, 2)
    assert.ok(transition.nextState instanceof ClusterNormalState)
  })

  test('should pin slot on first command with keys', () => {
    const { normalState } = createClusterStateMachine()
    const transport = new MockTransport()

    // Enter transaction
    let transition = normalState.handle(
      transport as any,
      Buffer.from('MULTI'),
      [],
    )
    let state = transition.nextState

    // First command pins the slot
    transition = state.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('{user}:name'),
      Buffer.from('Alice'),
    ])

    assert.strictEqual(transport.responses[1], 'QUEUED')
    state = transition.nextState

    // Second command with same slot should succeed
    transition = state.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('{user}:email'),
      Buffer.from('alice@example.com'),
    ])

    assert.strictEqual(transport.responses[2], 'QUEUED')
  })

  test('should reject commands with different slot after pinning', () => {
    const { normalState } = createClusterStateMachine()
    const transport = new MockTransport()

    // Enter transaction
    let transition = normalState.handle(
      transport as any,
      Buffer.from('MULTI'),
      [],
    )
    let state = transition.nextState

    // First command pins the slot
    transition = state.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('{user}:name'),
      Buffer.from('Alice'),
    ])
    state = transition.nextState

    // Second command with different slot should fail with CROSSSLOT
    transition = state.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('differentkey'),
      Buffer.from('value'),
    ])

    // Should receive CROSSSLOT error
    assert.ok(transport.responses[2] instanceof CorssSlot)
  })

  test('should allow keyless commands in transaction', () => {
    const { normalState } = createClusterStateMachine()
    const transport = new MockTransport()

    // Enter transaction
    let transition = normalState.handle(
      transport as any,
      Buffer.from('MULTI'),
      [],
    )
    let state = transition.nextState

    // Keyless command (PING) should not pin slot
    transition = state.handle(transport as any, Buffer.from('PING'), [])

    assert.strictEqual(transport.responses[1], 'QUEUED')
    state = transition.nextState

    // First command with keys pins the slot
    transition = state.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('{test}:key'),
      Buffer.from('value'),
    ])

    assert.strictEqual(transport.responses[2], 'QUEUED')
  })

  test('DISCARD should abort transaction and return to normal state', () => {
    const { normalState } = createClusterStateMachine()
    const transport = new MockTransport()

    // Enter transaction
    let transition = normalState.handle(
      transport as any,
      Buffer.from('MULTI'),
      [],
    )
    let state = transition.nextState

    // Queue a command
    transition = state.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('{test}:key'),
      Buffer.from('value'),
    ])
    state = transition.nextState

    // Discard
    transition = state.handle(transport as any, Buffer.from('DISCARD'), [])

    assert.strictEqual(transport.responses[2], 'OK')
    assert.ok(transition.nextState instanceof ClusterNormalState)
    assert.strictEqual(transition.executeBatch, undefined)
  })

  test('nested MULTI should return error', () => {
    const { normalState } = createClusterStateMachine()
    const transport = new MockTransport()

    // Enter transaction
    let transition = normalState.handle(
      transport as any,
      Buffer.from('MULTI'),
      [],
    )
    let state = transition.nextState

    // Try nested MULTI
    transition = state.handle(transport as any, Buffer.from('MULTI'), [])

    assert.ok(transport.responses[1] instanceof Error)
    assert.ok((transport.responses[1] as Error).message.includes('nested'))
    assert.strictEqual(transition.nextState, state)
  })

  test('cross-slot error should mark transaction for discard', () => {
    const { normalState } = createClusterStateMachine()
    const transport = new MockTransport()

    // Enter transaction
    let transition = normalState.handle(
      transport as any,
      Buffer.from('MULTI'),
      [],
    )
    let state = transition.nextState

    // First command pins slot
    transition = state.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('{user}:name'),
      Buffer.from('Alice'),
    ])
    state = transition.nextState

    // Second command with different slot should fail
    transition = state.handle(transport as any, Buffer.from('SET'), [
      Buffer.from('otherkey'),
      Buffer.from('value'),
    ])
    state = transition.nextState

    // EXEC should abort
    transition = state.handle(transport as any, Buffer.from('EXEC'), [])

    assert.ok(transport.responses[3] instanceof Error)
    assert.ok((transport.responses[3] as Error).message.includes('EXECABORT'))
  })
})

describe('ClusterNormalState', () => {
  test('should pass through non-MULTI commands for execution', () => {
    const localSlots: Array<[number, number]> = [[0, 16383]]
    const nodeMap = new Map<number, { host: string; port: number }>()
    const discoveryService = createMockDiscoveryService(localSlots, nodeMap)
    const myself = discoveryService.getById('local-node')

    const commands: Record<string, Command> = {
      get: new MockCommand(
        {
          name: 'get',
          arity: 2,
          flags: {},
          firstKey: 0,
          lastKey: 0,
          keyStep: 1,
          limit: 0,
          categories: [],
        },
        [0],
      ),
    }

    const baseValidator = new RegistryCommandValidator(commands)
    const slotValidator = new SlotValidator(discoveryService, myself)
    const clusterValidator = new ClusterCommandValidator(
      baseValidator,
      commands,
      slotValidator,
    )

    const normalState = new ClusterNormalState(baseValidator, clusterValidator)
    const transport = new MockTransport()

    const transition = normalState.handle(
      transport as any,
      Buffer.from('GET'),
      [Buffer.from('mykey')],
    )

    assert.ok(transition.executeCommand)
    assert.strictEqual(transition.executeCommand.command.toString(), 'GET')
    assert.strictEqual(transition.nextState, normalState)
  })
})
