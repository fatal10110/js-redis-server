import { describe, test } from 'node:test'
import assert from 'node:assert'
import { ClientSession, type RedisValue } from '../src/internal'
import { buildClusterNodes } from '../src/cluster'
import type { CompatibilitySpec } from '../src/core/compatibility/profile'

// #85 / #616: slot migration state (CLUSTER SETSLOT), ASK / TRYAGAIN and
// ASKING, following getNodeByQuery (redis 7.2.4 cluster.c:7236-7446). Every
// reply below was captured from real 2-master clusters (A owns 0-8191, B
// 8192-16383) on redis 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey 8.0.0,
// 9.0.0. `b` and `{b}...` hash to slot 3300 (A), `a` to 15495 and `foo` to
// 12182 (B).

function show(value: RedisValue): string {
  switch (value.kind) {
    case 'error':
      return `-${value.code ? `${value.code} ` : ''}${value.message}`
    case 'simple-string':
      return `+${value.value}`
    case 'bulk-string':
      return value.value === null ? 'nil' : value.value.toString()
    case 'integer':
      return `:${value.value}`
    case 'null':
    case 'null-array':
      return 'nil'
    case 'array':
      return `[${value.items.map(show).join(',')}]`
    default:
      return JSON.stringify(value)
  }
}

function createCluster(compatibility: CompatibilitySpec = 'redis-8.0') {
  const { topology, nodes } = buildClusterNodes({
    masters: 2,
    basePort: 7000,
    compatibility,
  })
  const [a, b] = nodes
  const connect = (node: (typeof nodes)[number]) => {
    const session = new ClientSession({
      server: node.state,
      executor: node.executor,
      nodeRole: node.role,
    })
    return async (...args: Array<string | Buffer>): Promise<string> => {
      const [name, ...rest] = args.map(arg => Buffer.from(arg))
      return show((await session.execute(name, rest)).value)
    }
  }
  return { topology, a, b, onA: connect(a), onB: connect(b), connect }
}

/** DUMP's binary payload of `key` on `node`. */
async function dumpOn(
  node: ReturnType<typeof buildClusterNodes>['nodes'][number],
  key: string,
): Promise<Buffer> {
  const session = new ClientSession({
    server: node.state,
    executor: node.executor,
    nodeRole: node.role,
  })
  const { value } = await session.execute(Buffer.from('DUMP'), [
    Buffer.from(key),
  ])
  assert.ok(value.kind === 'bulk-string' && value.value)
  return value.value
}

/** A migrating 3300 to B, B importing it from A, `b` and `{b}k2` on A. */
async function createResharding(compatibility?: CompatibilitySpec) {
  const cluster = createCluster(compatibility)
  const { onA, onB } = cluster
  assert.strictEqual(await onA('SET', 'b', '1'), '+OK')
  assert.strictEqual(await onA('SET', '{b}k2', '2'), '+OK')
  assert.strictEqual(
    await onB('CLUSTER', 'SETSLOT', '3300', 'IMPORTING', 'master-0'),
    '+OK',
  )
  assert.strictEqual(
    await onA('cluster', 'setslot', '3300', 'migrating', 'master-1'),
    '+OK',
  )
  return cluster
}

const ask = '-ASK 3300 127.0.0.1:7001'
const tryAgain = '-TRYAGAIN Multiple keys request during rehashing of slot'
const movedToA = '-MOVED 3300 127.0.0.1:7000'
const execAbort = '-EXECABORT Transaction discarded because of previous errors.'

describe('cluster slot migration: the MIGRATING source (#85)', () => {
  test('a missing key is ASKed for, an existing one served', async () => {
    const { onA } = await createResharding()
    const rows: Array<[string[], string]> = [
      [['GET', 'b'], '1'],
      [['SET', 'b', '2'], '+OK'],
      [['GET', '{b}missing'], ask],
      [['SET', '{b}new', 'x'], ask],
      [['DEL', '{b}missing'], ask],
      [['EXISTS', '{b}missing'], ask],
      [['LPUSH', '{b}list', '1'], ask],
      [['MSET', '{b}m1', '1', '{b}m2', '2'], ask],
      [['MGET', 'b', '{b}missing'], tryAgain],
      [['MGET', 'b', '{b}k2'], '[2,2]'],
      [['MGET', '{b}missing', '{b}missing2'], ask],
      [['MSET', 'b', '1', '{b}k2', '2'], '+OK'],
      [
        ['MGET', 'b', 'a'],
        "-CROSSSLOT Keys in request don't hash to the same slot",
      ],
      [['GET', 'a'], '-MOVED 15495 127.0.0.1:7001'],
      [['PING'], '+PONG'],
    ]
    for (const [args, expected] of rows) {
      assert.strictEqual(await onA(...args), expected, args.join(' '))
    }
  })

  test('an expired key counts as missing', async () => {
    const { onA } = await createResharding()
    assert.strictEqual(await onA('PEXPIRE', 'b', '1'), ':1')
    await new Promise(resolve => setTimeout(resolve, 5))
    assert.strictEqual(await onA('GET', 'b'), ask)
  })

  test('a script is routed by its declared keys; its own calls cannot redirect', async () => {
    const { onA } = await createResharding()
    assert.strictEqual(
      await onA('EVAL', "return redis.call('GET',KEYS[1])", '1', '{b}missing'),
      ask,
    )
    assert.strictEqual(
      await onA('EVAL', "return redis.pcall('GET','{b}missing')", '1', 'b'),
      '-ERR Script attempted to access a non local key in a cluster node',
    )
    assert.strictEqual(
      await onA('EVAL', "return redis.call('GET',KEYS[1])", '1', 'b'),
      '1',
    )
  })

  test('MULTI refuses a queued command whose key moved (redis, valkey 8)', async () => {
    const { onA } = await createResharding()
    assert.strictEqual(await onA('MULTI'), '+OK')
    assert.strictEqual(await onA('GET', '{b}missing'), ask)
    assert.strictEqual(await onA('GET', 'b'), '+QUEUED')
    assert.strictEqual(await onA('EXEC'), execAbort)

    assert.strictEqual(await onA('MULTI'), '+OK')
    assert.strictEqual(await onA('GET', 'b'), '+QUEUED')
    assert.strictEqual(await onA('GET', '{b}k2'), '+QUEUED')
    assert.strictEqual(await onA('EXEC'), '[1,2]')
  })

  test('EXEC routes its queue again: a key gone since it was queued is ASKed for', async () => {
    const { a, onA, connect } = await createResharding()
    const other = connect(a)
    assert.strictEqual(await onA('MULTI'), '+OK')
    assert.strictEqual(await onA('GET', 'b'), '+QUEUED')
    assert.strictEqual(await other('DEL', 'b'), ':1')
    assert.strictEqual(await onA('EXEC'), ask)
    // EXEC's refusal discarded the transaction.
    assert.strictEqual(await onA('EXEC'), '-ERR EXEC without MULTI')
  })

  test('commands EXEC runs are not routed again', async () => {
    const { onA } = await createResharding()
    assert.strictEqual(await onA('MULTI'), '+OK')
    assert.strictEqual(await onA('DEL', 'b'), '+QUEUED')
    assert.strictEqual(await onA('GET', 'b'), '+QUEUED')
    assert.strictEqual(await onA('EXEC'), '[:1,nil]')
  })

  test('before redis 7.0.3 any missing key is ASKed for', async () => {
    const { onA } = await createResharding('redis-6.2')
    assert.strictEqual(await onA('MGET', 'b', '{b}missing'), ask)
    assert.strictEqual(await onA('MGET', 'b', '{b}k2'), '[1,2]')
  })

  test('valkey 9.0 looks the keys up at EXEC', async () => {
    const { onA } = await createResharding('valkey-9.0')
    assert.strictEqual(await onA('MULTI'), '+OK')
    assert.strictEqual(await onA('GET', '{b}missing'), '+QUEUED')
    assert.strictEqual(await onA('EXEC'), ask)

    assert.strictEqual(await onA('MULTI'), '+OK')
    assert.strictEqual(await onA('GET', 'b'), '+QUEUED')
    assert.strictEqual(await onA('GET', '{b}missing'), '+QUEUED')
    assert.strictEqual(await onA('EXEC'), tryAgain)

    assert.strictEqual(await onA('MULTI'), '+OK')
    assert.strictEqual(await onA('GET', 'b'), '+QUEUED')
    assert.strictEqual(await onA('EXEC'), '[1]')
  })
})

describe('cluster slot migration: the IMPORTING target and ASKING (#85, #616)', () => {
  test('without ASKING the target answers MOVED to the owner', async () => {
    const { onB } = await createResharding()
    assert.strictEqual(await onB('GET', 'b'), movedToA)
    assert.strictEqual(await onB('ASKING'), '+OK')
    assert.strictEqual(await onB('GET', 'b'), 'nil')
    // The flag lasts one command.
    assert.strictEqual(await onB('GET', 'b'), movedToA)
  })

  test('ASKING is one-shot: any other command consumes it', async () => {
    const { onB } = await createResharding()
    assert.strictEqual(await onB('ASKING'), '+OK')
    assert.strictEqual(await onB('ASKING'), '+OK')
    assert.strictEqual(await onB('SET', '{b}imp', 'v'), '+OK')
    assert.strictEqual(await onB('ASKING'), '+OK')
    assert.strictEqual(await onB('PING'), '+PONG')
    assert.strictEqual(await onB('GET', '{b}imp'), movedToA)
    assert.strictEqual(await onB('ASKING'), '+OK')
    assert.strictEqual(await onB('GET', 'foo'), 'nil')
    assert.strictEqual(await onB('GET', '{b}imp'), movedToA)
    assert.strictEqual(
      await onB('ASKING', 'x'),
      "-ERR wrong number of arguments for 'asking' command",
    )
  })

  test('several distinct keys with some missing are TRYAGAIN', async () => {
    const { onB } = await createResharding()
    const asked = async (...args: string[]) => {
      assert.strictEqual(await onB('ASKING'), '+OK')
      return onB(...args)
    }
    assert.strictEqual(await asked('SET', '{b}imp', 'v'), '+OK')
    assert.strictEqual(await asked('MGET', '{b}imp', '{b}imp'), '[v,v]')
    assert.strictEqual(await asked('MGET', '{b}imp', '{b}other'), tryAgain)
    assert.strictEqual(await asked('DEL', '{b}imp', '{b}other'), tryAgain)
    assert.strictEqual(await asked('MGET', '{b}a', '{b}a'), '[nil,nil]')
  })

  test('RESTORE-ASKING is served without ASKING', async () => {
    const { a, onA, onB } = await createResharding()
    const dump = await dumpOn(a, 'b')
    assert.strictEqual(await onB('RESTORE', 'b', '0', dump), movedToA)
    assert.strictEqual(await onB('RESTORE-ASKING', 'b', '0', dump), '+OK')
    assert.strictEqual(
      await onB('RESTORE-ASKING', 'b', '0', dump),
      '-BUSYKEY Target key name already exists.',
    )
    assert.strictEqual(
      await onB('RESTORE-ASKING', '{b}x', '0', 'x'),
      '-ERR DUMP payload version or checksum are wrong',
    )
    // RESTORE-ASKING on a slot the node does not import is MOVED as usual.
    assert.strictEqual(
      await onA('RESTORE-ASKING', 'a', '0', dump),
      '-MOVED 15495 127.0.0.1:7001',
    )
  })

  test('the flag holds through MULTI until EXEC', async () => {
    const { onB } = await createResharding()
    assert.strictEqual(await onB('ASKING'), '+OK')
    assert.strictEqual(await onB('MULTI'), '+OK')
    assert.strictEqual(await onB('SET', '{b}t', '1'), '+QUEUED')
    assert.strictEqual(await onB('GET', '{b}t'), '+QUEUED')
    assert.strictEqual(await onB('EXEC'), '[+OK,1]')
    assert.strictEqual(await onB('GET', '{b}t'), movedToA)

    // ASKING queued inside MULTI sets nothing yet.
    assert.strictEqual(await onB('MULTI'), '+OK')
    assert.strictEqual(await onB('ASKING'), '+QUEUED')
    assert.strictEqual(await onB('GET', '{b}t'), movedToA)
    assert.strictEqual(await onB('EXEC'), execAbort)
  })

  test('RESET clears the flag', async () => {
    const { onB } = await createResharding()
    assert.strictEqual(await onB('ASKING'), '+OK')
    assert.strictEqual(await onB('RESET'), '+RESET')
    assert.strictEqual(await onB('GET', 'b'), movedToA)
  })
})

describe('CLUSTER SETSLOT / KEYSLOT / COUNTKEYSINSLOT / GETKEYSINSLOT (#85)', () => {
  test('SETSLOT refusals', async () => {
    const { onA, onB } = createCluster()
    const rows: Array<[typeof onA, string[], string]> = [
      [
        onA,
        ['3300', 'MIGRATING', 'nosuch'],
        "-ERR I don't know about node nosuch",
      ],
      [
        onA,
        ['15495', 'MIGRATING', 'master-1'],
        "-ERR I'm not the owner of hash slot 15495",
      ],
      [
        onA,
        ['3300', 'IMPORTING', 'nosuch'],
        "-ERR I'm already the owner of hash slot 3300",
      ],
      [
        onB,
        ['3300', 'IMPORTING', 'nosuch'],
        "-ERR I don't know about node nosuch",
      ],
      [onA, ['3300', 'NODE', 'nosuch'], '-ERR Unknown node nosuch'],
      [onA, ['16384', 'STABLE'], '-ERR Invalid or out of range slot'],
      [onA, ['x', 'STABLE'], '-ERR Invalid or out of range slot'],
      [
        onA,
        ['3300', 'BOGUS', 'x'],
        '-ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP',
      ],
      [
        onA,
        ['3300', 'MIGRATING'],
        '-ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP',
      ],
      [
        onA,
        ['3300', 'STABLE', 'x'],
        '-ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP',
      ],
      [onA, ['3300', 'STABLE'], '+OK'],
    ]
    for (const [run, args, expected] of rows) {
      assert.strictEqual(
        await run('CLUSTER', 'SETSLOT', ...args),
        expected,
        args.join(' '),
      )
    }
  })

  test('valkey 8.0+ ignores arguments after the action', async () => {
    const { onA } = createCluster('valkey-8.0')
    assert.strictEqual(
      await onA('CLUSTER', 'SETSLOT', '3300', 'STABLE', 'x'),
      '+OK',
    )
  })

  test('replicas refuse SETSLOT', async () => {
    const { nodes } = buildClusterNodes({
      masters: 2,
      replicasPerMaster: 1,
      basePort: 7100,
    })
    const replica = nodes.find(node => node.role === 'replica')!
    const session = new ClientSession({
      server: replica.state,
      executor: replica.executor,
      nodeRole: replica.role,
    })
    const result = await session.execute(
      Buffer.from('CLUSTER'),
      ['SETSLOT', '1', 'STABLE'].map(s => Buffer.from(s)),
    )
    assert.strictEqual(
      show(result.value),
      '-ERR Please use SETSLOT only with masters.',
    )
  })

  test('KEYSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT', async () => {
    const { onA, onB } = createCluster()
    await onA('MSET', 'b', '1', '{b}x', '2', '{b}y', '3')
    assert.strictEqual(await onA('CLUSTER', 'KEYSLOT', '{b}x'), ':3300')
    assert.strictEqual(await onB('CLUSTER', 'KEYSLOT', 'foo'), ':12182')
    assert.strictEqual(await onA('CLUSTER', 'COUNTKEYSINSLOT', '3300'), ':3')
    assert.strictEqual(await onB('CLUSTER', 'COUNTKEYSINSLOT', '3300'), ':0')
    assert.strictEqual(
      await onA('CLUSTER', 'COUNTKEYSINSLOT', 'x'),
      '-ERR value is not an integer or out of range',
    )
    assert.strictEqual(
      await onA('CLUSTER', 'COUNTKEYSINSLOT', '16384'),
      '-ERR Invalid slot',
    )
    assert.strictEqual(
      await onA('CLUSTER', 'COUNTKEYSINSLOT', '-1'),
      '-ERR Invalid slot',
    )
    assert.strictEqual(
      await onA('CLUSTER', 'GETKEYSINSLOT', '3300', '10'),
      '[b,{b}x,{b}y]',
    )
    assert.strictEqual(
      await onA('CLUSTER', 'GETKEYSINSLOT', '3300', '2'),
      '[b,{b}x]',
    )
    assert.strictEqual(await onA('CLUSTER', 'GETKEYSINSLOT', '3300', '0'), '[]')
    assert.strictEqual(
      await onA('CLUSTER', 'GETKEYSINSLOT', '3300', '-1'),
      '-ERR Invalid slot or number of keys',
    )
    assert.strictEqual(
      await onA('CLUSTER', 'GETKEYSINSLOT', '16384', '1'),
      '-ERR Invalid slot or number of keys',
    )
    assert.strictEqual(
      await onA('CLUSTER', 'GETKEYSINSLOT', 'x', '1'),
      '-ERR value is not an integer or out of range',
    )
    assert.strictEqual(
      await onA('CLUSTER', 'KEYSLOT'),
      "-ERR wrong number of arguments for 'cluster|keyslot' command",
    )
  })

  test('6.2 answers a wrong argument count with its syntax error', async () => {
    const { onA } = createCluster('redis-6.2')
    assert.strictEqual(
      await onA('CLUSTER', 'KEYSLOT'),
      "-ERR Unknown subcommand or wrong number of arguments for 'KEYSLOT'. Try CLUSTER HELP.",
    )
    assert.strictEqual(
      await onA('CLUSTER', 'SETSLOT', '1'),
      "-ERR Unknown subcommand or wrong number of arguments for 'SETSLOT'. Try CLUSTER HELP.",
    )
  })

  test("CLUSTER NODES marks the open slots on the node's own line", async () => {
    const { onA, onB } = await createResharding()
    const ownLine = (nodes: string, id: string) =>
      nodes.split('\n').find(line => line.startsWith(`${id} `)) ?? ''
    const fromA = await onA('CLUSTER', 'NODES')
    assert.match(
      ownLine(fromA, 'master-0'),
      / connected 0-8191 \[3300->-master-1\]$/,
    )
    assert.match(ownLine(fromA, 'master-1'), / connected 8192-16383$/)
    const fromB = await onB('CLUSTER', 'NODES')
    assert.match(
      ownLine(fromB, 'master-1'),
      / connected 8192-16383 \[3300-<-master-0\]$/,
    )
    assert.match(ownLine(fromB, 'master-0'), / connected 0-8191$/)
  })

  test('SETSLOT NODE moves the slot once its keys are gone', async () => {
    const { a, onA, onB } = await createResharding()
    assert.strictEqual(
      await onA('CLUSTER', 'SETSLOT', '3300', 'NODE', 'master-1'),
      "-ERR Can't assign hashslot 3300 to a different node while I still hold keys for this hash slot.",
    )
    // Move the keys as redis-cli --cluster reshard does without MIGRATE:
    // DUMP on the source, RESTORE-ASKING on the target, DEL on the source.
    assert.strictEqual(await onA('SET', 'b', 'moved'), '+OK')
    for (const key of ['b', '{b}k2']) {
      const dump = await dumpOn(a, key)
      assert.strictEqual(await onB('RESTORE-ASKING', key, '0', dump), '+OK')
      assert.strictEqual(await onA('DEL', key), ':1')
    }
    assert.strictEqual(
      await onA('CLUSTER', 'GETKEYSINSLOT', '3300', '10'),
      '[]',
    )
    assert.strictEqual(await onA('GET', 'b'), ask)
    assert.strictEqual(
      await onB('CLUSTER', 'SETSLOT', '3300', 'NODE', 'master-1'),
      '+OK',
    )
    assert.strictEqual(
      await onA('CLUSTER', 'SETSLOT', '3300', 'NODE', 'master-1'),
      '+OK',
    )
    assert.strictEqual(await onA('GET', 'b'), '-MOVED 3300 127.0.0.1:7001')
    assert.strictEqual(await onB('GET', 'b'), 'moved')
    const nodes = await onA('CLUSTER', 'NODES')
    assert.match(nodes, /master-0 [^\n]* connected 0-3299 3301-8191\n/)
    assert.match(nodes, /master-1 [^\n]* connected 3300 8192-16383\n/)
    assert.strictEqual(
      await onA('CLUSTER', 'SLOTS'),
      '[[:0,:3299,[127.0.0.1,:7000,master-0,[]]],[:3300,:3300,[127.0.0.1,:7001,master-1,[]]],[:3301,:8191,[127.0.0.1,:7000,master-0,[]]],[:8192,:16383,[127.0.0.1,:7001,master-1,[]]]]',
    )
  })
})
