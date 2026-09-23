import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  HINT,
  isAddressless,
  parseClusterNodes,
  planCluster,
  replicaMasterProblem,
} from '../scripts/flush-redis-topology'

// `clean:redis` (scripts/flush-redis.ts) decides what to flush from CLUSTER
// NODES. These fixtures are what real Redis 7 prints for the docker-compose
// cluster (masters 30000-30002, replicas 30003-30005), so the decisions are
// tested without breaking a shared cluster (#458).

const id = (c: string) => c.repeat(40)
const M1 = id('a')
const M2 = id('b')
const M3 = id('c')
const R1 = id('d')
const R2 = id('e')
const R3 = id('f')
const HANDSHAKE = id('9')
const NOADDR = id('8')

function line(
  nodeId: string,
  address: string,
  flags: string,
  master = '-',
  ...slots: string[]
): string {
  return [
    nodeId,
    address,
    flags,
    master,
    '0',
    '1700000000000',
    '1',
    'connected',
    ...slots,
  ].join(' ')
}

/** The healthy cluster as seen from `seedPort`. */
function healthyView(seedPort: number): string[] {
  const nodes: [string, number, string, string, string[]][] = [
    [M1, 30000, 'master', '-', ['0-5460']],
    [M2, 30001, 'master', '-', ['5461-10922']],
    [M3, 30002, 'master', '-', ['10923-16383']],
    [R1, 30003, 'slave', M1, []],
    [R2, 30004, 'slave', M2, []],
    [R3, 30005, 'slave', M3, []],
  ]
  return nodes.map(([nodeId, port, flags, master, slots]) =>
    line(
      nodeId,
      `127.0.0.1:${port}@${port + 10000}`,
      port === seedPort ? `myself,${flags}` : flags,
      master,
      ...slots,
    ),
  )
}

const HANDSHAKE_LINE = [
  HANDSHAKE,
  '127.0.0.1:7999@17999',
  'handshake',
  '-',
  '1700000000000',
  '0',
  '0',
  'disconnected',
].join(' ')

const NOADDR_LINE = [
  NOADDR,
  ':0@0',
  'master,noaddr',
  '-',
  '1700000000000',
  '1700000000000',
  '0',
  'disconnected',
].join(' ')

const view = (lines: string[]) => parseClusterNodes(lines.join('\n') + '\n')
const ids = (nodes: { id: string }[]) => nodes.map(n => n.id).sort()

describe('clean:redis cluster topology (#458)', () => {
  test('parses master, slots, and replica-of from CLUSTER NODES', () => {
    const nodes = view([
      ...healthyView(30000),
      line(
        id('7'),
        '127.0.0.1:30006@40006,host-7',
        'master',
        '-',
        '[5461->-' + M2 + ']',
      ),
    ])
    const byId = new Map(nodes.map(n => [n.id, n]))

    assert.deepStrictEqual(byId.get(M1), {
      id: M1,
      host: '127.0.0.1',
      port: 30000,
      flags: ['myself', 'master'],
      masterId: null,
      ownsSlots: true,
    })
    assert.strictEqual(byId.get(R1)?.masterId, M1)
    assert.strictEqual(byId.get(R1)?.ownsSlots, false)
    // A migration marker alone is not ownership.
    assert.strictEqual(byId.get(id('7'))?.ownsSlots, false)
    assert.strictEqual(byId.get(id('7'))?.port, 30006)
  })

  test('a healthy cluster covers every other master and replica', () => {
    const plan = planCluster([view(healthyView(30000))])
    assert.deepStrictEqual([...plan.seedIds], [M1])
    assert.deepStrictEqual(ids(plan.members), [M2, M3, R1, R2, R3].sort())
    assert.deepStrictEqual(plan.skipped, [])
  })

  test('a handshake node is skipped, and the plan does not depend on the seed', () => {
    // CLUSTER MEET to a dead port: only the node that issued it lists the
    // handshake entry. Seed 30000 sees it, seed 30001 does not.
    const withHandshake = view([...healthyView(30000), HANDSHAKE_LINE])
    const withoutHandshake = view(healthyView(30001))

    const fromA = planCluster([withHandshake])
    const fromB = planCluster([withoutHandshake])

    assert.deepStrictEqual(ids(fromA.skipped), [HANDSHAKE])
    assert.ok(!ids(fromA.members).includes(HANDSHAKE))
    // Same slot-holding nodes either way, seed aside.
    const all = [M1, M2, M3, R1, R2, R3].sort()
    assert.deepStrictEqual(ids([...fromA.members, { id: M1 }]), all)
    assert.deepStrictEqual(ids([...fromB.members, { id: M2 }]), all)

    const both = planCluster([withHandshake, withoutHandshake])
    assert.deepStrictEqual(ids(both.members), [M3, R1, R2, R3].sort())
    assert.deepStrictEqual(ids(both.skipped), [HANDSHAKE])
  })

  test('a noaddr node that owns no slots is skipped', () => {
    const plan = planCluster([view([...healthyView(30000), NOADDR_LINE])])
    assert.deepStrictEqual(ids(plan.skipped), [NOADDR])
    assert.ok(plan.members.every(node => !isAddressless(node)))
  })

  test('a noaddr master that owns slots is still covered (and addressless)', () => {
    const lines = healthyView(30000).filter(l => !l.startsWith(M3))
    lines.push(line(M3, ':0@0', 'master,fail,noaddr', '-', '10923-16383'))
    const plan = planCluster([view(lines)])
    const m3 = plan.members.find(node => node.id === M3)
    assert.ok(m3, 'slot-owning master must be covered')
    assert.ok(isAddressless(m3))
    // Its replica is still covered too.
    assert.ok(ids(plan.members).includes(R3))
  })

  test('a master without slots, and its replicas, are skipped', () => {
    const empty = id('7')
    const emptyReplica = id('6')
    const plan = planCluster([
      view([
        ...healthyView(30000),
        line(empty, '127.0.0.1:30006@40006', 'master'),
        line(emptyReplica, '127.0.0.1:30007@40007', 'slave', empty),
      ]),
    ])
    assert.deepStrictEqual(ids(plan.skipped), [empty, emptyReplica].sort())
  })

  test('views merge: one seed seeing a node own slots is enough to cover it', () => {
    // A stale view still lists M3 without slots; another seed knows better.
    const stale = healthyView(30000).map(l =>
      l.startsWith(M3) ? line(M3, '127.0.0.1:30002@40002', 'master') : l,
    )
    const fresh = healthyView(30001)
    const plan = planCluster([view(stale), view(fresh)])
    assert.deepStrictEqual(ids(plan.members), [M3, R1, R2, R3].sort())
    assert.deepStrictEqual(plan.skipped, [])
  })

  test('views merge: the report with a usable address wins', () => {
    const noaddr = healthyView(30000).map(l =>
      l.startsWith(R3) ? line(R3, ':0@0', 'slave,noaddr', M3) : l,
    )
    const plan = planCluster([view(noaddr), view(healthyView(30001))])
    const r3 = plan.members.find(node => node.id === R3)
    assert.strictEqual(r3?.port, 30005)
    assert.ok(r3 && !isAddressless(r3))
  })
})

describe('clean:redis replica attribution (#458)', () => {
  const flushed = new Set([30000, 30001])

  test('a replica of a flushed master is fine', () => {
    assert.strictEqual(replicaMasterProblem(30000, flushed, new Map()), null)
  })

  test('a replica of a failed master refers back to the master', () => {
    const failed = new Map([
      [30002, 'cluster node 127.0.0.1:30002 (discovered)'],
    ])
    const problem = replicaMasterProblem(30002, flushed, failed)
    assert.ok(problem)
    assert.match(problem.message, /its master failed/)
    assert.match(
      problem.message,
      /cluster node 127\.0\.0\.1:30002 \(discovered\)/,
    )
    assert.doesNotMatch(problem.message, /did not flush/)
    // The master's own failure carries the hint that fits; the stale-replica
    // hint would point away from the real cause.
    assert.strictEqual(problem.hint, undefined)
  })

  test('a replica of a master this run never knew is a stale replica', () => {
    const problem = replicaMasterProblem(31000, flushed, new Map())
    assert.ok(problem)
    assert.match(problem.message, /this run did not flush/)
    assert.strictEqual(problem.hint, HINT.staleReplica)
  })
})
