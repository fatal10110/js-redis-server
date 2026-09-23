import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  type ClusterNode,
  type EndpointOutcome,
  HINT,
  isAddressless,
  masterOutcomes,
  parseClusterNodes,
  planCluster,
  replicaMasterProblem,
} from '../scripts/flush-redis-topology'

// `clean:redis` (scripts/flush-redis.ts) decides what to flush from CLUSTER
// NODES. These fixtures follow what real Redis 7.2 prints for the
// docker-compose cluster (masters 30000-30002, replicas 30003-30005) — in
// particular, migration markers appear only on the `myself` line — so the
// decisions are tested without breaking a shared cluster (#458).

const id = (c: string) => c.repeat(40)
const M1 = id('a')
const M2 = id('b')
const M3 = id('c')
const R1 = id('d')
const R2 = id('e')
const R3 = id('f')
/** A master with no slots (e.g. just added, or the target of a migration). */
const M4 = id('7')
/** A replica of M4. */
const R4 = id('6')
const HANDSHAKE = id('9')
const NOADDR = id('8')

const PORT: Record<string, number> = {
  [M1]: 30000,
  [M2]: 30001,
  [M3]: 30002,
  [R1]: 30003,
  [R2]: 30004,
  [R3]: 30005,
  [M4]: 30006,
  [R4]: 30007,
}

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

type Spec = { flags: string; master: string; slots: string[] }

const HEALTHY: Record<string, Spec> = {
  [M1]: { flags: 'master', master: '-', slots: ['0-5460'] },
  [M2]: { flags: 'master', master: '-', slots: ['5461-10922'] },
  [M3]: { flags: 'master', master: '-', slots: ['10923-16383'] },
  [R1]: { flags: 'slave', master: M1, slots: [] },
  [R2]: { flags: 'slave', master: M2, slots: [] },
  [R3]: { flags: 'slave', master: M3, slots: [] },
}

const WITH_EMPTY_MASTER: Record<string, Spec> = {
  ...HEALTHY,
  [M4]: { flags: 'master', master: '-', slots: [] },
  [R4]: { flags: 'slave', master: M4, slots: [] },
}

/**
 * The cluster as CLUSTER NODES prints it on `seed`. `myselfSlots` are extra
 * entries (migration markers) printed only on the seed's own line.
 */
function viewFrom(
  seed: string,
  specs: Record<string, Spec> = HEALTHY,
  myselfSlots: string[] = [],
): string[] {
  return Object.entries(specs).map(([nodeId, { flags, master, slots }]) => {
    const port = PORT[nodeId]
    const mine = nodeId === seed
    return line(
      nodeId,
      `127.0.0.1:${port}@${port + 10000}`,
      mine ? `myself,${flags}` : flags,
      master,
      ...slots,
      ...(mine ? myselfSlots : []),
    )
  })
}

// Real Redis prints ping-sent 0 for a handshake entry.
const HANDSHAKE_LINE = `${HANDSHAKE} 127.0.0.1:7999@17999 handshake - 0 0 0 disconnected`
const NOADDR_LINE = `${NOADDR} :0@0 master,noaddr - 1700000000000 1700000000000 0 disconnected`

const view = (lines: string[]) => parseClusterNodes(lines.join('\n') + '\n')
const ids = (nodes: { id: string }[]) => nodes.map(n => n.id).sort()
/** Every node the flush covers: the seeds plus the planned members. */
const covered = (plan: ReturnType<typeof planCluster>) =>
  [...plan.seeds.keys(), ...ids(plan.members)].sort()

describe('clean:redis cluster topology (#458)', () => {
  test('parses master, slots, replica-of, and migration markers', () => {
    const nodes = view([...viewFrom(M4, WITH_EMPTY_MASTER, [`[5061-<-${M1}]`])])
    const byId = new Map(nodes.map(n => [n.id, n]))

    assert.deepStrictEqual(byId.get(M1), {
      id: M1,
      host: '127.0.0.1',
      port: 30000,
      flags: ['master'],
      masterId: null,
      ownsSlots: true,
    })
    assert.strictEqual(byId.get(R1)?.masterId, M1)
    assert.strictEqual(byId.get(R1)?.ownsSlots, false)
    // An importing marker on the `myself` line is not slot ownership.
    assert.deepStrictEqual(byId.get(M4)?.flags, ['myself', 'master'])
    assert.strictEqual(byId.get(M4)?.ownsSlots, false)
  })

  test('parses a hostname suffix and an address-less entry', () => {
    const [named, noaddr] = view([
      line(M1, '127.0.0.1:30000@40000,redis-1', 'master', '-', '0-16383'),
      NOADDR_LINE,
    ])
    assert.strictEqual(named.host, '127.0.0.1')
    assert.strictEqual(named.port, 30000)
    assert.ok(!isAddressless(named))
    assert.strictEqual(noaddr.host, '')
    assert.strictEqual(noaddr.port, 0)
    assert.ok(isAddressless(noaddr))
  })

  test('a healthy cluster covers every node', () => {
    const plan = planCluster([view(viewFrom(M1))])
    assert.deepStrictEqual([...plan.seeds.keys()], [M1])
    assert.deepStrictEqual(ids(plan.members), [M2, M3, R1, R2, R3].sort())
    assert.deepStrictEqual(plan.skipped, [])
  })

  test('a handshake node is skipped, and coverage does not depend on the seed', () => {
    // CLUSTER MEET to a dead port: only the node that issued it lists the
    // handshake entry. Seed 30000 sees it, seed 30001 does not.
    const withHandshake = view([...viewFrom(M1), HANDSHAKE_LINE])
    const withoutHandshake = view(viewFrom(M2))

    const fromA = planCluster([withHandshake])
    const fromB = planCluster([withoutHandshake])
    const all = [M1, M2, M3, R1, R2, R3].sort()

    assert.deepStrictEqual(ids(fromA.skipped), [HANDSHAKE])
    assert.deepStrictEqual(covered(fromA), all)
    assert.deepStrictEqual(ids(fromB.skipped), [])
    assert.deepStrictEqual(covered(fromB), all)

    const both = planCluster([withHandshake, withoutHandshake])
    assert.deepStrictEqual(covered(both), all)
    assert.deepStrictEqual(ids(both.skipped), [HANDSHAKE])
  })

  test('a noaddr node that owns no slots is skipped', () => {
    const plan = planCluster([view([...viewFrom(M1), NOADDR_LINE])])
    assert.deepStrictEqual(ids(plan.skipped), [NOADDR])
    assert.ok(plan.members.every(node => !isAddressless(node)))
  })

  test('a noaddr master that owns slots is still covered (and addressless)', () => {
    const lines = viewFrom(M1).filter(l => !l.startsWith(M3))
    lines.push(line(M3, ':0@0', 'master,fail,noaddr', '-', '10923-16383'))
    const plan = planCluster([view(lines)])
    const m3 = plan.members.find(node => node.id === M3)
    assert.ok(m3, 'slot-owning master must be covered')
    assert.ok(isAddressless(m3))
    assert.ok(ids(plan.members).includes(R3))
  })

  test('a slotless master mid-migration is covered from every seed', () => {
    // MIGRATE moved a key from M1 to M4 before CLUSTER SETSLOT NODE: M4 holds
    // it, owns no slots, and a client still reads it through -ASK. Only M1's
    // and M4's own lines carry the markers; from anywhere else M4 is a plain
    // empty master.
    const all = Object.keys(WITH_EMPTY_MASTER).sort()
    const seeds: [string, string[]][] = [
      [M1, [`[5061->-${M4}]`]], // the migrating node
      [M4, [`[5061-<-${M1}]`]], // the importing node
      [M2, []], // a bystander
      [R4, []], // a replica of the importing node
    ]
    for (const [seed, markers] of seeds) {
      const plan = planCluster([
        view(viewFrom(seed, WITH_EMPTY_MASTER, markers)),
      ])
      assert.deepStrictEqual(covered(plan), all, `seed ${PORT[seed]}`)
      assert.deepStrictEqual(plan.skipped, [], `seed ${PORT[seed]}`)
    }
  })

  test('a replica of a slotless master is covered whichever seed is used', () => {
    // The default REDIS_CLUSTER_PORTS lists replicas too, so a replica is an
    // ordinary seed; its master must be flushed either way, or the replica
    // would be judged against a master the run chose to skip.
    const fromReplica = planCluster([view(viewFrom(R4, WITH_EMPTY_MASTER))])
    const fromMaster = planCluster([view(viewFrom(M1, WITH_EMPTY_MASTER))])
    assert.ok(ids(fromReplica.members).includes(M4))
    assert.deepStrictEqual(covered(fromReplica), covered(fromMaster))
  })

  test('views merge: one covering report is enough', () => {
    // One seed only knows R3 without an address; another has its address.
    const noaddr = viewFrom(M1).map(l =>
      l.startsWith(R3) ? line(R3, ':0@0', 'slave,noaddr', M3) : l,
    )
    const plan = planCluster([view(noaddr), view(viewFrom(M2))])
    const r3 = plan.members.find(node => node.id === R3)
    assert.strictEqual(r3?.port, 30005)
    assert.ok(r3 && !isAddressless(r3))
    assert.deepStrictEqual(plan.skipped, [])
  })
})

describe('clean:redis master outcomes and replica attribution (#458)', () => {
  const nodes = new Map(
    view(viewFrom(M1, WITH_EMPTY_MASTER)).map(node => [node.id, node]),
  )
  const node = (nodeId: string): ClusterNode => {
    const found = nodes.get(nodeId)
    assert.ok(found)
    return found
  }
  const label = (nodeId: string) => `cluster node 127.0.0.1:${PORT[nodeId]}`

  test('sorts masters from every source into flushed and failed', () => {
    const noaddrM3: ClusterNode = { ...node(M3), host: '', port: 0 }
    const endpoints: EndpointOutcome[] = [
      // Flushed.
      {
        label: label(M1),
        port: 30000,
        node: node(M1),
        role: 'master',
        failed: false,
      },
      // FLUSHALL failed or left keys.
      {
        label: label(M2),
        port: 30001,
        node: node(M2),
        role: 'master',
        failed: true,
      },
      // Discovered as a master but never connected to, and without an address.
      {
        label: 'cluster node cccccccccccc…',
        port: 0,
        node: noaddrM3,
        failed: true,
      },
      // Discovered as a master, connection refused.
      { label: label(M4), port: 30006, node: node(M4), failed: true },
      // A seed whose CLUSTER NODES could not be read: known by port only.
      {
        label: 'cluster node 127.0.0.1:30009',
        port: 30009,
        role: 'master',
        failed: true,
      },
      // Replicas, reachable or not, are not masters.
      {
        label: label(R1),
        port: 30003,
        node: node(R1),
        role: 'replica',
        failed: true,
      },
      { label: label(R2), port: 30004, node: node(R2), failed: true },
      // A configured seed that never answered: role unknown, not a master.
      { label: 'cluster node 127.0.0.1:30010', port: 30010, failed: true },
    ]
    const outcomes = masterOutcomes(endpoints)

    assert.deepStrictEqual(
      [...outcomes.flushed].sort(),
      [`id:${M1}`, 'port:30000'].sort(),
    )
    assert.deepStrictEqual(
      [...outcomes.failed.keys()].sort(),
      [
        `id:${M2}`,
        'port:30001',
        `id:${M3}`,
        'port:0',
        `id:${M4}`,
        'port:30006',
        'port:30009',
      ].sort(),
    )
  })

  test('a replica is judged by its master id first', () => {
    const outcomes = masterOutcomes([
      {
        label: label(M1),
        port: 30000,
        node: node(M1),
        role: 'master',
        failed: false,
      },
      {
        label: 'cluster node cccccccccccc…',
        port: 0,
        node: { ...node(M3), host: '', port: 0, flags: ['master', 'noaddr'] },
        failed: true,
      },
    ])

    assert.strictEqual(replicaMasterProblem(30000, M1, outcomes), null)

    // The noaddr master's replica still reports the old master_port; only the
    // id ties it to the failed master.
    const problem = replicaMasterProblem(30002, M3, outcomes)
    assert.ok(problem)
    assert.match(
      problem.message,
      /its master failed \(see cluster node cccccccccccc…\)/,
    )
    assert.strictEqual(problem.hint, undefined)
  })

  test('a replica of a failed master refers back to the master', () => {
    const outcomes = masterOutcomes([
      {
        label: label(M3),
        port: 30002,
        node: node(M3),
        role: 'master',
        failed: true,
      },
    ])
    const problem = replicaMasterProblem(30002, M3, outcomes)
    assert.ok(problem)
    assert.match(problem.message, /its master failed/)
    assert.match(problem.message, /cluster node 127\.0\.0\.1:30002/)
    assert.doesNotMatch(problem.message, /did not flush/)
    // The master's own failure carries the hint that fits; the stale-replica
    // hint would point away from the real cause.
    assert.strictEqual(problem.hint, undefined)
  })

  test('falls back to the port when the master was never identified', () => {
    // A seed master whose CLUSTER NODES failed has no id; its replica does.
    const outcomes = masterOutcomes([
      { label: label(M3), port: 30002, role: 'master', failed: true },
    ])
    const problem = replicaMasterProblem(30002, M3, outcomes)
    assert.match(problem?.message ?? '', /its master failed/)
    // Standalone replicas have no id at all.
    assert.match(
      replicaMasterProblem(30002, undefined, outcomes)?.message ?? '',
      /its master failed/,
    )
  })

  test('a replica of a master this run never knew is a stale replica', () => {
    const outcomes = masterOutcomes([
      {
        label: label(M1),
        port: 30000,
        node: node(M1),
        role: 'master',
        failed: false,
      },
    ])
    const problem = replicaMasterProblem(31000, id('0'), outcomes)
    assert.ok(problem)
    assert.match(problem.message, /this run did not flush/)
    assert.strictEqual(problem.hint, HINT.staleReplica)
  })
})
