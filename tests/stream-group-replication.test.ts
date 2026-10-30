import { describe, test } from 'node:test'
import assert from 'node:assert'
import { setTimeout as sleep } from 'node:timers/promises'
import { buildClusterNodes } from '../src/cluster'
import { ClientSession } from '../src/internal'
import type { CompatibilitySpec, RedisMutationEvent } from '../src/internal'

// Real Redis propagates in-place consumer-group / last-id changes to its
// replicas (XGROUP subcommands, XSETID and XACK as themselves; XREADGROUP,
// XCLAIM and XAUTOCLAIM as idempotent XCLAIM / XGROUP SETID / XGROUP
// CREATECONSUMER, see streamPropagateXCLAIM in t_stream.c), so after each of
// them the replica's stream equals the master's — with no XADD in between to
// piggyback the group state on a full-value write.

function setup(
  options: { compatibility?: CompatibilitySpec; delayMs?: number } = {},
) {
  const { nodes, replicationLinks } = buildClusterNodes({
    masters: 1,
    replicasPerMaster: 1,
    basePort: 0,
    compatibility: options.compatibility,
    replicaUpdateDelayMs: options.delayMs,
  })
  const master = nodes.find(node => node.role === 'master')
  const replica = nodes.find(node => node.role === 'replica')
  assert.ok(master && replica)
  const session = new ClientSession({
    server: master.state,
    executor: master.executor,
  })
  const run = (command: string, ...args: string[]) =>
    session.execute(
      command,
      args.map(arg => Buffer.from(arg)),
    )
  return {
    run,
    master: master.state,
    masterDb: master.state.getDatabase(0),
    replicaDb: replica.state.getDatabase(0),
    close() {
      for (const link of replicationLinks) link.close()
      for (const node of nodes) node.state.close()
    },
  }
}

const key = Buffer.from('{repl}:s')

const steps: [string, ...string[]][] = [
  ['xgroup', 'CREATE', '{repl}:s', 'g', '0'],
  ['xgroup', 'CREATECONSUMER', '{repl}:s', 'g', 'c1'],
  // Implicit consumer creation + PEL + last-delivered-id.
  ['xreadgroup', 'GROUP', 'g', 'c2', 'COUNT', '2', 'STREAMS', '{repl}:s', '>'],
  ['xack', '{repl}:s', 'g', '1-1'],
  ['xclaim', '{repl}:s', 'g', 'c1', '0', '2-1'],
  ['xautoclaim', '{repl}:s', 'g', 'c3', '0', '0-0'],
  ['xclaim', '{repl}:s', 'g', 'c1', '0', '2-1', 'LASTID', '3-1'],
  ['xgroup', 'SETID', '{repl}:s', 'g', '1-1'],
  ['xreadgroup', 'GROUP', 'g', 'c4', 'NOACK', 'STREAMS', '{repl}:s', '>'],
  ['xsetid', '{repl}:s', '10-0'],
  ['xgroup', 'DELCONSUMER', '{repl}:s', 'g', 'c3'],
  ['xgroup', 'DESTROY', '{repl}:s', 'g'],
]

async function seed(run: ReturnType<typeof setup>['run']) {
  for (const id of ['1-1', '2-1', '3-1']) {
    await run('xadd', '{repl}:s', id, 'f', 'v')
  }
}

describe('stream consumer-group replication', () => {
  for (const compatibility of ['redis-8.0', 'redis-6.2'] as const) {
    test(`replica follows in-place group changes (${compatibility})`, async () => {
      const harness = setup({ compatibility })
      try {
        await seed(harness.run)
        for (const [command, ...args] of steps) {
          const reply = await harness.run(command, ...args)
          assert.notStrictEqual(reply.value.kind, 'error', command)
          assert.deepStrictEqual(
            harness.replicaDb.getStream(key),
            harness.masterDb.getStream(key),
            `${command} ${args.join(' ')}`,
          )
        }
      } finally {
        harness.close()
      }
    })
  }

  test('a delayed replica applies the group state as of each change', async () => {
    const harness = setup({ delayMs: 20 })
    try {
      await seed(harness.run)
      await harness.run('xgroup', 'CREATE', '{repl}:s', 'g', '0')
      await sleep(40)

      const snapshots: RedisMutationEvent[] = []
      harness.masterDb.subscribe(event => snapshots.push({ ...event }))
      await harness.run(
        'xreadgroup',
        'GROUP',
        'g',
        'c',
        'STREAMS',
        '{repl}:s',
        '>',
      )
      const delivered = snapshots.at(-1)
      await harness.run('xack', '{repl}:s', 'g', '1-1', '2-1', '3-1')

      // The XREADGROUP snapshot still holds the three pending entries the
      // XACK cleared right after it.
      assert.strictEqual(snapshots.at(-1)?.type, 'propagate')
      assert.ok(delivered?.type === 'propagate')
      assert.strictEqual(delivered.value.type, 'stream')
      assert.strictEqual(delivered.value.groups.get('67')?.pending.size, 3)

      await sleep(60)
      assert.deepStrictEqual(
        harness.replicaDb.getStream(key),
        harness.masterDb.getStream(key),
      )
      assert.strictEqual(
        harness.replicaDb.getStream(key)?.groups.get('67')?.pending.size,
        0,
      )
    } finally {
      harness.close()
    }
  })

  test('replicating group changes dirties no WATCH and publishes no event', async () => {
    const harness = setup()
    try {
      await seed(harness.run)
      await harness.run('config', 'set', 'notify-keyspace-events', 'KEA')
      const published: string[] = []
      harness.master.pubsubBroker.psubscribe(
        Buffer.from('__keyevent@0__:*'),
        m => published.push(m.channel.toString()),
      )
      const keyEvents: string[] = []
      harness.masterDb.subscribeKey(key, event => keyEvents.push(event.type))

      await harness.run('xgroup', 'CREATE', '{repl}:s', 'g', '0')
      await harness.run(
        'xreadgroup',
        'GROUP',
        'g',
        'c',
        'STREAMS',
        '{repl}:s',
        '>',
      )
      await harness.run('xack', '{repl}:s', 'g', '1-1')
      await harness.run('xclaim', '{repl}:s', 'g', 'c', '0', '2-1')
      await harness.run('xautoclaim', '{repl}:s', 'g', 'c', '0', '0-0')

      assert.deepStrictEqual(keyEvents, [])
      assert.deepStrictEqual(published, [
        '__keyevent@0__:xgroup-create',
        '__keyevent@0__:xgroup-createconsumer',
      ])
      assert.deepStrictEqual(
        harness.replicaDb.getStream(key),
        harness.masterDb.getStream(key),
      )
    } finally {
      harness.close()
    }
  })
})
