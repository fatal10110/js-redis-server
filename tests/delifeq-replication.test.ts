import { describe, test } from 'node:test'
import assert from 'node:assert'
import { buildClusterNodes } from '../src/cluster'
import { ClientSession } from '../src/internal'

// Valkey 9.0 propagates a DELIFEQ that deleted as `DEL key` and nothing when
// it answered 0 (valkey@9.0.0 t_string.c delifeqCommand); the replica follows
// the master's keyspace either way.
describe('DELIFEQ replication (valkey-9.0)', () => {
  test('the replica drops the key only when DELIFEQ deletes it', async () => {
    const { nodes, replicationLinks } = buildClusterNodes({
      masters: 1,
      replicasPerMaster: 1,
      basePort: 0,
      compatibility: 'valkey-9.0',
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
    const key = Buffer.from('{repl}:k')
    const replicaDb = replica.state.getDatabase(0)
    try {
      await run('set', '{repl}:k', 'a')
      assert.deepStrictEqual(replicaDb.getString(key), Buffer.from('a'))

      assert.deepStrictEqual((await run('delifeq', '{repl}:k', 'b')).value, {
        kind: 'integer',
        value: 0,
      })
      assert.deepStrictEqual(replicaDb.getString(key), Buffer.from('a'))

      assert.deepStrictEqual((await run('delifeq', '{repl}:k', 'a')).value, {
        kind: 'integer',
        value: 1,
      })
      assert.strictEqual(replicaDb.getType(key), null)
    } finally {
      for (const link of replicationLinks) link.close()
      for (const node of nodes) node.state.close()
    }
  })
})
