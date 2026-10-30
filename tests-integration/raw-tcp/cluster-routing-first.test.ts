import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import clusterKeySlot from 'cluster-key-slot'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import {
  connectToRawSlotOwner,
  expectReply,
  expectReplyPrefix,
  rawSlotOwner,
} from './helpers'

// #519. Real Redis (redis 6.2.14 to 8.0.0, valkey 8.0.0 and 9.0.0) routes a
// keyed command in processCommand (getNodeByQuery) after the arity check and
// before the command parses its own arguments; MOVE refuses the cluster in its
// own command proc. The real cluster in CI is three masters from
// `redis-cli --cluster create`, which splits the slots 0-5460, 5461-10922,
// 10923-16383.

const testRunner = new TestRunner()
const notInteger = '-ERR value is not an integer or out of range\r\n'

describe(`Raw TCP cluster routes before argument checks (${testRunner.getBackendName()}, ${activeProfile})`, () => {
  let ports: number[]
  const connections: RawRedisConnection[] = []

  before(async () => {
    ports = await testRunner.setupRawCluster()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  /** A connection to `local`'s owner, and a key another node owns. */
  async function setup() {
    const local = `routing-first:${randomKey()}`
    const conn = await connectToRawSlotOwner(ports, local)
    connections.push(conn)
    const owner = await rawSlotOwner(ports, local)
    let remote = `routing-first:${randomKey()}`
    while ((await rawSlotOwner(ports, remote)).port === owner.port) {
      remote = `routing-first:${randomKey()}`
    }
    return { conn, local, remote, moved: `-MOVED ${clusterKeySlot(remote)} ` }
  }

  test('a key on another node is MOVED before the argument check', async () => {
    const { conn, remote, moved } = await setup()

    for (const command of [
      ['EVAL', 'return 1', '1x', remote],
      ['SET', remote, '1', 'EX', 'x'],
      ['SET', remote, '1', 'NX', 'XX'],
      ['INCRBY', remote, 'x'],
      ['BLPOP', remote, 'x'],
      ['XREAD', 'COUNT', 'x', 'STREAMS', remote, '0'],
      ['MOVE', remote, '1'],
      ['MOVE', remote, 'x'],
    ]) {
      await expectReplyPrefix(conn, command, moved)
    }
  })

  test('a local key answers the command own argument error', async () => {
    const { conn, local } = await setup()

    await expectReply(conn, ['EVAL', 'return 1', '1x', local], notInteger)
    await expectReply(conn, ['SET', local, '1', 'EX', 'x'], notInteger)
    // numkeys past the arguments leaves the command keyless.
    await expectReply(
      conn,
      ['EVAL', 'return 1', '5', local],
      "-ERR Number of keys can't be greater than number of args\r\n",
    )
  })

  test('MOVE of a local key is refused before its database is parsed', async () => {
    const { conn, local } = await setup()
    // Valkey 9 clusters have databases (cluster.multi-db).
    const valkey9 = activeProfile === 'valkey-9.0'

    await expectReply(
      conn,
      ['MOVE', local, 'x'],
      valkey9 ? notInteger : '-ERR MOVE is not allowed in cluster mode\r\n',
    )
    await expectReply(
      conn,
      ['MOVE', local, '1'],
      valkey9
        ? '-ERR DB index is out of range\r\n'
        : '-ERR MOVE is not allowed in cluster mode\r\n',
    )
  })

  test('CLUSTER INFO has no cluster_enabled line', async () => {
    const { conn } = await setup()
    conn.write(commandFrame('CLUSTER', 'INFO'))
    const reply = (await conn.readRawFrame()).toString()
    const text = reply.slice(reply.indexOf('\r\n') + 2)

    assert.ok(text.startsWith('cluster_state:ok\r\n'), reply)
    assert.ok(!text.includes('cluster_enabled'), reply)
  })

  test('three masters split the slots as redis-cli --cluster create', async () => {
    // Slots 5460, 5461, 10922 and 10923 sit on the range edges.
    const edges = ['k100009', 'k13535', 'k19076', 'k12284', 'k69207']
    assert.deepStrictEqual(
      edges.map(key => clusterKeySlot(key)),
      [5460, 5461, 10921, 10922, 10923],
    )
    const [s5460, s5461, s10921, s10922, s10923] = await Promise.all(
      edges.map(async key => (await rawSlotOwner(ports, key)).port),
    )

    assert.notStrictEqual(s5460, s5461)
    assert.strictEqual(s5461, s10921)
    assert.strictEqual(s10921, s10922)
    assert.notStrictEqual(s10922, s10923)
  })
})
