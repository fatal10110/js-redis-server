import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import clusterKeySlot from 'cluster-key-slot'
import { needsCluster, TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection, respNumber, respText } from './raw-connection'
import { expectReply, rawSlotOwner } from './helpers'

// #85 / #616: resharding one slot by hand, as redis-cli --cluster reshard
// does: CLUSTER SETSLOT IMPORTING on the target, MIGRATING on the source,
// the keys moved (DUMP / RESTORE-ASKING / DEL here), then SETSLOT NODE on
// both. Meanwhile the source ASKs for the keys it no longer holds, the
// target serves them only after ASKING, and a multi-key command that finds
// some keys on each side is TRYAGAIN (getNodeByQuery, redis 7.2.4
// cluster.c). Captured from real 2-master clusters of redis 6.2.14 - 8.0.0
// and valkey 8.0.0, 9.0.0. Every test opens a slot of its own (a random hash
// tag) and closes it again, so other suites sharing the cluster never see an
// open slot.

const testRunner = new TestRunner()
const tryAgain = '-TRYAGAIN Multiple keys request during rehashing of slot\r\n'
const execAbort =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'

type Node = {
  conn: RawRedisConnection
  id: string
  address: string
}

const clusterSuite = `Raw TCP cluster slot migration and ASK (${testRunner.getBackendName()}, ${activeProfile})`
describe(clusterSuite, needsCluster, () => {
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

  async function call(
    conn: RawRedisConnection,
    ...args: Array<string | Buffer>
  ): Promise<unknown> {
    conn.write(commandFrame(...args))
    return conn.readFrame()
  }

  async function connectNode(host: string, port: number): Promise<Node> {
    const conn = await RawRedisConnection.connect(host, port)
    connections.push(conn)
    const id = respText(await call(conn, 'CLUSTER', 'MYID'))
    return { conn, id, address: `${host}:${port}` }
  }

  /** Another master than the one at `port`. */
  async function otherMaster(
    port: number,
  ): Promise<{ host: string; port: number }> {
    const probe = await RawRedisConnection.connect('127.0.0.1', ports[0])
    try {
      const slots = (await call(probe, 'CLUSTER', 'SLOTS')) as unknown[][]
      for (const range of slots) {
        const master = range[2] as unknown[]
        const masterPort = respNumber(master[1])
        if (masterPort !== port) {
          return {
            host: respText(master[0]) || '127.0.0.1',
            port: masterPort,
          }
        }
      }
    } finally {
      probe.close()
    }
    throw new Error('no second master')
  }

  /**
   * A slot of its own, MIGRATING from its owner (`source`) to another master
   * (`target`), with `{tag}a` and `{tag}b` on the source. `close()` drops
   * the keys on both sides and makes the slot stable again.
   */
  async function openSlot() {
    const tag = `{migrate:${randomKey()}}`
    const slot = clusterKeySlot(tag)
    const owner = await rawSlotOwner(ports, tag)
    const other = await otherMaster(owner.port)
    const source = await connectNode(owner.host, owner.port)
    const target = await connectNode(other.host, other.port)
    const keys = { a: `${tag}a`, b: `${tag}b`, missing: `${tag}missing` }

    await expectReply(source.conn, ['SET', keys.a, '1'], '+OK\r\n')
    await expectReply(source.conn, ['SET', keys.b, '2'], '+OK\r\n')
    await expectReply(
      target.conn,
      ['CLUSTER', 'SETSLOT', String(slot), 'IMPORTING', source.id],
      '+OK\r\n',
    )
    await expectReply(
      source.conn,
      ['CLUSTER', 'SETSLOT', String(slot), 'MIGRATING', target.id],
      '+OK\r\n',
    )

    const close = async () => {
      for (const node of [target, source]) {
        await call(node.conn, 'ASKING')
        await call(node.conn, 'DEL', keys.a, keys.b, keys.missing)
        await call(node.conn, 'CLUSTER', 'SETSLOT', String(slot), 'STABLE')
      }
    }
    return {
      slot,
      tag,
      keys,
      source,
      target,
      ask: `-ASK ${slot} ${target.address}\r\n`,
      movedToSource: `-MOVED ${slot} ${source.address}\r\n`,
      close,
    }
  }

  test('the source serves the keys it holds and ASKs for the others', async () => {
    const { keys, source, ask, close } = await openSlot()
    try {
      const conn = source.conn
      await expectReply(conn, ['GET', keys.a], '$1\r\n1\r\n')
      await expectReply(conn, ['GET', keys.missing], ask)
      await expectReply(conn, ['SET', keys.missing, 'x'], ask)
      await expectReply(
        conn,
        ['MSET', keys.missing, '1', `${keys.missing}2`, '2'],
        ask,
      )
      // Some keys here, some not: TRYAGAIN from 7.0.3, ASK before.
      await expectReply(
        conn,
        ['MGET', keys.a, keys.missing],
        activeProfile === 'redis-6.2' ? ask : tryAgain,
      )
      await expectReply(
        conn,
        ['MGET', keys.a, keys.b],
        '*2\r\n$1\r\n1\r\n$1\r\n2\r\n',
      )
      await expectReply(
        conn,
        ['EVAL', "return redis.call('GET', KEYS[1])", '1', keys.missing],
        ask,
      )
      // ASK applies to keyed commands only.
      await expectReply(conn, ['PING'], '+PONG\r\n')
    } finally {
      await close()
    }
  })

  test('the target serves the slot only after ASKING, for one command', async () => {
    const { keys, target, movedToSource, close } = await openSlot()
    try {
      const conn = target.conn
      await expectReply(conn, ['GET', keys.a], movedToSource)
      await expectReply(conn, ['ASKING'], '+OK\r\n')
      await expectReply(conn, ['GET', keys.a], '$-1\r\n')
      await expectReply(conn, ['GET', keys.a], movedToSource)

      await expectReply(conn, ['ASKING'], '+OK\r\n')
      await expectReply(conn, ['PING'], '+PONG\r\n')
      await expectReply(conn, ['GET', keys.a], movedToSource)

      await expectReply(conn, ['ASKING'], '+OK\r\n')
      await expectReply(conn, ['SET', keys.missing, 'v'], '+OK\r\n')
      await expectReply(conn, ['ASKING'], '+OK\r\n')
      await expectReply(conn, ['MGET', keys.missing, keys.a], tryAgain)
      await expectReply(conn, ['ASKING'], '+OK\r\n')
      await expectReply(conn, ['MGET', keys.a, keys.a], '*2\r\n$-1\r\n$-1\r\n')

      // The flag holds through MULTI until EXEC.
      await expectReply(conn, ['ASKING'], '+OK\r\n')
      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['GET', keys.missing], '+QUEUED\r\n')
      await expectReply(conn, ['GET', keys.missing], '+QUEUED\r\n')
      await expectReply(conn, ['EXEC'], '*2\r\n$1\r\nv\r\n$1\r\nv\r\n')
      await expectReply(conn, ['GET', keys.missing], movedToSource)
    } finally {
      await close()
    }
  })

  test('MULTI on the source: a moved key is refused when queued (valkey 9.0: at EXEC)', async () => {
    const { keys, source, ask, close } = await openSlot()
    try {
      const conn = source.conn
      await expectReply(conn, ['MULTI'], '+OK\r\n')
      if (activeProfile === 'valkey-9.0') {
        await expectReply(conn, ['GET', keys.missing], '+QUEUED\r\n')
        await expectReply(conn, ['GET', keys.a], '+QUEUED\r\n')
        await expectReply(conn, ['EXEC'], tryAgain)
      } else {
        await expectReply(conn, ['GET', keys.missing], ask)
        await expectReply(conn, ['GET', keys.a], '+QUEUED\r\n')
        await expectReply(conn, ['EXEC'], execAbort)
      }

      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['GET', keys.a], '+QUEUED\r\n')
      await expectReply(conn, ['GET', keys.b], '+QUEUED\r\n')
      await expectReply(conn, ['EXEC'], '*2\r\n$1\r\n1\r\n$1\r\n2\r\n')
    } finally {
      await close()
    }
  })

  test("CLUSTER NODES marks the open slot on each node's own line", async () => {
    const { slot, source, target, close } = await openSlot()
    try {
      const ownLine = async (node: Node) => {
        const nodes = respText(await call(node.conn, 'CLUSTER', 'NODES'))
        return nodes.split('\n').find(line => line.includes('myself')) ?? ''
      }
      assert.ok(
        (await ownLine(source)).endsWith(` [${slot}->-${target.id}]`),
        await ownLine(source),
      )
      assert.ok(
        (await ownLine(target)).endsWith(` [${slot}-<-${source.id}]`),
        await ownLine(target),
      )
    } finally {
      await close()
    }
  })

  test('CLUSTER SETSLOT refusals and the slot helpers', async () => {
    const { slot, keys, source, target, close } = await openSlot()
    try {
      const s = String(slot)
      await expectReply(
        source.conn,
        ['CLUSTER', 'KEYSLOT', keys.a],
        `:${slot}\r\n`,
      )
      await expectReply(
        source.conn,
        ['CLUSTER', 'COUNTKEYSINSLOT', s],
        ':2\r\n',
      )
      await expectReply(
        target.conn,
        ['CLUSTER', 'COUNTKEYSINSLOT', s],
        ':0\r\n',
      )
      const inSlot = (await call(
        source.conn,
        'CLUSTER',
        'GETKEYSINSLOT',
        s,
        '10',
      )) as unknown[]
      assert.deepStrictEqual(inSlot.map(respText).sort(), [keys.a, keys.b])
      await expectReply(
        source.conn,
        ['CLUSTER', 'SETSLOT', s, 'NODE', target.id],
        `-ERR Can't assign hashslot ${slot} to a different node while I still hold keys for this hash slot.\r\n`,
      )
      await expectReply(
        source.conn,
        ['CLUSTER', 'SETSLOT', s, 'IMPORTING', target.id],
        `-ERR I'm already the owner of hash slot ${slot}\r\n`,
      )
      await expectReply(
        target.conn,
        ['CLUSTER', 'SETSLOT', s, 'MIGRATING', source.id],
        `-ERR I'm not the owner of hash slot ${slot}\r\n`,
      )
      await expectReply(
        source.conn,
        ['CLUSTER', 'SETSLOT', s, 'NODE', 'nosuchnode'],
        '-ERR Unknown node nosuchnode\r\n',
      )
      await expectReply(
        source.conn,
        ['CLUSTER', 'SETSLOT', s, 'MIGRATING', 'nosuchnode'],
        "-ERR I don't know about node nosuchnode\r\n",
      )
      await expectReply(
        source.conn,
        ['CLUSTER', 'SETSLOT', s, 'BOGUS', 'x'],
        '-ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP\r\n',
      )
      await expectReply(
        source.conn,
        ['CLUSTER', 'SETSLOT', '16384', 'STABLE'],
        '-ERR Invalid or out of range slot\r\n',
      )
    } finally {
      await close()
    }
  })

  test('a whole resharding moves the slot, and moving it back restores it', async () => {
    const { slot, keys, source, target, ask, close } = await openSlot()
    const s = String(slot)
    let moved = false
    try {
      // Move every key: DUMP on the source, RESTORE-ASKING on the target
      // (served without ASKING), DEL on the source.
      for (const key of [keys.a, keys.b]) {
        const payload = await call(source.conn, 'DUMP', key)
        assert.ok(Buffer.isBuffer(payload) || typeof payload === 'string')
        await expectReply(
          target.conn,
          ['RESTORE-ASKING', key, '0', payload as Buffer],
          '+OK\r\n',
        )
        await expectReply(source.conn, ['DEL', key], ':1\r\n')
      }
      await expectReply(source.conn, ['GET', keys.a], ask)

      await expectReply(
        target.conn,
        ['CLUSTER', 'SETSLOT', s, 'NODE', target.id],
        '+OK\r\n',
      )
      await expectReply(
        source.conn,
        ['CLUSTER', 'SETSLOT', s, 'NODE', target.id],
        '+OK\r\n',
      )
      moved = true
      await expectReply(
        source.conn,
        ['GET', keys.a],
        `-MOVED ${slot} ${target.address}\r\n`,
      )
      await expectReply(target.conn, ['GET', keys.a], '$1\r\n1\r\n')
      const owner = await call(source.conn, 'CLUSTER', 'SLOTS')
      const range = (owner as unknown[][]).find(
        entry => respNumber(entry[0]) <= slot && slot <= respNumber(entry[1]),
      )
      assert.ok(range)
      assert.strictEqual(
        respNumber((range[2] as unknown[])[1]),
        Number(target.address.split(':').pop()),
      )

      // And back again, the other way round.
      await expectReply(
        source.conn,
        ['CLUSTER', 'SETSLOT', s, 'IMPORTING', target.id],
        '+OK\r\n',
      )
      await expectReply(
        target.conn,
        ['CLUSTER', 'SETSLOT', s, 'MIGRATING', source.id],
        '+OK\r\n',
      )
      for (const key of [keys.a, keys.b]) {
        const payload = await call(target.conn, 'DUMP', key)
        await expectReply(
          source.conn,
          ['RESTORE-ASKING', key, '0', payload as Buffer],
          '+OK\r\n',
        )
        await expectReply(target.conn, ['DEL', key], ':1\r\n')
      }
      await expectReply(
        source.conn,
        ['CLUSTER', 'SETSLOT', s, 'NODE', source.id],
        '+OK\r\n',
      )
      await expectReply(
        target.conn,
        ['CLUSTER', 'SETSLOT', s, 'NODE', source.id],
        '+OK\r\n',
      )
      moved = false
      await expectReply(source.conn, ['GET', keys.b], '$1\r\n2\r\n')
    } finally {
      if (moved) {
        // Leave the slot where it was: back to the source, keys and all.
        await call(source.conn, 'CLUSTER', 'SETSLOT', s, 'NODE', source.id)
        await call(target.conn, 'CLUSTER', 'SETSLOT', s, 'STABLE')
      }
      await close()
    }
  })
})
