import { after, before, describe, test } from 'node:test'
import clusterKeySlot from 'cluster-key-slot'
import { needsCluster, TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection, respNumber, respText } from './raw-connection'
import { expectReply, rawSlotOwner } from './helpers'

// #205: MIGRATE moving the keys of a slot open for resharding, as
// redis-cli --cluster reshard does: the source sends RESTORE-ASKING, which
// the IMPORTING target serves without ASKING, then asks for the keys it no
// longer holds. Every test opens a slot of its own and closes it again.

const testRunner = new TestRunner()

const clusterSuite = `Raw TCP cluster MIGRATE (${testRunner.getBackendName()}, ${activeProfile})`
describe(clusterSuite, needsCluster, () => {
  let ports: number[]
  const connections: RawRedisConnection[] = []

  before(async () => {
    ports = await testRunner.setupRawCluster()
  })

  after(async () => {
    for (const connection of connections) connection.close()
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

  async function connectNode(host: string, port: number) {
    const conn = await RawRedisConnection.connect(host, port)
    connections.push(conn)
    const id = respText(await call(conn, 'CLUSTER', 'MYID'))
    return { conn, id, host, port }
  }

  async function otherMaster(port: number) {
    const probe = await RawRedisConnection.connect('127.0.0.1', ports[0])
    try {
      const slots = (await call(probe, 'CLUSTER', 'SLOTS')) as unknown[][]
      for (const range of slots) {
        const master = range[2] as unknown[]
        if (respNumber(master[1]) !== port) {
          return {
            host: respText(master[0]) || '127.0.0.1',
            port: respNumber(master[1]),
          }
        }
      }
    } finally {
      probe.close()
    }
    throw new Error('no second master')
  }

  test('MIGRATE moves the keys of a MIGRATING slot with RESTORE-ASKING', async () => {
    const tag = `{migrate:${randomKey()}}`
    const slot = String(clusterKeySlot(tag))
    const owner = await rawSlotOwner(ports, tag)
    const other = await otherMaster(owner.port)
    const source = await connectNode(owner.host, owner.port)
    const target = await connectNode(other.host, other.port)
    const [a, b, missing] = [`${tag}a`, `${tag}b`, `${tag}missing`]
    await expectReply(source.conn, ['SET', a, '1'], '+OK\r\n')
    await expectReply(source.conn, ['SET', b, '2'], '+OK\r\n')
    await expectReply(
      target.conn,
      ['CLUSTER', 'SETSLOT', slot, 'IMPORTING', source.id],
      '+OK\r\n',
    )
    await expectReply(
      source.conn,
      ['CLUSTER', 'SETSLOT', slot, 'MIGRATING', target.id],
      '+OK\r\n',
    )
    try {
      // The source dials the target at the address the cluster announces.
      await expectReply(
        source.conn,
        [
          'MIGRATE',
          target.host,
          String(target.port),
          '',
          '0',
          '5000',
          'KEYS',
          a,
          missing,
          b,
        ],
        '+OK\r\n',
      )
      const ask = `-ASK ${slot} ${target.host}:${target.port}\r\n`
      await expectReply(source.conn, ['GET', a], ask)
      // Without ASKING the target redirects back; with it, it serves.
      await expectReply(
        target.conn,
        ['GET', a],
        `-MOVED ${slot} ${owner.host}:${owner.port}\r\n`,
      )
      await expectReply(target.conn, ['ASKING'], '+OK\r\n')
      await expectReply(target.conn, ['GET', a], '$1\r\n1\r\n')
      await expectReply(target.conn, ['ASKING'], '+OK\r\n')
      await expectReply(target.conn, ['GET', b], '$1\r\n2\r\n')
      // Nothing left on the source: NOKEY.
      await expectReply(
        source.conn,
        ['MIGRATE', target.host, String(target.port), a, '0', '5000'],
        '+NOKEY\r\n',
      )
    } finally {
      for (const node of [target, source]) {
        await call(node.conn, 'ASKING')
        await call(node.conn, 'DEL', a, b)
        await call(node.conn, 'CLUSTER', 'SETSLOT', slot, 'STABLE')
      }
    }
  })
})
