import { after, before, describe, test } from 'node:test'
import { createHash } from 'node:crypto'
import { needsCluster, TestRunner } from '../test-config'
import { activeProfile, keyInAnotherSlot, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import {
  connectToRawSlotOwner,
  expectReply,
  rawSlotOwner,
} from '../raw-tcp/helpers'

// #618: a command run from a script whose own keys hash to different slots
// answers the scripting layer's error (scriptVerifyClusterState), not
// -CROSSSLOT. Probed on single-node clusters of redis 6.2.14, 7.0.15, 7.2.4,
// 7.4.4, 8.0.0 and valkey 8.0.0, 9.0.0:
// - 6.2: `@user_script: 1: Lua script attempted to access a non local key in
//   a cluster node` (luaPushError, no code);
// - 7.0 / 7.2: `ERR Script attempted to access a non local key in a cluster
//   node`;
// - redis 7.4+ and valkey 8.0+: `ERR Command '<name>' in script attempted to
//   access keys that don't hash to the same slot`.
// redis.call aborts the script with the same text, decorated as usual.

const testRunner = new TestRunner()

const nonLocal = 'Script attempted to access a non local key in a cluster node'

function pcallReply(command: string): string {
  switch (activeProfile) {
    case 'redis-6.2':
      return `-@user_script: 1: Lua script attempted to access a non local key in a cluster node\r\n`
    case 'redis-7.0':
    case 'redis-7.2':
      return `-ERR ${nonLocal}\r\n`
    default:
      return `-ERR Command '${command}' in script attempted to access keys that don't hash to the same slot\r\n`
  }
}

function callReply(script: string, command: string): string {
  const sha = createHash('sha1').update(script).digest('hex')
  const text = pcallReply(command).slice(1, -2)
  if (activeProfile === 'redis-6.2') {
    return `-ERR Error running script (call to f_${sha}): @user_script:1: ${text}\r\n`
  }
  return `-${text} script: ${sha}, on @user_script:1.\r\n`
}

const clusterSuite = `Script command across slots in a cluster (${testRunner.getBackendName()}, ${activeProfile})`
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

  /**
   * A fresh connection to the owner of `first`, a key in another slot of the
   * same node, and a key in another slot on any node.
   */
  async function setup() {
    const first = `{scs:${randomKey()}}:a`
    const conn = await connectToRawSlotOwner(ports, first)
    connections.push(conn)
    const owner = await rawSlotOwner(ports, first)
    const candidate = () => `{scs:${randomKey()}}:b`
    let sameNode = keyInAnotherSlot(first, candidate)
    while ((await rawSlotOwner(ports, sameNode)).port !== owner.port) {
      sameNode = keyInAnotherSlot(first, candidate)
    }
    const anyNode = keyInAnotherSlot(first, candidate)
    return { conn, first, sameNode, anyNode }
  }

  test('redis.pcall returns the scripting error', async () => {
    const { conn, first, sameNode, anyNode } = await setup()
    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('MSET', ARGV[1], '1', ARGV[2], '1')",
        '0',
        first,
        sameNode,
      ],
      pcallReply('mset'),
    )
    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('MGET', ARGV[1], ARGV[2])",
        '0',
        first,
        anyNode,
      ],
      pcallReply('mget'),
    )
  })

  test('redis.call aborts the script with it', async () => {
    const { conn, first, sameNode } = await setup()
    const script = "return redis.call('MSET', ARGV[1], '1', ARGV[2], '1')"
    await expectReply(
      conn,
      ['EVAL', script, '0', first, sameNode],
      callReply(script, 'mset'),
    )
    await expectReply(conn, ['GET', first], '$-1\r\n')
  })

  test('keys sharing one slot still run', async () => {
    const { conn, first } = await setup()
    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('MSET', ARGV[1], '1', ARGV[2], '2')",
        '0',
        first,
        `${first}:twin`,
      ],
      '+OK\r\n',
    )
  })
})
