import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile, keyInAnotherSlot, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { connectToRawSlotOwner, expectReply, rawSlotOwner } from './helpers'

/**
 * LCS in a cluster (#206): its two keys must hash to one slot, like every
 * multi-key command. Keys in two slots of the same node answer CROSSSLOT;
 * keys sharing a hash tag run.
 */
const testRunner = new TestRunner()

const CROSSSLOT = "-CROSSSLOT Keys in request don't hash to the same slot\r\n"

describe(`Raw TCP cluster LCS across slots (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  test('LCS answers CROSSSLOT for keys in different slots', async () => {
    const tag = `lcs:${randomKey()}`
    const first = `{${tag}}:a`
    const conn = await connectToRawSlotOwner(ports, first)
    connections.push(conn)
    const owner = await rawSlotOwner(ports, first)
    const candidate = () => `{lcs:${randomKey()}}:b`
    let second = keyInAnotherSlot(first, candidate)
    while ((await rawSlotOwner(ports, second)).port !== owner.port) {
      second = keyInAnotherSlot(first, candidate)
    }

    await expectReply(conn, ['SET', first, 'ohmytext'], '+OK\r\n')
    await expectReply(conn, ['LCS', first, second], CROSSSLOT)
    await expectReply(conn, ['LCS', first, second, 'IDX'], CROSSSLOT)

    const sameSlot = `{${tag}}:b`
    await expectReply(conn, ['SET', sameSlot, 'mynewtext'], '+OK\r\n')
    await expectReply(conn, ['LCS', first, sameSlot], '$6\r\nmytext\r\n')
  })
})
