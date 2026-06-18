import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import {
  commandFrame,
  connectToSlotOwner,
  errorWithMessage,
  findSlotOwner,
  randomKey,
} from '../utils'
import {
  RawRedisConnection,
  type RespWireValue,
} from '../raw-tcp/raw-connection'

const testRunner = new TestRunner()

describe(`Sharded Pub/Sub cluster integration (${testRunner.getBackendName()})`, () => {
  let publisher: Cluster
  const rawConnections: RawRedisConnection[] = []

  before(async () => {
    publisher = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    for (const connection of rawConnections) {
      connection.close()
    }
    rawConnections.length = 0
    await testRunner.cleanup()
  })

  test('routes shard subscriptions and publishes by channel slot', async () => {
    const channel = `sharded-pubsub:{${randomKey()}}`
    const [host, port] = await findSlotOwner(publisher, channel)
    const owner = await connectToSlotOwner(publisher, channel)
    const subscriber = await RawRedisConnection.connect(host, port)
    rawConnections.push(subscriber)

    try {
      subscriber.write(commandFrame('SSUBSCRIBE', channel))
      assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
        'ssubscribe',
        channel,
        1,
      ])
      assert.deepStrictEqual(
        await owner.call('PUBSUB', 'SHARDNUMSUB', channel),
        [channel, 1],
      )

      const message = subscriber.readFrame()
      assert.strictEqual(
        await owner.call('SPUBLISH', channel, 'hello-shard'),
        1,
      )
      assert.deepStrictEqual(normalizeFrame(await message), [
        'smessage',
        channel,
        'hello-shard',
      ])

      subscriber.write(commandFrame('SUNSUBSCRIBE', channel))
      assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
        'sunsubscribe',
        channel,
        0,
      ])
      assert.deepStrictEqual(
        await owner.call('PUBSUB', 'SHARDNUMSUB', channel),
        [channel, 0],
      )
    } finally {
      owner.disconnect()
    }
  })

  test('rejects sharded Pub/Sub commands routed to the wrong slot', async () => {
    const localChannel = `sharded-pubsub:{${randomKey()}}:local`
    const [, localPort] = await findSlotOwner(publisher, localChannel)
    const remoteChannel = await findChannelNotOwnedByPort(publisher, localPort)
    const directClient = await connectToSlotOwner(publisher, localChannel)

    try {
      await assert.rejects(
        directClient.spublish(remoteChannel, 'wrong-node'),
        error => {
          assert.ok(error instanceof Error)
          assert.match(error.message, /^MOVED /)
          return true
        },
      )

      await assert.rejects(
        directClient.call('SSUBSCRIBE', localChannel, remoteChannel),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
    } finally {
      directClient.disconnect()
    }
  })
})

async function findChannelNotOwnedByPort(
  cluster: Cluster,
  port: number,
): Promise<string> {
  for (let attempt = 0; attempt < 1000; attempt++) {
    const channel = `sharded-pubsub:{${randomKey()}}:remote`
    const [, candidatePort] = await findSlotOwner(cluster, channel)
    if (candidatePort !== port) {
      return channel
    }
  }

  throw new Error(`Could not find a shard channel not owned by port ${port}`)
}

function normalizeFrame(value: RespWireValue): RespWireValue {
  if (Buffer.isBuffer(value)) {
    return value.toString()
  }

  if (Array.isArray(value)) {
    return value.map(normalizeFrame)
  }

  return value
}
