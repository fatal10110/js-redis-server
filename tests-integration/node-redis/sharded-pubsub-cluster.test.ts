import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  findNodeRedisSlotOwnerEndpoint,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe(`Sharded Pub/Sub cluster integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let publisher: RedisClusterType

  before(async () => {
    publisher = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('routes shard subscriptions and publishes by channel slot', async () => {
    const channel = `sharded-pubsub:{${randomKey()}}`
    let resolveMessage: (value: { message: string; channel: string }) => void
    const received = new Promise<{ message: string; channel: string }>(
      resolve => {
        resolveMessage = resolve
      },
    )

    const shardNumSub = (): Promise<unknown> =>
      publisher.sendCommand(channel, true, ['PUBSUB', 'SHARDNUMSUB', channel])

    try {
      await publisher.sSubscribe(channel, (message: string, ch: string) => {
        resolveMessage({ message, channel: ch })
      })
      assert.deepStrictEqual(await shardNumSub(), [channel, 1])

      assert.strictEqual(await publisher.sPublish(channel, 'hello-shard'), 1)
      assert.deepStrictEqual(await received, {
        message: 'hello-shard',
        channel,
      })

      await publisher.sUnsubscribe(channel)
      assert.deepStrictEqual(await shardNumSub(), [channel, 0])
    } finally {
      await publisher.sUnsubscribe(channel).catch(() => undefined)
    }
  })

  test('rejects sharded Pub/Sub commands routed to the wrong slot', async () => {
    const localChannel = `sharded-pubsub:{${randomKey()}}:local`
    const localOwner = await findNodeRedisSlotOwnerEndpoint(
      publisher,
      localChannel,
    )
    const remoteChannel = await findChannelNotOwnedByPort(
      publisher,
      localOwner.port,
    )
    const directClient = await connectToNodeRedisSlotOwner(
      publisher,
      localChannel,
    )

    try {
      await assert.rejects(
        () =>
          directClient.sendCommand(['SPUBLISH', remoteChannel, 'wrong-node']),
        error => {
          assert.ok(error instanceof Error)
          assert.match(error.message, /^MOVED /)
          return true
        },
      )

      await assert.rejects(
        () =>
          directClient.sendCommand(['SSUBSCRIBE', localChannel, remoteChannel]),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'PUBSUB',
          'SHARDNUMSUB',
          remoteChannel,
        ]),
        [remoteChannel, 0],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'PUBSUB',
          'SHARDNUMSUB',
          localChannel,
          remoteChannel,
        ]),
        [localChannel, 0, remoteChannel, 0],
      )
    } finally {
      directClient.destroy()
    }
  })
})

async function findChannelNotOwnedByPort(
  cluster: RedisClusterType,
  port: number,
): Promise<string> {
  for (let attempt = 0; attempt < 1000; attempt++) {
    const channel = `sharded-pubsub:{${randomKey()}}:remote`
    const owner = await findNodeRedisSlotOwnerEndpoint(cluster, channel)
    if (owner.port !== port) {
      return channel
    }
  }

  throw new Error(`Could not find a shard channel not owned by port ${port}`)
}
