import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import clusterKeySlot from 'cluster-key-slot'
import { TestRunner } from '../test-config'
import {
  connectToSlotOwner,
  errorWithMessage,
  findSlotOwner,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe(`Cluster protocol integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('cluster-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('direct node connections return MOVED for keys owned by another node', async () => {
    const { localKey, remoteKey, remoteHost, remotePort } =
      await findDifferentNodeKeys(redisClient!)
    const directClient = await connectToSlotOwner(redisClient!, localKey)

    try {
      await assert.rejects(
        () => directClient.get(remoteKey),
        (error: unknown) => {
          assert.ok(error instanceof Error)
          assert.strictEqual(
            error.message,
            `MOVED ${clusterKeySlot(remoteKey)} ${remoteHost}:${remotePort}`,
          )
          return true
        },
      )
    } finally {
      directClient.disconnect()
    }
  })

  test('CLUSTER arity and subcommand errors match Redis', async () => {
    const directClient = await connectToSlotOwner(
      redisClient!,
      `{cluster:${randomKey()}}:probe`,
    )

    try {
      await assert.rejects(
        () => directClient.call('CLUSTER'),
        errorWithMessage("ERR wrong number of arguments for 'cluster' command"),
      )
      await assert.rejects(
        () => directClient.call('CLUSTER', 'nope'),
        errorWithMessage("ERR unknown subcommand 'nope'. Try CLUSTER HELP."),
      )

      for (const subcommand of ['slots', 'shards', 'nodes', 'info', 'myid']) {
        await assert.rejects(
          () => directClient.call('CLUSTER', subcommand, 'extra'),
          errorWithMessage(
            `ERR wrong number of arguments for 'cluster|${subcommand}' command`,
          ),
        )
      }
    } finally {
      directClient.disconnect()
    }
  })
})

async function findDifferentNodeKeys(cluster: Cluster): Promise<{
  localKey: string
  remoteKey: string
  remoteHost: string
  remotePort: number
}> {
  const localKey = `{moved-local:${randomKey()}}:key`
  const [localHost, localPort] = await findSlotOwner(cluster, localKey)

  for (let index = 0; index < 10000; index++) {
    const remoteKey = `{moved-remote:${randomKey()}:${index}}:key`
    const [remoteHost, remotePort] = await findSlotOwner(cluster, remoteKey)
    if (remoteHost !== localHost || remotePort !== localPort) {
      return { localKey, remoteKey, remoteHost, remotePort }
    }
  }

  throw new Error('Could not find keys owned by different cluster nodes')
}
