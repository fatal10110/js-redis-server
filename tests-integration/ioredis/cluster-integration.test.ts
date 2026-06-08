import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import clusterKeySlot from 'cluster-key-slot'
import { TestRunner } from '../test-config'
import {
  connectToEndpoint,
  connectToSlotOwner,
  eventually,
  errorWithMessage,
  findSlotMasterAndReplica,
  findSlotOwner,
  randomKey,
} from '../utils'

const testRunner = new TestRunner()

describe(`Cluster protocol integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('cluster-integration', {
      replicasPerMaster: 1,
    })
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

  test('direct replica connections redirect keyed commands to the master', async () => {
    const key = `{replica:${randomKey()}}:key`
    const { slot, master, replica } = await findSlotMasterAndReplica(
      redisClient!,
      key,
    )
    const masterClient = await connectToEndpoint(master)
    const replicaClient = await connectToEndpoint(replica)
    const movedError = errorWithMessage(
      `MOVED ${slot} ${master.host}:${master.port}`,
    )

    try {
      await masterClient.set(key, 'original')

      await assert.rejects(() => replicaClient.get(key), movedError)
      await assert.rejects(() => replicaClient.set(key, 'replica'), movedError)
      assert.strictEqual(await masterClient.get(key), 'original')
    } finally {
      await masterClient.del(key)
      masterClient.disconnect()
      replicaClient.disconnect()
    }
  })

  test('READONLY lets direct replica connections serve readonly commands for master slots', async () => {
    const tag = `readonly:${randomKey()}`
    const key = `{${tag}}:key`
    const siblingKey = `{${tag}}:sibling`
    const { slot, master, replica } = await findSlotMasterAndReplica(
      redisClient!,
      key,
    )
    const wrongSlotKey = await findKeyOwnedByDifferentMaster(
      redisClient!,
      master,
    )
    const masterClient = await connectToEndpoint(master)
    const replicaClient = await connectToEndpoint(replica)

    try {
      await masterClient.set(key, 'original')

      await assert.rejects(
        () => replicaClient.get(key),
        errorWithMessage(`MOVED ${slot} ${master.host}:${master.port}`),
      )

      assert.strictEqual(await replicaClient.call('READONLY'), 'OK')
      await eventually(async () => {
        assert.strictEqual(await replicaClient.get(key), 'original')
      })
      assert.deepStrictEqual(await replicaClient.mget(key, siblingKey), [
        'original',
        null,
      ])

      await assert.rejects(
        () => replicaClient.set(key, 'replica'),
        errorWithMessage(`MOVED ${slot} ${master.host}:${master.port}`),
      )
      assert.strictEqual(await masterClient.get(key), 'original')

      await assert.rejects(
        () => replicaClient.get(wrongSlotKey.key),
        errorWithMessage(
          `MOVED ${wrongSlotKey.slot} ${wrongSlotKey.host}:${wrongSlotKey.port}`,
        ),
      )

      assert.strictEqual(await replicaClient.call('READWRITE'), 'OK')
      await assert.rejects(
        () => replicaClient.get(key),
        errorWithMessage(`MOVED ${slot} ${master.host}:${master.port}`),
      )
    } finally {
      await masterClient.del(key, siblingKey)
      masterClient.disconnect()
      replicaClient.disconnect()
    }
  })

  test('READONLY and READWRITE arity errors match Redis', async () => {
    const key = `{readonly-arity:${randomKey()}}:key`
    const { replica } = await findSlotMasterAndReplica(redisClient!, key)
    const replicaClient = await connectToEndpoint(replica)

    try {
      await assert.rejects(
        () => replicaClient.call('READONLY', 'extra'),
        errorWithMessage(
          "ERR wrong number of arguments for 'readonly' command",
        ),
      )
      await assert.rejects(
        () => replicaClient.call('READWRITE', 'extra'),
        errorWithMessage(
          "ERR wrong number of arguments for 'readwrite' command",
        ),
      )
    } finally {
      replicaClient.disconnect()
    }
  })

  test('RESET clears READONLY replica mode', async () => {
    const key = `{readonly-reset:${randomKey()}}:key`
    const { slot, master, replica } = await findSlotMasterAndReplica(
      redisClient!,
      key,
    )
    const masterClient = await connectToEndpoint(master)
    const replicaClient = await connectToEndpoint(replica)

    try {
      await masterClient.set(key, 'value')
      assert.strictEqual(await replicaClient.call('READONLY'), 'OK')
      await eventually(async () => {
        assert.strictEqual(await replicaClient.get(key), 'value')
      })

      assert.strictEqual(await replicaClient.call('RESET'), 'RESET')
      await assert.rejects(
        () => replicaClient.get(key),
        errorWithMessage(`MOVED ${slot} ${master.host}:${master.port}`),
      )
    } finally {
      await masterClient.del(key)
      masterClient.disconnect()
      replicaClient.disconnect()
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

async function findKeyOwnedByDifferentMaster(
  cluster: Cluster,
  localMaster: { host: string; port: number },
): Promise<{
  key: string
  slot: number
  host: string
  port: number
}> {
  for (let index = 0; index < 10000; index++) {
    const key = `{readonly-wrong:${randomKey()}:${index}}:key`
    const [host, port] = await findSlotOwner(cluster, key)
    if (host !== localMaster.host || port !== localMaster.port) {
      return { key, slot: clusterKeySlot(key), host, port }
    }
  }

  throw new Error('Could not find key owned by a different master')
}

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
