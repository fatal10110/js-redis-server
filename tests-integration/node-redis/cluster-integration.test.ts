import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'
import clusterKeySlot from 'cluster-key-slot'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisEndpoint,
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  eventually,
  findNodeRedisSlotMasterAndReplica,
  findNodeRedisSlotOwnerEndpoint,
  randomKey,
  type RedisEndpoint,
} from '../utils'

const testRunner = new TestRunner()

describe(`Cluster protocol integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster({
      replicasPerMaster: 1,
    })) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('direct node connections return MOVED for keys owned by another node', async () => {
    const { localKey, remoteKey, remoteHost, remotePort } =
      await findDifferentNodeKeys(redisClient)
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      localKey,
    )

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
      directClient.destroy()
    }
  })

  test('Lua redis.call and redis.pcall non-local key errors match Redis', async () => {
    const { localKey, remoteKey } = await findDifferentNodeKeys(redisClient)
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      localKey,
    )
    const callScript = "return redis.call('get', ARGV[1])"
    const callSha = createHash('sha1').update(callScript).digest('hex')

    try {
      await assert.rejects(
        () => directClient.eval(callScript, { arguments: [remoteKey] }),
        errorWithMessage(
          `ERR Script attempted to access a non local key in a cluster node script: ${callSha}, on @user_script:1.`,
        ),
      )
      await assert.rejects(
        () =>
          directClient.eval("return redis.pcall('get', ARGV[1])", {
            arguments: [remoteKey],
          }),
        errorWithMessage(
          'ERR Script attempted to access a non local key in a cluster node',
        ),
      )
    } finally {
      directClient.destroy()
    }
  })

  test('direct replica connections redirect keyed commands to the master', async () => {
    const key = `{replica:${randomKey()}}:key`
    const { slot, master, replica } = await findNodeRedisSlotMasterAndReplica(
      redisClient,
      key,
    )
    const masterClient = await connectToNodeRedisEndpoint(master)
    const replicaClient = await connectToNodeRedisEndpoint(replica)
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
      masterClient.destroy()
      replicaClient.destroy()
    }
  })

  test('HELLO reports master and replica roles for direct node connections', async () => {
    const key = `{hello-role:${randomKey()}}:probe`
    const { master, replica } = await findNodeRedisSlotMasterAndReplica(
      redisClient,
      key,
    )
    const masterClient = await connectToNodeRedisEndpoint(master)
    const replicaClient = await connectToNodeRedisEndpoint(replica)

    try {
      const masterHello = (await masterClient.sendCommand([
        'HELLO',
        '2',
      ])) as unknown[]
      const replicaHello = (await replicaClient.sendCommand([
        'HELLO',
        '2',
      ])) as unknown[]

      assertHelloEntry(masterHello, 'role', 'master')
      assertHelloEntry(replicaHello, 'role', 'replica')
    } finally {
      masterClient.destroy()
      replicaClient.destroy()
    }
  })

  test('READONLY lets direct replica connections serve readonly commands for master slots', async () => {
    const tag = `readonly:${randomKey()}`
    const key = `{${tag}}:key`
    const siblingKey = `{${tag}}:sibling`
    const { slot, master, replica } = await findNodeRedisSlotMasterAndReplica(
      redisClient,
      key,
    )
    const wrongSlotKey = await findKeyOwnedByDifferentMaster(
      redisClient,
      master,
    )
    const masterClient = await connectToNodeRedisEndpoint(master)
    const replicaClient = await connectToNodeRedisEndpoint(replica)

    try {
      await masterClient.set(key, 'original')

      await assert.rejects(
        () => replicaClient.get(key),
        errorWithMessage(`MOVED ${slot} ${master.host}:${master.port}`),
      )

      assert.strictEqual(await replicaClient.sendCommand(['READONLY']), 'OK')
      await eventually(async () => {
        assert.strictEqual(await replicaClient.get(key), 'original')
      })
      assert.deepStrictEqual(await replicaClient.mGet([key, siblingKey]), [
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

      assert.strictEqual(await replicaClient.sendCommand(['READWRITE']), 'OK')
      await assert.rejects(
        () => replicaClient.get(key),
        errorWithMessage(`MOVED ${slot} ${master.host}:${master.port}`),
      )
    } finally {
      await masterClient.del([key, siblingKey])
      masterClient.destroy()
      replicaClient.destroy()
    }
  })

  test('READONLY and READWRITE arity errors match Redis', async () => {
    const key = `{readonly-arity:${randomKey()}}:key`
    const { replica } = await findNodeRedisSlotMasterAndReplica(
      redisClient,
      key,
    )
    const replicaClient = await connectToNodeRedisEndpoint(replica)

    try {
      await assert.rejects(
        () => replicaClient.sendCommand(['READONLY', 'extra']),
        errorWithMessage(
          "ERR wrong number of arguments for 'readonly' command",
        ),
      )
      await assert.rejects(
        () => replicaClient.sendCommand(['READWRITE', 'extra']),
        errorWithMessage(
          "ERR wrong number of arguments for 'readwrite' command",
        ),
      )
    } finally {
      replicaClient.destroy()
    }
  })

  test('RESET clears READONLY replica mode', async () => {
    const key = `{readonly-reset:${randomKey()}}:key`
    const { slot, master, replica } = await findNodeRedisSlotMasterAndReplica(
      redisClient,
      key,
    )
    const masterClient = await connectToNodeRedisEndpoint(master)
    const replicaClient = await connectToNodeRedisEndpoint(replica)

    try {
      await masterClient.set(key, 'value')
      assert.strictEqual(await replicaClient.sendCommand(['READONLY']), 'OK')
      await eventually(async () => {
        assert.strictEqual(await replicaClient.get(key), 'value')
      })

      assert.strictEqual(await replicaClient.sendCommand(['RESET']), 'RESET')
      await assert.rejects(
        () => replicaClient.get(key),
        errorWithMessage(`MOVED ${slot} ${master.host}:${master.port}`),
      )
    } finally {
      await masterClient.del(key)
      masterClient.destroy()
      replicaClient.destroy()
    }
  })

  test('CLUSTER arity and subcommand errors match Redis', async () => {
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      `{cluster:${randomKey()}}:probe`,
    )

    try {
      await assert.rejects(
        () => directClient.sendCommand(['CLUSTER']),
        errorWithMessage("ERR wrong number of arguments for 'cluster' command"),
      )
      await assert.rejects(
        () => directClient.sendCommand(['CLUSTER', 'nope']),
        errorWithMessage("ERR unknown subcommand 'nope'. Try CLUSTER HELP."),
      )

      for (const subcommand of ['slots', 'shards', 'nodes', 'info', 'myid']) {
        await assert.rejects(
          () => directClient.sendCommand(['CLUSTER', subcommand, 'extra']),
          errorWithMessage(
            `ERR wrong number of arguments for 'cluster|${subcommand}' command`,
          ),
        )
      }
    } finally {
      directClient.destroy()
    }
  })

  test('CLUSTER SHARDS returns structured shard metadata', async () => {
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      `{cluster-shards:${randomKey()}}:probe`,
    )

    try {
      // node-redis decodes CLUSTER SHARDS (RESP3) into objects.
      const reply = (await directClient.sendCommand([
        'CLUSTER',
        'SHARDS',
      ])) as Array<Record<string, unknown>>
      assert.ok(Array.isArray(reply))
      assert.ok(reply.length > 0)

      const shard = reply[0]
      assert.ok(Array.isArray(shard.slots))

      const nodes = shard.nodes as Array<Record<string, unknown>>
      assert.ok(Array.isArray(nodes))
      assert.ok(String(nodes[0].id).length > 0)
      assert.ok(Number(nodes[0].port) > 0)
      assert.match(String(nodes[0].role), /^(master|replica)$/)
    } finally {
      directClient.destroy()
    }
  })

  test('CLUSTER NODES reports bus port as client port + 10000', async () => {
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      `{cluster-nodes:${randomKey()}}:probe`,
    )

    try {
      const text = (await directClient.sendCommand([
        'CLUSTER',
        'NODES',
      ])) as string
      const lines = text.split('\n').filter(line => line.trim().length > 0)
      assert.ok(lines.length > 0)

      for (const line of lines) {
        const address = line.split(' ')[1]
        const match = address.match(/^(.+):(\d+)@(\d+)$/)
        assert.ok(match, `address field malformed: ${address}`)
        assert.strictEqual(Number(match[3]), Number(match[2]) + 10000)
      }
    } finally {
      directClient.destroy()
    }
  })
})

async function findKeyOwnedByDifferentMaster(
  cluster: RedisClusterType,
  localMaster: RedisEndpoint,
): Promise<{ key: string; slot: number; host: string; port: number }> {
  for (let index = 0; index < 10000; index++) {
    const key = `{readonly-wrong:${randomKey()}:${index}}:key`
    const owner = await findNodeRedisSlotOwnerEndpoint(cluster, key)
    if (owner.host !== localMaster.host || owner.port !== localMaster.port) {
      return {
        key,
        slot: clusterKeySlot(key),
        host: owner.host,
        port: owner.port,
      }
    }
  }

  throw new Error('Could not find key owned by a different master')
}

async function findDifferentNodeKeys(cluster: RedisClusterType): Promise<{
  localKey: string
  remoteKey: string
  remoteHost: string
  remotePort: number
}> {
  const localKey = `{moved-local:${randomKey()}}:key`
  const local = await findNodeRedisSlotOwnerEndpoint(cluster, localKey)

  for (let index = 0; index < 10000; index++) {
    const remoteKey = `{moved-remote:${randomKey()}:${index}}:key`
    const remote = await findNodeRedisSlotOwnerEndpoint(cluster, remoteKey)
    if (remote.host !== local.host || remote.port !== local.port) {
      return {
        localKey,
        remoteKey,
        remoteHost: remote.host,
        remotePort: remote.port,
      }
    }
  }

  throw new Error('Could not find keys owned by different cluster nodes')
}

function assertHelloEntry(
  reply: unknown[],
  key: string,
  expected: string | number,
): void {
  const index = reply.indexOf(key)
  assert.notStrictEqual(index, -1)
  assert.strictEqual(reply[index + 1], expected)
}
