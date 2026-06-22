import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'
import { Cluster } from 'ioredis'
import clusterKeySlot from 'cluster-key-slot'
import { TestRunner } from '../test-config'
import {
  commandFrame,
  connectToEndpoint,
  connectToSlotOwner,
  eventually,
  errorWithMessage,
  findSlotMasterAndReplica,
  findSlotOwner,
  randomKey,
} from '../utils'
import {
  RawRedisConnection,
  respMapGet,
  respNumber,
  respText,
} from '../raw-tcp/raw-connection'

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

  test('Lua redis.call and redis.pcall non-local key errors match Redis', async () => {
    const { localKey, remoteKey } = await findDifferentNodeKeys(redisClient!)
    const directClient = await connectToSlotOwner(redisClient!, localKey)
    const callScript = "return redis.call('get', ARGV[1])"
    const callSha = createHash('sha1').update(callScript).digest('hex')

    try {
      await assert.rejects(
        () => directClient.eval(callScript, 0, remoteKey),
        errorWithMessage(
          `ERR Script attempted to access a non local key in a cluster node script: ${callSha}, on @user_script:1.`,
        ),
      )
      await assert.rejects(
        () =>
          directClient.eval("return redis.pcall('get', ARGV[1])", 0, remoteKey),
        errorWithMessage(
          'ERR Script attempted to access a non local key in a cluster node',
        ),
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

  test('HELLO reports master and replica roles for direct node connections', async () => {
    const key = `{hello-role:${randomKey()}}:probe`
    const { master, replica } = await findSlotMasterAndReplica(
      redisClient!,
      key,
    )
    const masterClient = await connectToEndpoint(master)
    const replicaClient = await connectToEndpoint(replica)

    try {
      const masterHello = (await masterClient.call('HELLO', '2')) as unknown[]
      const replicaHello = (await replicaClient.call('HELLO', '2')) as unknown[]

      assertHelloEntry(masterHello, 'role', 'master')
      assertHelloEntry(replicaHello, 'role', 'replica')
    } finally {
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

      assert.strictEqual(await replicaClient.readonly(), 'OK')
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

      assert.strictEqual(await replicaClient.readwrite(), 'OK')
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
      assert.strictEqual(await replicaClient.readonly(), 'OK')
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

  test('CLUSTER SHARDS returns RESP3 maps after HELLO 3', async () => {
    const port = testRunner.getClusterPorts()[0]
    assert.notStrictEqual(port, undefined)
    const connection = await RawRedisConnection.connect('127.0.0.1', port!)

    try {
      connection.write(commandFrame('HELLO', '3'))
      assert.ok((await connection.readFrame()) instanceof Map)

      connection.write(commandFrame('CLUSTER', 'SHARDS'))
      const reply = await connection.readFrame()
      assert.ok(Array.isArray(reply))
      assert.ok(reply.length > 0)

      const shard = reply[0]
      assert.ok(shard instanceof Map)
      assert.ok(Array.isArray(respMapGet(shard, 'slots')))

      const nodes = respMapGet(shard, 'nodes')
      assert.ok(Array.isArray(nodes))
      assert.ok(nodes[0] instanceof Map)
      assert.ok(respText(respMapGet(nodes[0], 'id')).length > 0)
      assert.ok(respNumber(respMapGet(nodes[0], 'port')) > 0)
      assert.match(respText(respMapGet(nodes[0], 'role')), /^(master|replica)$/)
    } finally {
      connection.close()
    }
  })

  test('CLUSTER NODES reports bus port as client port + 10000', async () => {
    const port = testRunner.getClusterPorts()[0]
    assert.notStrictEqual(port, undefined)
    const connection = await RawRedisConnection.connect('127.0.0.1', port!)

    try {
      connection.write(commandFrame('CLUSTER', 'NODES'))
      const text = respText(await connection.readFrame())
      const lines = text.split('\n').filter(line => line.trim().length > 0)
      assert.ok(lines.length > 0)

      for (const line of lines) {
        const address = line.split(' ')[1]
        const match = address.match(/^(.+):(\d+)@(\d+)$/)
        assert.ok(match, `address field malformed: ${address}`)
        assert.strictEqual(Number(match[3]), Number(match[2]) + 10000)
      }
    } finally {
      connection.close()
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

function assertHelloEntry(
  reply: unknown[],
  key: string,
  expected: string | number,
): void {
  const index = reply.indexOf(key)
  assert.notStrictEqual(index, -1)
  assert.strictEqual(reply[index + 1], expected)
}
