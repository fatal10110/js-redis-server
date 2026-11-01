import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { needsCluster, TestRunner } from '../test-config'
import { activeProfile, commandFrame } from '../utils'
import { RawRedisConnection, respText } from './raw-connection'

/**
 * `call()` hides a command from MONITOR only when its command-table entry is
 * flagged `admin` (or `skip_monitor`). From 7.0 the entry is the
 * subcommand's, and only some CLUSTER / CONFIG subcommands are admin: CLUSTER
 * INFO and CONFIG HELP are shown, CLUSTER MEET and CONFIG GET are not. On 6.2
 * the `cluster` and `config` containers are admin, so every call is hidden.
 * Arity errors and unknown subcommands are refused before call() and never
 * shown. Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0 (#613).
 */
const testRunner = new TestRunner()
const legacy = activeProfile === 'redis-6.2'

type Step = {
  argv: string[]
  /** Shown on 7.0+; nothing in this list is shown on 6.2. */
  shown: boolean
}

const STEPS: Step[] = [
  { argv: ['CLUSTER', 'INFO'], shown: true },
  { argv: ['CLUSTER', 'KEYSLOT', 'foo'], shown: true },
  { argv: ['CLUSTER', 'HELP'], shown: true },
  { argv: ['CLUSTER', 'MYID'], shown: true },
  { argv: ['CLUSTER', 'NODES'], shown: true },
  { argv: ['CLUSTER', 'SLOTS'], shown: true },
  { argv: ['CLUSTER', 'SHARDS'], shown: true },
  { argv: ['CLUSTER', 'COUNTKEYSINSLOT', '0'], shown: true },
  { argv: ['CLUSTER', 'GETKEYSINSLOT', '0', '1'], shown: true },
  { argv: ['CLUSTER', 'LINKS'], shown: true },
  // Admin subcommands, including ones without a mock command-table entry.
  { argv: ['CLUSTER', 'COUNT-FAILURE-REPORTS', 'abc'], shown: false },
  { argv: ['CLUSTER', 'MEET', '127.0.0.1', '1'], shown: false },
  { argv: ['CLUSTER', 'FORGET', 'abc'], shown: false },
  { argv: ['CLUSTER', 'SETSLOT', '0', 'STABLE'], shown: false },
  { argv: ['CLUSTER', 'ADDSLOTS', '0'], shown: false },
  { argv: ['CLUSTER', 'RESET'], shown: false },
  // Refused before call(): unknown subcommand, arity.
  { argv: ['CLUSTER', 'BOGUS'], shown: false },
  { argv: ['CLUSTER', 'INFO', 'extra'], shown: false },
  { argv: ['CLUSTER', 'KEYSLOT'], shown: false },
  { argv: ['CONFIG', 'HELP'], shown: true },
  { argv: ['CONFIG', 'GET', 'maxmemory'], shown: false },
  { argv: ['CONFIG', 'SET', 'maxmemory', '0'], shown: false },
  { argv: ['CONFIG', 'RESETSTAT'], shown: false },
  { argv: ['CONFIG', 'BOGUS'], shown: false },
  { argv: ['CONFIG', 'GET'], shown: false },
  // Other containers' admin subcommands without a mock table entry.
  { argv: ['ACL', 'WHOAMI'], shown: true },
  { argv: ['ACL', 'USERS'], shown: false },
  { argv: ['ACL', 'GETUSER', 'default'], shown: false },
  { argv: ['CLIENT', 'ID'], shown: true },
  { argv: ['CLIENT', 'UNBLOCK', '0'], shown: false },
]

type Expected = { argv: string[]; source?: 'lua' }

describe(`Raw TCP MONITOR of CLUSTER and CONFIG subcommands (${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(targetPort = port): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', targetPort)
    connections.push(connection)
    return connection
  }

  test('shows non-admin subcommands from 7.0 and hides admin ones', async () => {
    const monitor = await connect()
    const actor = await connect()

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    const expected: Expected[] = []
    for (const { argv, shown } of STEPS) {
      actor.write(commandFrame(...argv))
      await actor.readFrame()
      if (shown && !legacy) {
        expected.push({ argv })
      }
    }

    // A queued admin subcommand stays hidden when EXEC runs it.
    const transaction = [
      ['MULTI'],
      ['CLUSTER', 'KEYSLOT', 'bar'],
      ['CONFIG', 'GET', 'port'],
      ['EXEC'],
    ]
    for (const argv of transaction) {
      actor.write(commandFrame(...argv))
      await actor.readFrame()
    }
    expected.push({ argv: ['MULTI'] })
    if (!legacy) {
      expected.push({ argv: ['CLUSTER', 'KEYSLOT', 'bar'] })
    }
    expected.push({ argv: ['EXEC'] })

    // CLUSTER runs from a script (source `lua`); CONFIG is noscript.
    const clusterScript = [
      'EVAL',
      "return redis.pcall('CLUSTER','KEYSLOT','sx')",
      '0',
    ]
    actor.write(commandFrame(...clusterScript))
    await actor.readFrame()
    expected.push({ argv: clusterScript })
    if (!legacy) {
      expected.push({ argv: ['CLUSTER', 'KEYSLOT', 'sx'], source: 'lua' })
    }
    const configScript = [
      'EVAL',
      "return redis.pcall('CONFIG','GET','port')",
      '0',
    ]
    actor.write(commandFrame(...configScript))
    await actor.readFrame()
    expected.push({ argv: configScript })

    actor.write(commandFrame('PING', 'mark'))
    assert.deepStrictEqual(
      await actor.readRawFrame(),
      Buffer.from('$4\r\nmark\r\n'),
    )
    expected.push({ argv: ['PING', 'mark'] })

    // Read up to the PING mark, so a missing line fails the comparison
    // instead of waiting for a line that never comes.
    const lines: string[] = []
    for (;;) {
      const line = respText(await monitor.readFrame())
      lines.push(line)
      if (line.endsWith(' "PING" "mark"')) {
        break
      }
    }
    assert.deepStrictEqual(
      lines.map(parseMonitorLine),
      expected.map(({ argv, source }) => ({
        argv,
        lua: source === 'lua',
      })),
      `monitor lines:\n${lines.join('\n')}`,
    )
  })

  test('shows CLUSTER INFO on a cluster node from 7.0', async t => {
    if (needsCluster.skip) return t.skip(needsCluster.skip)
    await testRunner.setupIoredisCluster('monitor-cluster-info')
    const directPort = testRunner.getClusterPorts()[0]
    assert.notStrictEqual(directPort, undefined)
    const monitor = await connect(directPort)
    const actor = await connect(directPort)

    monitor.write(commandFrame('MONITOR'))
    assert.deepStrictEqual(await monitor.readRawFrame(), Buffer.from('+OK\r\n'))

    actor.write(commandFrame('CLUSTER', 'INFO'))
    assert.match(respText(await actor.readFrame()), /^cluster_state:/)
    actor.write(commandFrame('CLUSTER', 'COUNT-FAILURE-REPORTS', 'abc'))
    await actor.readFrame()
    actor.write(commandFrame('PING', 'monitor-cluster-info'))
    assert.strictEqual(
      respText(await actor.readFrame()),
      'monitor-cluster-info',
    )

    // Other clients (cluster clients refreshing slots) may share the node,
    // so only this test's CLUSTER calls are asserted, up to the mark.
    const seen: string[][] = []
    for (;;) {
      const { argv } = parseMonitorLine(respText(await monitor.readFrame()))
      if (argv[0] === 'PING' && argv[1] === 'monitor-cluster-info') {
        break
      }
      if (
        argv[0]?.toUpperCase() === 'CLUSTER' &&
        ['INFO', 'COUNT-FAILURE-REPORTS'].includes(argv[1]?.toUpperCase())
      ) {
        seen.push(argv)
      }
    }
    assert.deepStrictEqual(seen, legacy ? [] : [['CLUSTER', 'INFO']])
  })
})

/** The argv of a MONITOR line and whether a script ran it. */
function parseMonitorLine(line: string): { argv: string[]; lua: boolean } {
  const match = /^\d+\.\d{6} \[\d+ ([^\]]+)\] (.*)$/.exec(line)
  assert.ok(match, `not a monitor line: ${line}`)
  const argv = Array.from(match[2].matchAll(/"((?:[^"\\]|\\.)*)"/g), m => m[1])
  return { argv, lua: match[1] === 'lua' }
}
