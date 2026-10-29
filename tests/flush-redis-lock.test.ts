import { describe, test } from 'node:test'
import assert from 'node:assert'
import { spawn } from 'node:child_process'
import { constants } from 'node:os'
import path from 'node:path'
import { Redis } from 'ioredis'
import {
  describeHolder,
  exitCodeFor,
  otherRunHolders,
  parseClientList,
  parseCommandLine,
  RUN_LOCK_PREFIX,
  runLockName,
} from '../scripts/flush-redis-lock'
import { HINT } from '../scripts/flush-redis-topology'
import { createRedisServer } from '../src/index'

// `clean:redis` takes a run lock on the real-backend stack before it flushes,
// and refuses while another run holds it (#542). The CLIENT LIST lines below
// are what real Redis 7.0.15 prints.

const OWN = runLockName('ci-host', 4242, 'aaaa0000')
const OTHER = runLockName('dev box', 77, 'bbbb1111')

function clientLine(id: number, name: string, age = 3): string {
  return (
    `id=${id} addr=127.0.0.1:${50000 + id} laddr=127.0.0.1:30000 fd=${id + 5} ` +
    `name=${name} age=${age} idle=0 flags=N db=0 sub=0 psub=0 ssub=0 ` +
    `multi=-1 qbuf=26 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=16384 ` +
    `rbp=16384 obl=0 oll=0 omem=0 tot-mem=37658 events=r cmd=client|list ` +
    `user=default redir=-1 resp=2`
  )
}

describe('clean:redis run lock (#542)', () => {
  test('lock names are valid client names that say where the run is from', () => {
    assert.strictEqual(OWN, `${RUN_LOCK_PREFIX}ci-host:4242:aaaa0000`)
    // CLIENT SETNAME rejects spaces; anything odd in a hostname is replaced.
    assert.strictEqual(OTHER, `${RUN_LOCK_PREFIX}dev_box:77:bbbb1111`)
    assert.match(runLockName('', 1, 'x'), /^js-redis-server-test-run:_:1:x$/)
  })

  test('parses CLIENT LIST lines', () => {
    const [client] = parseClientList(clientLine(9, OTHER, 12) + '\n')
    assert.deepStrictEqual(client, {
      id: '9',
      addr: '127.0.0.1:50009',
      name: OTHER,
      age: 12,
    })
    assert.deepStrictEqual(parseClientList('id=1 addr=a name=\n'), [
      { id: '1', addr: 'a', name: '', age: undefined },
    ])
  })

  test('finds only other runs among the clients', () => {
    const list = [
      clientLine(3, ''),
      clientLine(4, 'primary-xyz'),
      clientLine(5, OWN),
      clientLine(6, OTHER),
    ].join('\n')

    assert.deepStrictEqual(
      otherRunHolders(list, OWN).map(c => c.name),
      [OTHER],
    )
    // Our own name, even on two connections to one server, is not contention.
    assert.deepStrictEqual(
      otherRunHolders([clientLine(5, OWN), clientLine(7, OWN)].join('\n'), OWN),
      [],
    )
    assert.strictEqual(
      describeHolder(parseClientList(clientLine(6, OTHER, 40))[0]),
      `held by ${OTHER} (from 127.0.0.1:50006, connected 40s ago)`,
    )
  })

  test('the refusal hint names the fix', () => {
    assert.match(
      HINT.locked,
      /docs\/TEST-INTEGRATION\.md#running-a-private-stack/,
    )
  })

  test('command line: nothing, or a command after --', () => {
    assert.strictEqual(parseCommandLine([]), null)
    assert.deepStrictEqual(parseCommandLine(['--', 'node', '--test', 'a b']), [
      'node',
      '--test',
      'a b',
    ])
    assert.throws(() => parseCommandLine(['--']), /usage/)
    assert.throws(() => parseCommandLine(['node']), /usage/)
  })

  test('exit status follows the command, signals as a shell reports them', () => {
    assert.strictEqual(exitCodeFor(0, null, constants.signals), 0)
    assert.strictEqual(exitCodeFor(3, null, constants.signals), 3)
    assert.strictEqual(exitCodeFor(null, 'SIGTERM', constants.signals), 143)
    assert.strictEqual(exitCodeFor(null, null, constants.signals), 1)
  })
})

// End to end, against this project's own server (it implements every command
// the script sends): the script runs as a child, exactly as the npm scripts
// run it.

const SCRIPT = path.join(__dirname, '..', 'scripts', 'flush-redis.ts')

type Run = {
  done: Promise<{ code: number | null; stdout: string; stderr: string }>
  stdout(): string
  kill(signal: NodeJS.Signals): void
}

function runFlush(env: Record<string, string>, args: string[] = []): Run {
  const child = spawn(
    process.execPath,
    ['--import', 'tsx', '--no-warnings', SCRIPT, ...args],
    {
      env: {
        ...process.env,
        REDIS_CLUSTER_PORTS: '',
        REDIS_CLUSTER_PORT_RANGE: '',
        REDIS_STANDALONE_PORT: '',
        REDIS_STANDALONE_AUTH_PORT: '',
        ...env,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  )
  let stdout = ''
  let stderr = ''
  child.stdout.on('data', chunk => (stdout += chunk))
  child.stderr.on('data', chunk => (stderr += chunk))
  return {
    done: new Promise((resolve, reject) => {
      child.once('error', reject)
      child.once('close', code => resolve({ code, stdout, stderr }))
    }),
    stdout: () => stdout,
    kill: signal => child.kill(signal),
  }
}

async function waitFor(check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 20_000
  while (!(await check())) {
    if (Date.now() > deadline) {
      throw new Error('timed out')
    }
    await new Promise(resolve => setTimeout(resolve, 50))
  }
}

describe('clean:redis run lock end to end (#542)', () => {
  test('a second run refuses before flushing; the first releases on exit or crash', async () => {
    const cluster = await createRedisServer({ cluster: { masters: 1 } })
    const standalone = await createRedisServer()
    const env = {
      REDIS_CLUSTER_PORTS: String(cluster.nodes[0].port),
      REDIS_STANDALONE_PORT: String(standalone.port),
    }
    const probe = new Redis({ port: standalone.port, lazyConnect: true })
    await probe.connect()
    const holders = async () =>
      parseClientList((await probe.client('LIST')) as string).filter(c =>
        c.name.startsWith(RUN_LOCK_PREFIX),
      )

    let suitePid: number | undefined
    // The killed run's suite outlives it, as an orphan would, and keeps the
    // run's output pipe open until it goes.
    const killSuite = () => {
      if (suitePid === undefined) {
        return
      }
      try {
        process.kill(suitePid, 'SIGKILL')
      } catch {
        // already gone
      }
      suitePid = undefined
    }
    try {
      // A run that holds the stack while its "suite" runs; the suite flushes
      // too, which a key-based lock would not survive.
      const first = runFlush(env, [
        '--',
        process.execPath,
        '-e',
        'console.log(`suite pid ${process.pid}`); setTimeout(() => {}, 60000)',
      ])
      await waitFor(async () => /suite pid \d+/.test(first.stdout()))
      suitePid = Number(/suite pid (\d+)/.exec(first.stdout())?.[1])
      assert.strictEqual((await holders()).length, 1)
      await probe.flushall()

      await probe.set('first-run-key', '1')
      const second = await runFlush(env).done
      assert.strictEqual(second.code, 1, second.stderr)
      assert.match(second.stderr, /another real-backend run holds this stack/)
      assert.match(second.stderr, /#running-a-private-stack/)
      assert.strictEqual(await probe.get('first-run-key'), '1')

      // A crashed run (SIGKILL: no cleanup at all) releases the stack at
      // once, with no TTL to wait out, even while its suite lives on.
      first.kill('SIGKILL')
      await waitFor(async () => (await holders()).length === 0)
      killSuite()
      await first.done

      const third = await runFlush(env, [
        '--',
        process.execPath,
        '-e',
        'process.exit(3)',
      ]).done
      assert.strictEqual(third.code, 3, third.stderr)
      assert.match(third.stdout, /holding the run lock on 2 endpoint\(s\)/)
      assert.strictEqual(await probe.get('first-run-key'), null)
      await waitFor(async () => (await holders()).length === 0)

      // Without a command, the lock lasts only as long as the flush.
      const plain = await runFlush(env).done
      assert.strictEqual(plain.code, 0, plain.stderr)
      assert.deepStrictEqual(await holders(), [])
    } finally {
      killSuite()
      probe.disconnect()
      await standalone.close()
      await cluster.close()
    }
  })
})
