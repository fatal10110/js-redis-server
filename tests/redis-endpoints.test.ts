import { describe, test } from 'node:test'
import assert from 'node:assert'
import { spawnSync } from 'node:child_process'
import path from 'node:path'
import {
  DEFAULT_CLUSTER_PORTS,
  parseClusterPortRange,
  resolveClusterPorts,
} from '../tests-integration/redis-endpoints'

// The real backend's cluster ports can be moved as one range so a second
// docker-compose stack (another worktree or checkout) runs beside the default
// one instead of sharing it — and being flushed by — a concurrent run (#497).
// docker/redis-cluster-init.sh reads the same variable and must accept exactly
// the same ranges; the last describe below runs it to check.

describe('parseClusterPortRange', () => {
  test('expands first-last into the six node ports', () => {
    assert.deepStrictEqual(
      parseClusterPortRange('31000-31005'),
      [31000, 31001, 31002, 31003, 31004, 31005],
    )
    assert.deepStrictEqual(
      parseClusterPortRange('30000-30005'),
      DEFAULT_CLUSTER_PORTS,
    )
  })

  test('rejects a range that is not six consecutive ports', () => {
    for (const raw of [
      '31000-31004',
      '31000-31006',
      '31005-31000',
      '31000',
      '31000,31005',
      '31000 - 31005',
      'a-f',
      '0-5',
      '-31000-31005',
      '031000-031005',
      // The value reaches the init script and compose's port map verbatim,
      // and both refuse whitespace, so the harness does too.
      ' 31000-31005',
      '31000-31005 ',
      '99999999999999999999-100000000000000000004',
      '31000:1-31005',
    ]) {
      assert.throws(
        () => parseClusterPortRange(raw),
        /is not a range of 6 consecutive ports/,
        raw,
      )
    }
  })

  test('rejects a range whose cluster bus ports would not exist', () => {
    // Bus port = client port + 10000, so 55530-55535 is the highest range.
    assert.deepStrictEqual(parseClusterPortRange('55530-55535').at(-1), 55535)
    assert.throws(
      () => parseClusterPortRange('55531-55536'),
      /puts the cluster bus .* past port 65535/,
    )
  })
})

describe('resolveClusterPorts', () => {
  test('defaults to the docker-compose ports when neither variable is set', () => {
    assert.deepStrictEqual(
      resolveClusterPorts(undefined, undefined),
      DEFAULT_CLUSTER_PORTS,
    )
    assert.deepStrictEqual(resolveClusterPorts(' ', ''), DEFAULT_CLUSTER_PORTS)
  })

  test('refuses a whitespace-only range instead of reading it as unset', () => {
    // Compose's ${REDIS_CLUSTER_PORT_RANGE:-...} only falls back when the
    // variable is empty, so ' ' reaches the init script, which rejects it.
    assert.throws(
      () => resolveClusterPorts(undefined, ' '),
      /is not a range of 6 consecutive ports/,
    )
  })

  test('uses the range when no explicit seeds are given', () => {
    assert.deepStrictEqual(
      resolveClusterPorts(undefined, '31000-31005'),
      [31000, 31001, 31002, 31003, 31004, 31005],
    )
  })

  test('explicit seeds win, as long as they lie inside the range', () => {
    assert.deepStrictEqual(resolveClusterPorts('31002', '31000-31005'), [31002])
    assert.deepStrictEqual(resolveClusterPorts('31100', undefined), [31100])
  })

  test('refuses seeds that point outside the range', () => {
    assert.throws(
      () => resolveClusterPorts('30000,31001', '31000-31005'),
      /names port\(s\) 30000 outside REDIS_CLUSTER_PORT_RANGE/,
    )
  })

  test('still validates the range when explicit seeds are given', () => {
    assert.throws(
      () => resolveClusterPorts('31000', '31000-31009'),
      /is not a range of 6 consecutive ports/,
    )
  })
})

describe('docker/redis-cluster-init.sh agrees with the harness', () => {
  const script = path.join(__dirname, '..', 'docker', 'redis-cluster-init.sh')

  // Runs the script's `ports` mode, which validates CLUSTER_PORT_RANGE exactly
  // as a boot does and prints the node ports without starting anything.
  function scriptPorts(range: string): number[] | 'rejected' {
    const run = spawnSync('sh', [script, 'ports'], {
      env: { ...process.env, CLUSTER_PORT_RANGE: range },
      encoding: 'utf8',
    })
    if (run.status !== 0) {
      // Rejection must be the script's own FATAL, not a shell error such as
      // dash's "Illegal number" aborting it halfway.
      assert.match(run.stdout, /FATAL: CLUSTER_PORT_RANGE=/, range)
      assert.strictEqual(run.stderr, '', range)
      return 'rejected'
    }
    return run.stdout.trim().split(' ').map(Number)
  }

  function harnessPorts(range: string): number[] | 'rejected' {
    try {
      return resolveClusterPorts(undefined, range)
    } catch {
      return 'rejected'
    }
  }

  test(
    'accepts and rejects the same ranges',
    { skip: process.platform === 'win32' && 'needs a POSIX sh' },
    () => {
      for (const range of [
        '',
        '30000-30005',
        '31000-31005',
        '1-6',
        '55530-55535',
        '55531-55536',
        '31000-31004',
        '31000-31006',
        '31005-31000',
        '31000',
        '31000-',
        '-31005',
        '-',
        '31000-31005-31010',
        '31000,31005',
        '31000 - 31005',
        ' 31000-31005',
        '31000-31005 ',
        ' ',
        'a-f',
        '0-5',
        '031000-031005',
        '31000:1-31005',
        '99999-100004',
        '99999999999999999999-100000000000000000004',
      ]) {
        assert.deepStrictEqual(scriptPorts(range), harnessPorts(range), range)
      }
    },
  )
})
