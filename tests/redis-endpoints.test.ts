import { describe, test } from 'node:test'
import assert from 'node:assert'
import {
  DEFAULT_CLUSTER_PORTS,
  parseClusterPortRange,
  resolveClusterPorts,
} from '../tests-integration/redis-endpoints'

// The real backend's cluster ports can be moved as one range so a second
// docker-compose stack (another worktree or checkout) runs beside the default
// one instead of sharing it — and being flushed by — a concurrent run (#497).
// docker/redis-cluster-init.sh applies the same rules to the same variable.

describe('parseClusterPortRange', () => {
  test('expands first-last into the six node ports', () => {
    assert.deepStrictEqual(
      parseClusterPortRange('31000-31005'),
      [31000, 31001, 31002, 31003, 31004, 31005],
    )
    assert.deepStrictEqual(
      parseClusterPortRange(' 30000-30005 '),
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
