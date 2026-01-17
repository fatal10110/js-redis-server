import { describe, test } from 'node:test'
import assert from 'node:assert'
import { parseArgs } from '../src/cli'

describe('CLI argument parsing', () => {
  test('uses defaults with no args', () => {
    const options = parseArgs([])
    assert.strictEqual(options.mode, 'single')
    assert.strictEqual(options.port, 6379)
    assert.strictEqual(options.masters, 3)
    assert.strictEqual(options.slaves, 0)
    assert.strictEqual(options.basePort, 30000)
  })

  test('parses cluster mode and options', () => {
    const options = parseArgs([
      '--cluster',
      '--masters',
      '4',
      '--slaves',
      '2',
      '--base-port',
      '31000',
    ])
    assert.strictEqual(options.mode, 'cluster')
    assert.strictEqual(options.masters, 4)
    assert.strictEqual(options.slaves, 2)
    assert.strictEqual(options.basePort, 31000)
  })

  test('rejects invalid values', () => {
    assert.throws(() => parseArgs(['--port', 'abc']), /Invalid value/)
    assert.throws(() => parseArgs(['--mode', 'bad']), /Invalid mode/)
  })
})
