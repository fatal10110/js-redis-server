import { describe, test } from 'node:test'
import assert from 'node:assert'
import * as commandApi from '../src/commands'
import * as publicApi from '../src'

describe('public exports', () => {
  test('exports only the plural cluster command factory', () => {
    assert.strictEqual('createClusterCommands' in publicApi, true)
    assert.strictEqual('createClusterCommand' in publicApi, false)

    assert.strictEqual('createClusterCommands' in commandApi, true)
    assert.strictEqual('createClusterCommand' in commandApi, false)
  })
})
