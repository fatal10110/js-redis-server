import { describe, test } from 'node:test'
import assert from 'node:assert'
import * as commandApi from '../src/commands'
import * as coreApi from '../src/internal'
import * as publicApi from '../src'

describe('public exports', () => {
  test('exports only the plural cluster command factory', () => {
    assert.strictEqual('createClusterCommands' in coreApi, true)
    assert.strictEqual('createClusterCommand' in coreApi, false)

    assert.strictEqual('createClusterCommands' in commandApi, true)
    assert.strictEqual('createClusterCommand' in commandApi, false)
  })

  test('command factories live on the core subpath, not the root barrel', () => {
    assert.strictEqual('createClusterCommands' in publicApi, false)
  })
})
