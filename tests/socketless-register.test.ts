import { describe, test } from 'node:test'
import assert from 'node:assert'
import { spawnSync } from 'node:child_process'
import path from 'node:path'

/**
 * The socketless known-gaps preload (`tests-integration/socketless/register.ts`)
 * must fail a file whenever a listed todo test ends any way other than the
 * recorded failure (#412). Runs the preload against a fixture test file whose
 * one listed test is expected to fail with `mGet is not a function`.
 */
const ROOT = path.resolve(__dirname, '..')
const REGISTER = path.join(ROOT, 'tests-integration/socketless/register.ts')
const FIXTURE = path.join(
  ROOT,
  'tests-integration/socketless/fixtures/listed.fixture.ts',
)
const GAPS = path.join(
  ROOT,
  'tests-integration/socketless/fixtures/gaps.fixture.ts',
)

function runFixture(mode: string): { status: number | null; output: string } {
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    TEST_BACKEND: 'socketless',
    SOCKETLESS_KNOWN_GAPS_MODULE: GAPS,
    FIXTURE_MODE: mode,
  }
  // We run inside node:test ourselves; the nested runner must not think so.
  delete env.NODE_TEST_CONTEXT
  const result = spawnSync(
    process.execPath,
    [
      '--import',
      'tsx',
      '--import',
      REGISTER,
      '--no-warnings',
      '--test',
      '--test-reporter',
      'spec',
      FIXTURE,
    ],
    { cwd: ROOT, env, encoding: 'utf8', timeout: 30000 },
  )
  return { status: result.status, output: `${result.stdout}${result.stderr}` }
}

describe('socketless known-gaps preload', () => {
  test('a listed test failing with the recorded error is a todo', () => {
    const { status, output } = runFixture('expected')
    assert.strictEqual(status, 0, output)
    assert.match(output, /todo 1/)
  })

  test('a listed test that passes fails the file', () => {
    const { status, output } = runFixture('pass')
    assert.strictEqual(status, 1, output)
    assert.match(output, /'listed': passes now/)
  })

  test('a listed test failing with another error fails the file', () => {
    const { status, output } = runFixture('unexpected')
    assert.strictEqual(status, 1, output)
    assert.match(output, /failed with an unexpected error: ERR something else/)
  })

  test('a listed test that times out fails the file', () => {
    const { status, output } = runFixture('timeout')
    assert.strictEqual(status, 1, output)
    assert.match(output, /'listed': never finished \(timed out or cancelled\)/)
  })

  test('a timed-out listed test that later throws the expected error fails the file', () => {
    const { status, output } = runFixture('late-expected')
    assert.strictEqual(status, 1, output)
    assert.match(output, /'listed': never finished \(timed out or cancelled\)/)
  })

  test('a timed-out listed test that later returns fails the file as never finished', () => {
    const { status, output } = runFixture('late-pass')
    assert.strictEqual(status, 1, output)
    assert.match(output, /'listed': never finished \(timed out or cancelled\)/)
    assert.doesNotMatch(output, /passes now/)
  })
})
