import { describe, test } from 'node:test'
import assert from 'node:assert'
import { redisGlobMatch } from '../src/core/glob'

describe('redisGlobMatch', () => {
  test('matches Redis glob wildcards and character classes', () => {
    assert.strictEqual(matches('news.*', 'news.world'), true)
    assert.strictEqual(matches('news.?', 'news.a'), true)
    assert.strictEqual(matches('news.?', 'news.ab'), false)
    assert.strictEqual(matches('h[ae]llo', 'hello'), true)
    assert.strictEqual(matches('h[ae]llo', 'hillo'), false)
    assert.strictEqual(matches('h[^i]llo', 'hello'), true)
    assert.strictEqual(matches('h[^i]llo', 'hillo'), false)
    assert.strictEqual(matches('item[3-1]', 'item2'), true)
  })

  test('handles escaped glob tokens literally', () => {
    assert.strictEqual(matches('literal\\*', 'literal*'), true)
    assert.strictEqual(matches('literal\\*', 'literal-value'), false)
    assert.strictEqual(matches('file\\?', 'file?'), true)
    assert.strictEqual(matches('file\\?', 'file1'), false)
  })
})

function matches(pattern: string, value: string): boolean {
  return redisGlobMatch(Buffer.from(pattern), Buffer.from(value))
}
