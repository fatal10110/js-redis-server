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

  test('nocase folds ASCII letters on both sides, ranges included', () => {
    const nocase = (pattern: string, value: string) =>
      redisGlobMatch(Buffer.from(pattern), Buffer.from(value), {
        nocase: true,
      })
    assert.strictEqual(nocase('MY*', 'mylib'), true)
    assert.strictEqual(nocase('my?IB', 'MYLIB'), true)
    assert.strictEqual(nocase('[A-Z]ylib', 'mylib'), true)
    assert.strictEqual(nocase('[a-z]ylib', 'Mylib'), true)
    assert.strictEqual(nocase('[M]ylib', 'mylib'), true)
    assert.strictEqual(nocase('[\\M]ylib', 'mylib'), true)
    assert.strictEqual(nocase('[^M]ylib', 'mylib'), false)
    assert.strictEqual(nocase('\\Mylib', 'mylib'), true)
    assert.strictEqual(nocase('lib', 'mylib'), false)
    // Without nocase the match stays exact.
    assert.strictEqual(matches('MY*', 'mylib'), false)
    assert.strictEqual(matches('[A-Z]ylib', 'mylib'), false)
  })
})

function matches(pattern: string, value: string): boolean {
  return redisGlobMatch(Buffer.from(pattern), Buffer.from(value))
}
