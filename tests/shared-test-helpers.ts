import assert from 'node:assert'

export function commandFrame(...items: string[]): Buffer {
  return Buffer.from(
    `*${items.length}\r\n${items
      .map(item => `$${Buffer.byteLength(item)}\r\n${item}\r\n`)
      .join('')}`,
  )
}

export function errorWithMessage(message: string): (error: unknown) => boolean {
  return (error: unknown): boolean => {
    assert.ok(error instanceof Error)
    assert.strictEqual(error.message, message)
    return true
  }
}

export function assertBuffersEqual(actual: Buffer[], expected: Buffer[]): void {
  assert.deepStrictEqual(cloneBuffers(actual), cloneBuffers(expected))
}

export function assertBufferSetsEqual(
  actual: Buffer[],
  expected: Buffer[],
): void {
  assert.deepStrictEqual(
    cloneBuffers(actual).sort(Buffer.compare),
    cloneBuffers(expected).sort(Buffer.compare),
  )
}

function cloneBuffers(values: Buffer[]): Buffer[] {
  return values.map(value => Buffer.from(value))
}
