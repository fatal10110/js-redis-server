import assert from 'node:assert'

// Buffers are accepted so a test can put bytes on the wire that are not valid
// UTF-8 — the argument reaches the server exactly as written.
export function commandFrame(...items: (string | Buffer)[]): Buffer {
  const tokens = items.map(item =>
    Buffer.isBuffer(item) ? item : Buffer.from(item),
  )
  return Buffer.concat([
    Buffer.from(`*${tokens.length}\r\n`),
    ...tokens.flatMap(token => [
      Buffer.from(`$${token.length}\r\n`),
      token,
      Buffer.from('\r\n'),
    ]),
  ])
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
