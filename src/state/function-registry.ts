export type RedisFunctionDefinition = {
  name: string
  libraryName: string
  script: Buffer
}

export type RedisFunctionLibrary = {
  name: string
  code: Buffer
  functions: RedisFunctionDefinition[]
}

export class RedisFunctionRegistry {
  private readonly libraries = new Map<string, RedisFunctionLibrary>()

  load(library: RedisFunctionLibrary, replace = false): void {
    if (!replace && this.libraries.has(library.name)) {
      throw new Error(`Library '${library.name}' already exists`)
    }

    this.libraries.set(library.name, cloneLibrary(library))
  }

  delete(name: string): boolean {
    return this.libraries.delete(name)
  }

  clear(): void {
    this.libraries.clear()
  }

  list(): RedisFunctionLibrary[] {
    return [...this.libraries.values()].map(cloneLibrary)
  }

  findFunction(name: string): RedisFunctionDefinition | null {
    for (const library of this.libraries.values()) {
      const fn = library.functions.find(entry => entry.name === name)
      if (fn) {
        return { ...fn, script: Buffer.from(fn.script) }
      }
    }

    return null
  }

  dump(): Buffer {
    return Buffer.from(
      JSON.stringify(
        this.list().map(library => ({
          code: library.code.toString('base64'),
        })),
      ),
    )
  }

  restore(payload: Buffer, mode: 'append' | 'flush' | 'replace'): void {
    const libraries = parseDump(payload)
    if (mode === 'flush') {
      this.clear()
    }

    for (const library of libraries) {
      this.load(library, mode === 'replace')
    }
  }
}

export function parseFunctionLibrary(code: Buffer): RedisFunctionLibrary {
  const text = code.toString()
  const libraryMatch = text.match(/^#!lua\s+name=([^\s]+)\s*$/m)
  if (!libraryMatch) {
    throw new Error('Missing library metadata')
  }

  const name = libraryMatch[1]
  const functions = [...registeredFunctions(text)].map(functionName => ({
    name: functionName,
    libraryName: name,
    script: buildFunctionScript(text, functionName),
  }))

  if (functions.length === 0) {
    throw new Error('No functions registered')
  }

  return { name, code: Buffer.from(code), functions }
}

function* registeredFunctions(text: string): Iterable<string> {
  const regex =
    /redis\.register_function\s*\(\s*(["'])([^"']+)\1\s*,\s*function\s*\(\s*keys\s*,\s*args\s*\)\s*[\s\S]*?\s*end\s*\)/g
  let match: RegExpExecArray | null
  while ((match = regex.exec(text)) !== null) {
    yield match[2]
  }
}

function buildFunctionScript(text: string, name: string): Buffer {
  const withoutShebang = text.replace(/^#![^\n]*(?:\n|$)/, '')
  const body = withoutShebang.replace(
    /redis\.register_function\s*\(\s*(["'])([^"']+)\1\s*,\s*function\s*\(\s*keys\s*,\s*args\s*\)\s*([\s\S]*?)\s*end\s*\)/g,
    (_match, _quote, functionName, functionBody) =>
      `__redis_functions[${JSON.stringify(functionName)}] = function(keys, args)\n${functionBody}\nend`,
  )

  return Buffer.from(
    `local __redis_functions = {}\n${body}\nreturn __redis_functions[${JSON.stringify(name)}](KEYS, ARGV)`,
  )
}

function parseDump(payload: Buffer): RedisFunctionLibrary[] {
  let entries: Array<{ code: string }>
  try {
    entries = JSON.parse(payload.toString()) as Array<{ code: string }>
  } catch {
    throw new Error('Invalid function payload')
  }

  return entries.map(entry =>
    parseFunctionLibrary(Buffer.from(entry.code, 'base64')),
  )
}

function cloneLibrary(library: RedisFunctionLibrary): RedisFunctionLibrary {
  return {
    name: library.name,
    code: Buffer.from(library.code),
    functions: library.functions.map(fn => ({
      ...fn,
      script: Buffer.from(fn.script),
    })),
  }
}
