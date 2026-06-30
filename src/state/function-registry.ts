export type RedisFunctionDefinition = {
  name: string
  libraryName: string
  script: Buffer
  flags: string[]
}

export type RedisFunctionLibrary = {
  name: string
  code: Buffer
  functions: RedisFunctionDefinition[]
}

type ParsedFunctionRegistration = {
  name: string
  callback: string
  flags: string[]
  start: number
  end: number
}

const REGISTER_FUNCTION = 'redis.register_function'

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
        return { ...fn, script: Buffer.from(fn.script), flags: [...fn.flags] }
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
    const snapshot = this.list()

    try {
      if (mode === 'flush') {
        this.clear()
      }

      for (const library of libraries) {
        this.load(library, mode === 'replace')
      }
    } catch (err) {
      this.clear()
      for (const library of snapshot) {
        this.libraries.set(library.name, cloneLibrary(library))
      }
      throw err
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
  const body = text.replace(/^#![^\n]*(?:\n|$)/, '')
  const registrations = registeredFunctions(body)
  const functions = registrations.map(registration => ({
    name: registration.name,
    libraryName: name,
    flags: registration.flags,
    script: buildFunctionScript(body, registration.name, registrations),
  }))

  if (functions.length === 0) {
    throw new Error('No functions registered')
  }

  return { name, code: Buffer.from(code), functions }
}

function registeredFunctions(text: string): ParsedFunctionRegistration[] {
  const registrations: ParsedFunctionRegistration[] = []
  let index = 0

  while (index < text.length) {
    const start = findNextRegisterFunction(text, index)
    if (start === -1) {
      break
    }

    const registration = parseRegisterFunction(text, start)
    if (!registration) {
      index = start + REGISTER_FUNCTION.length
      continue
    }

    registrations.push(registration)
    index = registration.end
  }

  return registrations
}

function buildFunctionScript(
  text: string,
  name: string,
  registrations: readonly ParsedFunctionRegistration[],
): Buffer {
  let body = ''
  let cursor = 0

  for (const registration of registrations) {
    body += text.slice(cursor, registration.start)
    body += `\n__redis_functions[${JSON.stringify(registration.name)}] = ${registration.callback}\n`
    cursor = registration.end
  }
  body += text.slice(cursor)

  return Buffer.from(
    `local __redis_functions = {}\n${body}\nreturn __redis_functions[${JSON.stringify(name)}](KEYS, ARGV)`,
  )
}

function parseDump(payload: Buffer): RedisFunctionLibrary[] {
  let entries: unknown
  try {
    entries = JSON.parse(payload.toString())
  } catch {
    throw new Error('Invalid function payload')
  }

  if (!Array.isArray(entries) || !entries.every(entry => isDumpEntry(entry))) {
    throw new Error('Invalid function payload')
  }

  return entries.map(entry =>
    parseFunctionLibrary(Buffer.from(entry.code, 'base64')),
  )
}

function isDumpEntry(entry: unknown): entry is { code: string } {
  return (
    typeof entry === 'object' &&
    entry !== null &&
    typeof (entry as { code?: unknown }).code === 'string'
  )
}

function cloneLibrary(library: RedisFunctionLibrary): RedisFunctionLibrary {
  return {
    name: library.name,
    code: Buffer.from(library.code),
    functions: library.functions.map(fn => ({
      ...fn,
      script: Buffer.from(fn.script),
      flags: [...fn.flags],
    })),
  }
}

function findNextRegisterFunction(text: string, start: number): number {
  for (let index = start; index < text.length; index++) {
    const skipped = skipStringOrComment(text, index)
    if (skipped !== index) {
      index = skipped - 1
      continue
    }

    if (text.startsWith(REGISTER_FUNCTION, index)) {
      return index
    }
  }

  return -1
}

function parseRegisterFunction(
  text: string,
  start: number,
): ParsedFunctionRegistration | null {
  let index = skipWhitespace(text, start + REGISTER_FUNCTION.length)

  if (text[index] === '{') {
    const end = findMatchingDelimiter(text, index, '{', '}')
    return parseTableRegistration(text, index, end, start, end + 1)
  }

  if (text[index] !== '(') {
    return null
  }

  const callEnd = findMatchingDelimiter(text, index, '(', ')')
  const innerStart = skipWhitespace(text, index + 1)
  if (text[innerStart] === '{') {
    const tableEnd = findMatchingDelimiter(text, innerStart, '{', '}')
    return parseTableRegistration(
      text,
      innerStart,
      tableEnd,
      start,
      callEnd + 1,
    )
  }

  const name = parseStringLiteral(text, innerStart)
  if (!name) {
    return null
  }

  index = skipWhitespace(text, name.end)
  if (text[index] !== ',') {
    return null
  }

  const callbackStart = skipWhitespace(text, index + 1)
  const callbackEnd = findLuaFunctionEnd(text, callbackStart)
  return {
    name: name.value,
    callback: text.slice(callbackStart, callbackEnd),
    flags: [],
    start,
    end: callEnd + 1,
  }
}

function parseTableRegistration(
  text: string,
  tableStart: number,
  tableEnd: number,
  start: number,
  end: number,
): ParsedFunctionRegistration | null {
  const nameStart = findFieldValueStart(
    text,
    tableStart + 1,
    tableEnd,
    'function_name',
  )
  const callbackStart = findFieldValueStart(
    text,
    tableStart + 1,
    tableEnd,
    'callback',
  )
  if (nameStart === -1 || callbackStart === -1) {
    return null
  }

  const name = parseStringLiteral(text, skipWhitespace(text, nameStart))
  if (!name) {
    return null
  }

  const callbackValueStart = skipWhitespace(text, callbackStart)
  const callbackEnd = findLuaFunctionEnd(text, callbackValueStart)
  return {
    name: name.value,
    callback: text.slice(callbackValueStart, callbackEnd),
    flags: parseFlagsField(text, tableStart + 1, tableEnd),
    start,
    end,
  }
}

function parseFlagsField(text: string, start: number, end: number): string[] {
  const flagsStart = findFieldValueStart(text, start, end, 'flags')
  if (flagsStart === -1) {
    return []
  }

  const tableStart = skipWhitespace(text, flagsStart)
  if (text[tableStart] !== '{') {
    return []
  }

  const tableEnd = findMatchingDelimiter(text, tableStart, '{', '}')
  const flags: string[] = []
  let index = tableStart + 1

  while (index < tableEnd) {
    const flag = parseStringLiteral(text, index)
    if (flag) {
      flags.push(flag.value)
      index = flag.end
      continue
    }

    const skipped = skipStringOrComment(text, index)
    if (skipped !== index) {
      index = skipped
      continue
    }

    index++
  }

  return flags
}

function findFieldValueStart(
  text: string,
  start: number,
  end: number,
  field: string,
): number {
  let depth = 0

  for (let index = start; index < end; index++) {
    const skipped = skipStringOrComment(text, index)
    if (skipped !== index) {
      index = skipped - 1
      continue
    }

    if (isWordAt(text, index, 'function')) {
      index = findLuaFunctionEnd(text, index) - 1
      continue
    }

    const char = text[index]
    if (char === '{' || char === '(' || char === '[') {
      depth++
      continue
    }
    if (char === '}' || char === ')' || char === ']') {
      depth--
      continue
    }

    if (depth !== 0 || !isWordAt(text, index, field)) {
      continue
    }

    const equals = skipWhitespace(text, index + field.length)
    if (text[equals] === '=') {
      return equals + 1
    }
  }

  return -1
}

function findMatchingDelimiter(
  text: string,
  start: number,
  open: string,
  close: string,
): number {
  let depth = 0

  for (let index = start; index < text.length; index++) {
    const skipped = skipStringOrComment(text, index)
    if (skipped !== index) {
      index = skipped - 1
      continue
    }

    const char = text[index]
    if (char === open) {
      depth++
      continue
    }
    if (char === close) {
      depth--
      if (depth === 0) {
        return index
      }
    }
  }

  throw new Error('Malformed function registration')
}

function findLuaFunctionEnd(text: string, start: number): number {
  if (!isWordAt(text, start, 'function')) {
    throw new Error('Malformed function registration')
  }

  let depth = 0
  for (let index = start; index < text.length; index++) {
    const skipped = skipStringOrComment(text, index)
    if (skipped !== index) {
      index = skipped - 1
      continue
    }

    if (!isIdentifierStart(text[index])) {
      continue
    }

    const tokenStart = index
    while (isIdentifierPart(text[index])) {
      index++
    }
    const token = text.slice(tokenStart, index)
    index--

    if (
      token === 'function' ||
      token === 'then' ||
      token === 'do' ||
      token === 'repeat'
    ) {
      depth++
      continue
    }

    if (token === 'end' || token === 'until') {
      depth--
      if (depth === 0) {
        return index + 1
      }
    }
  }

  throw new Error('Malformed function registration')
}

function parseStringLiteral(
  text: string,
  start: number,
): { value: string; end: number } | null {
  const quote = text[start]
  if (quote !== '"' && quote !== "'") {
    return null
  }

  let value = ''
  for (let index = start + 1; index < text.length; index++) {
    const char = text[index]
    if (char === '\\') {
      value += text[index + 1] ?? ''
      index++
      continue
    }

    if (char === quote) {
      return { value, end: index + 1 }
    }

    value += char
  }

  throw new Error('Malformed function registration')
}

function skipWhitespace(text: string, start: number): number {
  let index = start
  while (/\s/.test(text[index] ?? '')) {
    index++
  }
  return index
}

function skipStringOrComment(text: string, start: number): number {
  if (text[start] === '-' && text[start + 1] === '-') {
    const longEnd = skipLongBracket(text, start + 2)
    if (longEnd !== start + 2) {
      return longEnd
    }

    const newline = text.indexOf('\n', start + 2)
    return newline === -1 ? text.length : newline + 1
  }

  if (text[start] === '"' || text[start] === "'") {
    return skipQuotedString(text, start)
  }

  return skipLongBracket(text, start)
}

function skipQuotedString(text: string, start: number): number {
  const quote = text[start]
  for (let index = start + 1; index < text.length; index++) {
    if (text[index] === '\\') {
      index++
      continue
    }

    if (text[index] === quote) {
      return index + 1
    }
  }

  throw new Error('Malformed function registration')
}

function skipLongBracket(text: string, start: number): number {
  if (text[start] !== '[') {
    return start
  }

  let markerEnd = start + 1
  while (text[markerEnd] === '=') {
    markerEnd++
  }
  if (text[markerEnd] !== '[') {
    return start
  }

  const close = `]${'='.repeat(markerEnd - start - 1)}]`
  const closeStart = text.indexOf(close, markerEnd + 1)
  if (closeStart === -1) {
    throw new Error('Malformed function registration')
  }

  return closeStart + close.length
}

function isWordAt(text: string, start: number, word: string): boolean {
  return (
    text.startsWith(word, start) &&
    !isIdentifierPart(text[start - 1]) &&
    !isIdentifierPart(text[start + word.length])
  )
}

function isIdentifierStart(char: string | undefined): boolean {
  return char !== undefined && /[A-Za-z_]/.test(char)
}

function isIdentifierPart(char: string | undefined): boolean {
  return char !== undefined && /[A-Za-z0-9_]/.test(char)
}
