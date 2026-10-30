/**
 * Redis's `stringmatchlen()`. `nocase` folds ASCII letters as its `nocase`
 * argument does (`tolower()` on both sides, ranges included): FUNCTION LIST
 * LIBRARYNAME matches that way (redis@7.0.15 functions.c:543, #583).
 */
export function redisGlobMatch(
  pattern: Buffer,
  value: Buffer,
  options: { nocase?: boolean } = {},
): boolean {
  return matchGlob(pattern, 0, value, 0, options.nocase === true)
}

function matchGlob(
  pattern: Buffer,
  patternIndex: number,
  value: Buffer,
  valueIndex: number,
  nocase: boolean,
): boolean {
  const fold = nocase ? asciiToLower : identity

  let patternCursor = patternIndex
  let valueCursor = valueIndex

  while (patternCursor < pattern.length && valueCursor < value.length) {
    const token = pattern[patternCursor]

    if (token === STAR) {
      while (pattern[patternCursor + 1] === STAR) {
        patternCursor++
      }

      if (patternCursor + 1 === pattern.length) {
        return true
      }

      for (
        let nextValueCursor = valueCursor;
        nextValueCursor < value.length;
        nextValueCursor++
      ) {
        if (
          matchGlob(pattern, patternCursor + 1, value, nextValueCursor, nocase)
        ) {
          return true
        }
      }

      return false
    }

    if (token === QUESTION_MARK) {
      patternCursor++
      valueCursor++
      continue
    }

    if (token === OPEN_BRACKET) {
      const characterClass = matchCharacterClass(
        pattern,
        patternCursor,
        value[valueCursor],
        fold,
      )

      if (!characterClass.matches) {
        return false
      }

      patternCursor = characterClass.nextPatternIndex
      valueCursor++
      continue
    }

    if (token === BACKSLASH && patternCursor + 1 < pattern.length) {
      patternCursor++
    }

    if (fold(pattern[patternCursor]) !== fold(value[valueCursor])) {
      return false
    }

    patternCursor++
    valueCursor++
  }

  while (pattern[patternCursor] === STAR) {
    patternCursor++
  }

  return patternCursor === pattern.length && valueCursor === value.length
}

function matchCharacterClass(
  pattern: Buffer,
  openBracketIndex: number,
  value: number,
  fold: (byte: number) => number,
): { matches: boolean; nextPatternIndex: number } {
  let patternCursor = openBracketIndex + 1
  let negated = false

  if (pattern[patternCursor] === CARET) {
    negated = true
    patternCursor++
  }

  let matches = false

  while (true) {
    if (pattern[patternCursor] === CLOSE_BRACKET) {
      break
    }

    if (patternCursor >= pattern.length) {
      patternCursor--
      break
    }

    if (
      pattern[patternCursor] === BACKSLASH &&
      patternCursor + 1 < pattern.length
    ) {
      patternCursor++
      if (fold(pattern[patternCursor]) === fold(value)) {
        matches = true
      }
    } else if (
      patternCursor + 2 < pattern.length &&
      pattern[patternCursor + 1] === DASH
    ) {
      let start = pattern[patternCursor]
      let end = pattern[patternCursor + 2]

      if (start > end) {
        const previousStart = start
        start = end
        end = previousStart
      }

      // Swapped before folding, as stringmatchlen() does.
      start = fold(start)
      end = fold(end)
      const folded = fold(value)
      if (folded >= start && folded <= end) {
        matches = true
      }

      patternCursor += 2
    } else if (fold(pattern[patternCursor]) === fold(value)) {
      matches = true
    }

    patternCursor++
  }

  return {
    matches: negated ? !matches : matches,
    nextPatternIndex: patternCursor + 1,
  }
}

function asciiToLower(byte: number): number {
  return byte >= 0x41 && byte <= 0x5a ? byte + 0x20 : byte
}

function identity(byte: number): number {
  return byte
}

const BACKSLASH = '\\'.charCodeAt(0)
const CARET = '^'.charCodeAt(0)
const CLOSE_BRACKET = ']'.charCodeAt(0)
const DASH = '-'.charCodeAt(0)
const OPEN_BRACKET = '['.charCodeAt(0)
const QUESTION_MARK = '?'.charCodeAt(0)
const STAR = '*'.charCodeAt(0)
