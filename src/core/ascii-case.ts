/**
 * ASCII-only case folding for command and subcommand names.
 *
 * Real Redis matches names byte by byte with C `tolower()`/`strcasecmp`, so
 * only `A-Z` (0x41-0x5A) is case-insensitive. `String.prototype.toLowerCase()`
 * and `toUpperCase()` are Unicode-aware and fold some non-ASCII characters
 * onto ASCII letters — U+212A KELVIN SIGN lowercases to `k`, U+017F LATIN
 * SMALL LETTER LONG S uppercases to `S` — which would resolve names real Redis
 * rejects (#382). Every other character passes through unchanged.
 */
export function asciiLowerCase(value: string): string {
  return value.replace(/[A-Z]/g, char =>
    String.fromCharCode(char.charCodeAt(0) + 0x20),
  )
}

/** Upper-case counterpart of {@link asciiLowerCase}: folds `a-z` only. */
export function asciiUpperCase(value: string): string {
  return value.replace(/[a-z]/g, char =>
    String.fromCharCode(char.charCodeAt(0) - 0x20),
  )
}
