import type {
  CommandDocumentation,
  CommandDocumentationArgument,
  CommandIntrospection,
  CommandKeySpec,
} from '../core/command-definition'

export function commandKeySpec(
  beginSearchIndex: number,
  lastKey: number,
  keyStep: number,
  flags: readonly string[],
  options?: { notes?: string },
): CommandKeySpec {
  return {
    flags,
    beginSearchIndex,
    lastKey,
    keyStep,
    notes: options?.notes,
  }
}

export function commandDocs(
  summary: string,
  group: string,
  args: readonly CommandDocumentationArgument[] = [],
  options?: { since?: string; complexity?: string },
): CommandDocumentation {
  return {
    summary,
    since: options?.since ?? '1.0.0',
    group,
    complexity: options?.complexity ?? 'O(1)',
    arguments: args,
  }
}

export function commandKeyArgument(
  name: string,
  keySpecIndex: number,
  options?: { flags?: readonly string[] },
): CommandDocumentationArgument {
  return {
    name,
    type: 'key',
    keySpecIndex,
    flags: options?.flags,
  }
}

export function commandSubcommandInfo(
  name: string,
  arity: CommandIntrospection['arity'],
  options?: {
    flags?: readonly string[]
    categories?: readonly string[]
    tips?: readonly string[]
  },
): CommandIntrospection {
  return {
    name,
    arity,
    flags: options?.flags ?? ['loading', 'stale'],
    categories: options?.categories ?? ['@slow', '@connection'],
    tips: options?.tips ?? [],
    docs: {
      summary: name,
      group: commandGroupFromName(name),
    },
  }
}

function commandGroupFromName(name: string): string {
  const separator = name.indexOf('|')
  return separator === -1 ? 'generic' : name.slice(0, separator)
}

/**
 * A stream container's `container|subcommand` COMMAND INFO entry whose key is
 * the argument after the subcommand (XINFO / XGROUP). Arity, flags,
 * categories and key-spec flags as redis-server 8.0.6 reports them.
 */
export function streamSubcommandInfo(
  name: string,
  arity: number,
  flags: readonly string[],
  keyFlags: readonly string[],
): CommandIntrospection {
  const write = flags.includes('write')
  return {
    ...commandSubcommandInfo(name, arity, {
      flags,
      categories: [write ? '@write' : '@read', '@stream', '@slow'],
    }),
    keySpecs: [commandKeySpec(2, 0, 1, keyFlags)],
  }
}

/** A key spec whose keys follow `keyword`, searched for from `startFrom`. */
export function commandKeywordKeySpec(
  keyword: string,
  startFrom: number,
  range: { lastKey: number; keyStep: number; limit?: number },
  flags: readonly string[],
): CommandKeySpec {
  return {
    flags,
    beginSearchIndex: 0,
    beginSearchKeyword: { keyword, startFrom },
    lastKey: range.lastKey,
    keyStep: range.keyStep,
    limit: range.limit,
  }
}

/**
 * A key spec for a `numkeys key [key ...]` tail starting at
 * `beginSearchIndex`: the count is the argument there, the keys follow it.
 */
export function commandKeynumKeySpec(
  beginSearchIndex: number,
  flags: readonly string[],
  options?: { notes?: string },
): CommandKeySpec {
  return {
    flags,
    beginSearchIndex,
    lastKey: 0,
    keyStep: 1,
    findKeysKeynum: { keyNumIdx: 0, firstKey: 1, keyStep: 1 },
    notes: options?.notes,
  }
}
