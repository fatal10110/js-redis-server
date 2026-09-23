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

/** Summary wording before and after the Redis 7.2 / Valkey 7.2 docs rewrite. */
type Summaries = { before72: string; from72: string }

function docsFor(
  summaries: Summaries,
  since: string,
  complexity: string,
): {
  docs: CommandDocumentation
  forProfile: CommandIntrospection['forProfile']
} {
  const docs = (summary: string): CommandDocumentation => ({
    summary,
    since,
    group: 'stream',
    complexity,
  })
  return {
    docs: docs(summaries.from72),
    forProfile: profile =>
      profile.has('docs.summary-7.2-wording')
        ? undefined
        : { docs: docs(summaries.before72) },
  }
}

/**
 * A stream container's `container|subcommand` entry (XINFO / XGROUP): arity,
 * flags, categories, tips, the key spec of the key after the subcommand
 * (`keyFlags`, none for HELP) and docs, as redis-server 7.0.15 / 8.0.6 and
 * valkey 8.0 / 9.0 report them.
 */
export function streamSubcommandInfo(
  name: string,
  arity: number,
  options: {
    flags: readonly string[]
    keyFlags?: readonly string[]
    tips?: readonly string[]
    summaries: Summaries
    since?: string
    complexity?: string
  },
): CommandIntrospection {
  const write = options.flags.includes('write')
  const keyed = options.keyFlags !== undefined
  return {
    name,
    arity,
    flags: options.flags,
    categories: keyed
      ? [write ? '@write' : '@read', '@stream', '@slow']
      : ['@stream', '@slow'],
    tips: options.tips ?? [],
    keySpecs: keyed ? [commandKeySpec(2, 0, 1, options.keyFlags ?? [])] : [],
    ...docsFor(
      options.summaries,
      options.since ?? '5.0.0',
      options.complexity ?? 'O(1)',
    ),
  }
}

/**
 * A stream container's own entry. From Redis 7.0 / Valkey 7.2 it is a bare
 * container (no flags, no keys) whose subcommands carry the details; Redis
 * 6.2 has no subcommand entries, and its single entry carries the flags,
 * categories and the 2,2,1 key range of the key after the subcommand.
 */
export function streamContainerIntrospection(options: {
  summaries: Summaries
  legacy: { flags: readonly string[]; keyFlags: readonly string[] }
  subcommands: readonly CommandIntrospection[]
}): CommandIntrospection {
  const container = docsFor(
    options.summaries,
    '5.0.0',
    'Depends on subcommand.',
  )
  const write = options.legacy.flags.includes('write')
  return {
    flags: [],
    categories: ['@slow'],
    subcommands: options.subcommands,
    docs: container.docs,
    forProfile: profile => {
      if (!profile.has('error.unknown-subcommand-dispatch-timing')) {
        return {
          flags: options.legacy.flags,
          categories: [write ? '@write' : '@read', '@stream', '@slow'],
          keySpecs: [commandKeySpec(2, 0, 1, options.legacy.keyFlags)],
          docs: container.docs,
        }
      }
      return container.forProfile?.(profile)
    },
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

/**
 * GEORADIUS / GEORADIUSBYMEMBER key specs: the key, and the destination after
 * STORE or STOREDIST searched for from `storeFrom`. Valkey 8.0+ marks the
 * destination specs `variable_flags` (`geo.store-keyspec-variable-flags`).
 */
export function georadiusIntrospection(
  storeFrom: number,
): CommandIntrospection {
  const specs = (destinationFlags: readonly string[]) => [
    commandKeySpec(1, 0, 1, ['RO', 'access']),
    commandKeywordKeySpec(
      'STORE',
      storeFrom,
      { lastKey: 0, keyStep: 1 },
      destinationFlags,
    ),
    commandKeywordKeySpec(
      'STOREDIST',
      storeFrom,
      { lastKey: 0, keyStep: 1 },
      destinationFlags,
    ),
  ]
  return {
    keySpecs: specs(['OW', 'update']),
    forProfile: profile =>
      profile.has('geo.store-keyspec-variable-flags')
        ? { keySpecs: specs(['OW', 'update', 'variable_flags']) }
        : undefined,
  }
}
