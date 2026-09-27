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

/**
 * A declared `container|subcommand` entry: its name and arity. Its flags,
 * categories, tips and key specs come from the real command table
 * (`commandTableEntry`).
 */
export function commandSubcommandInfo(
  name: string,
  arity: CommandIntrospection['arity'],
): CommandIntrospection {
  return {
    name,
    arity,
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
 * A stream container's `container|subcommand` entry (XINFO / XGROUP): arity
 * and docs, as redis-server 7.0.15 / 8.0.6 and valkey 8.0 / 9.0 report them.
 */
export function streamSubcommandInfo(
  name: string,
  arity: number,
  options: {
    summaries: Summaries
    since?: string
    complexity?: string
  },
): CommandIntrospection {
  return {
    name,
    arity,
    ...docsFor(
      options.summaries,
      options.since ?? '5.0.0',
      options.complexity ?? 'O(1)',
    ),
  }
}

/**
 * A stream container's own entry. From Redis 7.0 / Valkey 7.2 it is a bare
 * container whose subcommands carry the details; Redis 6.2 has no
 * subcommand entries, and its single entry has the 2,2,1 key range of the
 * key after the subcommand.
 */
export function streamContainerIntrospection(options: {
  summaries: Summaries
  subcommands: readonly CommandIntrospection[]
}): CommandIntrospection {
  const container = docsFor(
    options.summaries,
    '5.0.0',
    'Depends on subcommand.',
  )
  return {
    subcommands: options.subcommands,
    docs: container.docs,
    forProfile: profile => {
      if (!profile.has('error.unknown-subcommand-dispatch-timing')) {
        return {
          keySpecs: [commandKeySpec(2, 0, 1, [])],
          docs: container.docs,
        }
      }
      return container.forProfile?.(profile)
    },
  }
}
