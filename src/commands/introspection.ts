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
  arity: number,
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
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: options?.categories ?? ['@slow', '@connection'],
    tips: options?.tips ?? [],
    keySpecs: [],
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
