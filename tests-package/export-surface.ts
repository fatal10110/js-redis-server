// Reads the *published* export surface out of the emitted declaration files
// (`dist/index.d.ts`, `dist/core.d.ts`) using the TypeScript compiler API.
//
// Why the `.d.ts` and not the built JS: runtime enumeration (`Object.keys` on
// `require('js-redis-server/core')`) only sees *value* exports. Most of the
// `/core` surface — `RedisClientSession`, `CommandPlan`, `Resp2ServerOptions`,
// every `*Options` type — is type-only and therefore invisible at runtime, and
// that is exactly where the breaking changes in #374/#376/#377 lived. The
// declaration bundle is the only artifact that carries both.
//
// Two levels are captured per entry point:
//
//   1. the exported symbol names, each tagged `value` (has a runtime binding:
//      class, function, const) or `type` (interface / type alias only), and
//   2. the *own* members declared on each exported interface, class, object
//      type alias, or `const` object namespace.
//
// Level 2 is what makes this guard worth having. Three of the four `/core`
// breaks this was written in response to were member-level, not symbol-level:
// an option dropped from four option types (#374), six interface methods
// collapsed into two (#376), three registry methods deleted (#377). A
// symbol-list-only snapshot would have stayed green for all three.
//
// "Own members" means members written on the declaration itself — inherited
// members are deliberately not walked. That keeps 120-odd error subclasses from
// each snapshotting `message`/`stack`/`cause` out of `lib.es5.d.ts`, which would
// churn the baseline on every TypeScript bump while proving nothing about this
// package. `RedisCommandError` still snapshots its own `code`, so deleting it
// there is still caught.

import ts from 'typescript'
import { fileURLToPath } from 'node:url'

export type ExportKind = 'value' | 'type'

export type ExportEntry = {
  readonly kind: ExportKind
  /** Own members of the declaration, sorted. Omitted when there are none. */
  readonly members?: readonly string[]
}

/** Exported symbol name -> what it is. Keys are sorted. */
export type ExportSurface = Record<string, ExportEntry>

const HIDDEN_MEMBER = ts.ModifierFlags.Private | ts.ModifierFlags.Protected

function memberName(member: ts.NamedDeclaration): string | undefined {
  if (ts.getCombinedModifierFlags(member as ts.Declaration) & HIDDEN_MEMBER) {
    return undefined
  }

  const name = member.name

  if (!name) {
    // Index signatures, call/construct signatures, static blocks — nothing
    // nameable to pin.
    return undefined
  }

  if (ts.isIdentifier(name) || ts.isStringLiteral(name)) {
    return name.text
  }

  // `#private` fields and computed names are not part of the surface.
  return undefined
}

function membersOfTypeNode(node: ts.TypeNode | undefined): string[] {
  if (!node) {
    return []
  }

  if (ts.isParenthesizedTypeNode(node)) {
    return membersOfTypeNode(node.type)
  }

  if (ts.isTypeLiteralNode(node)) {
    return node.members
      .map(memberName)
      .filter((name): name is string => name !== undefined)
  }

  if (ts.isIntersectionTypeNode(node)) {
    // `NodePipeline & { host: string; port: number }` contributes the literal's
    // own members; the referenced half is snapshotted under its own name.
    return node.types.flatMap(membersOfTypeNode)
  }

  // Unions, type references, mapped types, keyof, … have no own members to pin.
  return []
}

function ownMembers(symbol: ts.Symbol): string[] {
  const members = new Set<string>()

  for (const declaration of symbol.declarations ?? []) {
    if (
      ts.isInterfaceDeclaration(declaration) ||
      ts.isClassDeclaration(declaration)
    ) {
      for (const member of declaration.members) {
        const name = memberName(member)
        if (name !== undefined) {
          members.add(name)
        }
      }
      continue
    }

    if (ts.isTypeAliasDeclaration(declaration)) {
      for (const name of membersOfTypeNode(declaration.type)) {
        members.add(name)
      }
      continue
    }

    if (ts.isVariableDeclaration(declaration)) {
      // `const` object namespaces — `t`, `RedisValue`, `RedisResult`. In a
      // declaration file these carry an explicit type literal.
      for (const name of membersOfTypeNode(declaration.type)) {
        members.add(name)
      }
    }
  }

  return [...members].sort()
}

/**
 * Parse one emitted declaration bundle and return its full export surface.
 *
 * `entryDeclarationFile` is an absolute path to a `dist/*.d.ts` (or `.d.mts`).
 * The bundle's shared chunk is resolved by the compiler, so symbols that tsup
 * hoisted out of the entry file are still seen.
 */
export function readExportSurface(entryDeclarationFile: string): ExportSurface {
  const program = ts.createProgram([entryDeclarationFile], {
    noEmit: true,
    skipLibCheck: true,
    strict: true,
    target: ts.ScriptTarget.ESNext,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
  })

  const checker = program.getTypeChecker()
  const sourceFile = program.getSourceFile(entryDeclarationFile)

  if (!sourceFile) {
    throw new Error(`could not load declaration file: ${entryDeclarationFile}`)
  }

  const moduleSymbol = checker.getSymbolAtLocation(sourceFile)

  if (!moduleSymbol) {
    throw new Error(
      `${entryDeclarationFile} is not a module — the build emitted no exports`,
    )
  }

  const surface: ExportSurface = {}

  for (const exported of checker.getExportsOfModule(moduleSymbol)) {
    const resolved =
      exported.flags & ts.SymbolFlags.Alias
        ? checker.getAliasedSymbol(exported)
        : exported

    const kind: ExportKind =
      resolved.flags & ts.SymbolFlags.Value ? 'value' : 'type'
    const members = ownMembers(resolved)

    surface[exported.getName()] =
      members.length > 0 ? { kind, members } : { kind }
  }

  return sortSurface(surface)
}

function sortSurface(surface: ExportSurface): ExportSurface {
  const sorted: ExportSurface = {}

  for (const name of Object.keys(surface).sort()) {
    sorted[name] = surface[name]
  }

  return sorted
}

/** The two published entry points, keyed by the name used in the baseline. */
export const PUBLISHED_ENTRIES = {
  // package root — `js-redis-server`
  index: 'index',
  // `js-redis-server/core`
  core: 'core',
} as const

export type PublishedEntry = keyof typeof PUBLISHED_ENTRIES

export type ExportBaseline = {
  readonly generatedBy: string
  readonly entries: Record<PublishedEntry, ExportSurface>
}

export function declarationFileFor(
  entry: PublishedEntry,
  extension: '.d.ts' | '.d.mts' = '.d.ts',
): string {
  return fileURLToPath(new URL(`../dist/${entry}${extension}`, import.meta.url))
}

export function readAllEntrySurfaces(
  extension: '.d.ts' | '.d.mts' = '.d.ts',
): Record<PublishedEntry, ExportSurface> {
  const entries = {} as Record<PublishedEntry, ExportSurface>

  for (const entry of Object.keys(PUBLISHED_ENTRIES) as PublishedEntry[]) {
    entries[entry] = readExportSurface(declarationFileFor(entry, extension))
  }

  return entries
}
