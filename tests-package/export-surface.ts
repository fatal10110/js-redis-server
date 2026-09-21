// Reads the *published* export surface out of the emitted declaration files
// (`dist/index.d.ts`, `dist/core.d.ts`) using the TypeScript compiler API, and
// diffs two such surfaces.
//
// Why the `.d.ts` and not the built JS: runtime enumeration (`Object.keys` on
// `require('js-redis-server/core')`) only sees *value* exports. Most of the
// `/core` surface — `RedisClientSession`, `CommandPlan`, `Resp2ServerOptions`,
// every `*Options` type — is type-only and therefore invisible at runtime, and
// that is where two of the four breaks this was written for lived. The
// declaration bundle is the only artifact that carries both.
//
// Three levels are captured per entry point:
//
//   1. the exported symbol names, each tagged `value` (has a runtime binding:
//      class, function, const) or `type` (interface / type alias only),
//   2. `members` — the *own* members declared on each exported interface,
//      class, object type alias, or `const` object namespace, and
//   3. `variants` — every non-object constituent of a union, by source text, so
//      that dropping `'noscript'` from `CommandFlag` or an arm from
//      `RedisDataValue` reads as a removal. Source text rather than the
//      literal's value, so `2 | 3` and `'2' | '3'` are not byte-identical.
//
// Levels 2 and 3 are what make this guard worth having. Of the four `/core`
// breaks it responds to, two were invisible to a symbol-list-only snapshot:
// an option dropped from four option types (#374) and six interface methods
// collapsed into two (#376). (#375 and #377 each also removed a top-level
// symbol, so a plain list would have caught those two — member level still
// catches *more* of #377: `CommandRegistry.override/has/getNames`,
// `CommandPlan.flags`, `ClientSessionOptions.turnQueue`.)
//
// "Own members" means members written on the declaration itself — inherited
// members are deliberately not walked. That keeps 120-odd error subclasses from
// each snapshotting `message`/`stack`/`cause` out of `lib.es5.d.ts`, which would
// churn the baseline on every TypeScript bump while proving nothing about this
// package. `RedisCommandError` still snapshots its own `code`, so deleting it
// there is still caught. Members carry a namespace prefix where two namespaces
// would otherwise collide in one list: `static:` for class statics (so
// `static create()` cannot mask an instance `create()`), and `type:` for the
// type half of a declaration-merged symbol (`RedisValue` is the only one — its
// union arms' field names are not callable factories).
//
// A symbol whose declaration lives in `node_modules` is pinned by name only.
// Nothing re-exports a dependency's type today, but doing so would otherwise
// snapshot that dependency's declared members, turning every version bump of it
// into baseline churn and its API changes into "breaks" in this package.
// (A re-exported Node *builtin* never reaches that guard: under this program's
// options `export type { Socket } from 'net'` does not resolve at all and trips
// the unresolvable-declaration invariant first.)
//
// `FEATURE_GATES.members` deliberately repeats `FeatureId.variants`. TypeScript
// keeps them in sync today only because the const is annotated
// `Record<FeatureId, VersionGate>`; loosen that to `Partial<Record<…>>` and a
// dropped key would break consumers reading the object at runtime while the
// type union still advertised it. Measured cost of the redundancy: adding one
// feature gate is a 3-line baseline diff, not the wholesale churn that would
// make the file rubber-stampable.

import ts from 'typescript'
import { fileURLToPath } from 'node:url'

export type ExportKind = 'value' | 'type'

export type ExportEntry = {
  readonly kind: ExportKind
  /** Own members of the declaration, sorted. Omitted when there are none. */
  readonly members?: readonly string[]
  /**
   * Non-object constituents of a union, by source text, sorted. Omitted when
   * there are none.
   */
  readonly variants?: readonly string[]
}

/** Exported symbol name -> what it is. Keys are sorted. */
export type ExportSurface = Record<string, ExportEntry>

const HIDDEN_MEMBER = ts.ModifierFlags.Private | ts.ModifierFlags.Protected

type Collected = {
  readonly members: Set<string>
  readonly variants: Set<string>
}

function memberName(member: ts.NamedDeclaration): string | undefined {
  const modifiers = ts.getCombinedModifierFlags(member as ts.Declaration)

  if (modifiers & HIDDEN_MEMBER) {
    return undefined
  }

  const name = member.name

  if (!name) {
    // Index signatures, call/construct signatures, static blocks — nothing
    // nameable to pin.
    return undefined
  }

  if (!ts.isIdentifier(name) && !ts.isStringLiteral(name)) {
    // `#private` fields and computed names are not part of the surface.
    return undefined
  }

  // Statics and instance members live in separate namespaces; flattening them
  // into one list would let `static create()` mask an instance `create()`.
  return modifiers & ts.ModifierFlags.Static ? `static:${name.text}` : name.text
}

function collectFromTypeNode(
  node: ts.TypeNode | undefined,
  into: Collected,
): void {
  if (!node) {
    return
  }

  if (ts.isParenthesizedTypeNode(node)) {
    collectFromTypeNode(node.type, into)
    return
  }

  if (ts.isTypeLiteralNode(node)) {
    for (const member of node.members) {
      const name = memberName(member)
      if (name !== undefined) {
        into.members.add(name)
      }
    }
    return
  }

  if (ts.isIntersectionTypeNode(node)) {
    // `NodePipeline & { host: string; port: number }` contributes the literal's
    // own members; the referenced half is snapshotted under its own name.
    for (const constituent of node.types) {
      collectFromTypeNode(constituent, into)
    }
    return
  }

  if (ts.isUnionTypeNode(node)) {
    // Every constituent contributes. Object-shaped arms contribute their
    // members — deliberately the *union* of them, not the intersection: a field
    // present on only one arm still counts as published surface. The cost is
    // that moving a field between arms is invisible; the benefit is that
    // deleting it outright is not, and before this branch existed a union-typed
    // export recorded nothing at all (`CreateIoredisMockOptions`,
    // `CompatibilitySpec`, `SeedEntry`, …).
    //
    // Every other arm is recorded in `variants` by its *source text*. Recording
    // only string and numeric literals would have been worse than recording
    // nothing: the entry would look pinned while `-1` (a `PrefixUnaryExpression`,
    // and the canonical Redis sentinel), `1n`, `undefined` and template-literal
    // arms vanished silently next to their captured siblings. Partial-but-
    // plausible is the same failure shape as a half-resolved program.
    for (const constituent of node.types) {
      if (isMemberBearing(constituent)) {
        collectFromTypeNode(constituent, into)
        continue
      }

      into.variants.add(sourceTextOf(constituent))
    }
    return
  }

  if (ts.isLiteralTypeNode(node)) {
    // A literal reached outside a union — a single-arm alias such as
    // `type Only = 'x'`.
    into.variants.add(sourceTextOf(node))
    return
  }

  // Type references, mapped types, keyof, … reached outside a union have no own
  // members to pin syntactically. Variable declarations get a checker-based
  // pass below.
}

function unwrapParens(node: ts.TypeNode): ts.TypeNode {
  return ts.isParenthesizedTypeNode(node) ? unwrapParens(node.type) : node
}

/** Union arms worth descending into for members rather than recording as text. */
function isMemberBearing(node: ts.TypeNode): boolean {
  const inner = unwrapParens(node)

  return (
    ts.isTypeLiteralNode(inner) ||
    ts.isIntersectionTypeNode(inner) ||
    ts.isUnionTypeNode(inner)
  )
}

/**
 * A union arm's source text, whitespace-collapsed.
 *
 * Source text rather than `literal.text`, because `.text` strips the quotes and
 * makes `2 | 3` and `'2' | '3'` byte-identical in the baseline — so normalising
 * `RespVersion` from numbers to strings, which breaks every consumer passing
 * `{ version: 2 }`, would diff to nothing. `getText()` keeps them apart and
 * handles `-1`, `1n`, `undefined` and template-literal arms for free.
 */
function sourceTextOf(node: ts.Node): string {
  return node.getText().replace(/\s+/g, ' ').trim()
}

/**
 * Members of a `const` object namespace, resolved through the checker so that
 * `Record<FeatureId, VersionGate>` and other mapped types answer with their real
 * keys instead of nothing.
 *
 * Two gates, both meaning "pin only what is written *here*":
 *
 *  - the type must be anonymous or mapped. A const annotated with an interface
 *    — `export const getCommand: CommandDefinition<…>`, or any of the command
 *    arrays — would otherwise resolve to that interface's (or `Array`'s)
 *    members, which are either snapshotted under their own exported name or
 *    pure `lib` noise. On the current `/core` build this blocks 55 of 59
 *    consts and 742 member entries.
 *
 *  - `aliasSymbol` must not name something this surface already exports. A type
 *    *alias* creates no distinct type, so `const DEFAULTS: Opts` where
 *    `type Opts = { alpha, beta }` is "anonymous" and would duplicate `Opts`'s
 *    own entry. Worse, it made the outcome depend on whether `Opts` was spelled
 *    `type` or `interface`: converting one to the other is invisible to every
 *    consumer, but flipped `DEFAULTS` between having members and not, which the
 *    diff then reported as BREAKING and sent the author off to write a
 *    changelog entry for a no-op.
 *
 * Intersections are not descended here: `type.flags & Object` is false for
 * them, so `const wired: Opts & Named` records nothing while the structurally
 * identical `type Wired = Opts & Named` records everything. That asymmetry is
 * deliberate rather than fixed — for the const the members are already pinned
 * under `Opts` and `Named`, which is exactly what the second gate is for.
 */
function checkerMembersOfConst(
  checker: ts.TypeChecker,
  symbol: ts.Symbol,
  declaration: ts.VariableDeclaration,
  exportedNames: ReadonlySet<string>,
): string[] {
  const type = checker.getTypeOfSymbolAtLocation(symbol, declaration)

  if (!(type.flags & ts.TypeFlags.Object)) {
    return []
  }

  const objectFlags = (type as ts.ObjectType).objectFlags

  if (!(objectFlags & (ts.ObjectFlags.Anonymous | ts.ObjectFlags.Mapped))) {
    return []
  }

  const alias = type.aliasSymbol?.getName()

  if (alias !== undefined && exportedNames.has(alias)) {
    return []
  }

  return type
    .getProperties()
    .filter(property => !property.getName().startsWith('#'))
    .map(property => property.getName())
}

/**
 * Re-exporting a dependency's type would snapshot that dependency's own
 * declared members, so every version bump of it would churn the baseline and
 * the removal check would start reporting its API changes as breaks in this
 * package. Nothing does so today; this keeps it that way by construction. The
 * symbol itself is still pinned, just without members.
 */
function isThirdPartyDeclaration(declaration: ts.Declaration): boolean {
  return declaration.getSourceFile().fileName.includes('/node_modules/')
}

function describeSymbol(
  checker: ts.TypeChecker,
  symbol: ts.Symbol,
  exportedNames: ReadonlySet<string>,
): ExportEntry {
  const collected: Collected = { members: new Set(), variants: new Set() }
  const declarations = symbol.declarations ?? []

  // A declaration-merged symbol — `RedisValue` is the only one on either entry
  // point — has a type half and a value half whose names mean different things.
  // `RedisValue.kind` is a field of the union; `RedisValue.push()` is a factory
  // on the const. Flattening both into one list re-introduces, one level up,
  // exactly the masking `static:` was added to stop: a future factory named
  // `name` or `items` could be deleted unreported because a union arm already
  // contributed that string. It also makes the entry unreviewable — you cannot
  // tell which of the 24 names are callable.
  const isMerged =
    declarations.some(ts.isTypeAliasDeclaration) &&
    declarations.some(
      d => ts.isVariableDeclaration(d) || ts.isFunctionDeclaration(d),
    )

  for (const declaration of declarations) {
    if (isThirdPartyDeclaration(declaration)) {
      continue
    }

    if (
      ts.isInterfaceDeclaration(declaration) ||
      ts.isClassDeclaration(declaration)
    ) {
      for (const member of declaration.members) {
        const name = memberName(member)
        if (name !== undefined) {
          collected.members.add(name)
        }
      }
      continue
    }

    if (ts.isEnumDeclaration(declaration)) {
      for (const member of declaration.members) {
        const name = memberName(member)
        if (name !== undefined) {
          collected.members.add(name)
        }
      }
      continue
    }

    if (ts.isTypeAliasDeclaration(declaration)) {
      if (!isMerged) {
        collectFromTypeNode(declaration.type, collected)
        continue
      }

      const typeHalf: Collected = { members: new Set(), variants: new Set() }
      collectFromTypeNode(declaration.type, typeHalf)

      for (const name of typeHalf.members) {
        collected.members.add(`type:${name}`)
      }
      for (const variant of typeHalf.variants) {
        collected.variants.add(variant)
      }
      continue
    }

    if (ts.isVariableDeclaration(declaration)) {
      for (const name of checkerMembersOfConst(
        checker,
        symbol,
        declaration,
        exportedNames,
      )) {
        collected.members.add(name)
      }
    }
  }

  const kind: ExportKind =
    symbol.flags & ts.SymbolFlags.Value ? 'value' : 'type'

  return {
    kind,
    ...(collected.members.size > 0
      ? { members: [...collected.members].sort() }
      : {}),
    ...(collected.variants.size > 0
      ? { variants: [...collected.variants].sort() }
      : {}),
  }
}

/**
 * Parse one emitted declaration bundle and return its full export surface.
 *
 * `entryDeclarationFile` is an absolute path to a `dist/*.d.ts` (or `.d.mts`).
 * The bundle's shared chunk is resolved by the compiler, so symbols that tsup
 * hoisted out of the entry file are still seen.
 *
 * Throws rather than returning a thin surface when the program did not resolve
 * properly. A half-resolved program is the dangerous failure here: it returns a
 * plausible-looking symbol list with members and type-only kinds quietly
 * missing, and `skipLibCheck` (which these all-declaration inputs need)
 * suppresses the `TS2307` that would otherwise reveal it. The two invariants
 * below are structural, so they hold regardless of diagnostics.
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

  const exported = checker.getExportsOfModule(moduleSymbol)

  if (exported.length === 0) {
    throw new Error(
      `${entryDeclarationFile} resolved to zero exports — the build is broken`,
    )
  }

  const surface: ExportSurface = {}
  const unresolved: string[] = []
  const exportedNames = new Set(exported.map(symbol => symbol.getName()))

  for (const symbol of exported) {
    const resolved =
      symbol.flags & ts.SymbolFlags.Alias
        ? checker.getAliasedSymbol(symbol)
        : symbol

    if ((resolved.declarations?.length ?? 0) === 0) {
      // The symbol survived as a name but its declaration did not resolve —
      // typically a missing shared chunk. Everything derived from it (kind,
      // members) would be silently wrong.
      unresolved.push(symbol.getName())
      continue
    }

    surface[symbol.getName()] = describeSymbol(checker, resolved, exportedNames)
  }

  if (unresolved.length > 0) {
    throw new Error(
      `${entryDeclarationFile}: ${unresolved.length} exported symbol(s) have no ` +
        `resolvable declaration, so the extracted surface would be wrong. ` +
        `Rebuild with \`npm run build\`. First few: ${unresolved.slice(0, 5).join(', ')}`,
    )
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

// ---------------------------------------------------------------------------
// Diffing
//
// Exported (rather than kept local to the test) so the comparison itself can be
// unit-tested against synthetic surfaces. A guard whose diff quietly stops
// diffing passes every assertion it has, which is the exact failure mode this
// whole file exists to prevent.
// ---------------------------------------------------------------------------

function missing(
  before: readonly string[] | undefined,
  after: readonly string[] | undefined,
): readonly string[] {
  const present = new Set(after ?? [])
  return (before ?? []).filter(name => !present.has(name))
}

/**
 * Everything in `baseline` that `current` no longer has, as human-readable
 * lines. A non-empty result is a breaking change.
 */
export function findRemovals(
  baseline: ExportSurface,
  current: ExportSurface,
): readonly string[] {
  const gone: string[] = []

  for (const [name, entry] of Object.entries(baseline)) {
    const now = current[name]

    if (!now) {
      gone.push(`${name} (${entry.kind}) — gone`)
      continue
    }

    if (entry.kind === 'value' && now.kind === 'type') {
      gone.push(
        `${name} — was a value export, is now type-only (no runtime binding)`,
      )
    }

    for (const member of missing(entry.members, now.members)) {
      gone.push(`${name}.${member} — member gone`)
    }

    for (const variant of missing(entry.variants, now.variants)) {
      gone.push(`${name} — union variant '${variant}' gone`)
    }
  }

  return gone
}

/**
 * Everything in `current` that `baseline` does not record. Not a breaking
 * change — a stale baseline.
 */
export function findAdditions(
  baseline: ExportSurface,
  current: ExportSurface,
): readonly string[] {
  const added: string[] = []

  for (const [name, entry] of Object.entries(current)) {
    const before = baseline[name]

    if (!before) {
      added.push(`${name} (${entry.kind})`)
      continue
    }

    if (before.kind === 'type' && entry.kind === 'value') {
      // Not breaking in itself — but leaving it unreported disarms the
      // downgrade check in `findRemovals` permanently. A type-only export that
      // gains a runtime binding (an interface replaced by a class, a type alias
      // joined by a same-named const) would never prompt a refresh, so the
      // baseline would keep `kind: "type"` forever; when the binding is removed
      // again, baseline `type` vs current `type` reports nothing and the
      // consumer's `import { Foo }` breaks at runtime with CI green. That is
      // the #375 shape — the one this field exists to catch.
      added.push(`${name} — is now a value export (was type-only)`)
    }

    for (const member of missing(entry.members, before.members)) {
      added.push(`${name}.${member}`)
    }

    for (const variant of missing(entry.variants, before.variants)) {
      added.push(`${name} — union variant '${variant}'`)
    }
  }

  return added
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/**
 * The two published entry points. `dist` is the emitted file's basename;
 * `subpath` is what a consumer appends to the package name to import it.
 */
export const PUBLISHED_ENTRIES = {
  index: { dist: 'index', subpath: '' },
  core: { dist: 'core', subpath: '/core' },
} as const

export type PublishedEntry = keyof typeof PUBLISHED_ENTRIES

export const PUBLISHED_ENTRY_NAMES = Object.keys(
  PUBLISHED_ENTRIES,
) as readonly PublishedEntry[]

export type ExportBaseline = {
  readonly generatedBy: string
  readonly entries: Record<PublishedEntry, ExportSurface>
}

/** How a consumer spells the import for this entry — `pkg` or `pkg/core`. */
export function importPathFor(
  packageName: string,
  entry: PublishedEntry,
): string {
  return `${packageName}${PUBLISHED_ENTRIES[entry].subpath}`
}

export function declarationFileFor(
  entry: PublishedEntry,
  extension: '.d.ts' | '.d.mts' = '.d.ts',
): string {
  return fileURLToPath(
    new URL(
      `../dist/${PUBLISHED_ENTRIES[entry].dist}${extension}`,
      import.meta.url,
    ),
  )
}

export function readAllEntrySurfaces(
  extension: '.d.ts' | '.d.mts' = '.d.ts',
): Record<PublishedEntry, ExportSurface> {
  const entries = {} as Record<PublishedEntry, ExportSurface>

  for (const entry of PUBLISHED_ENTRY_NAMES) {
    entries[entry] = readExportSurface(declarationFileFor(entry, extension))
  }

  return entries
}
