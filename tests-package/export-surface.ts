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
//   3. `variants` — every union arm the extractor does not descend into, by
//      canonicalised source text, so that dropping `'noscript'` from
//      `CommandFlag` or an arm from `RedisDataValue` reads as a removal. Text
//      rather than the literal's value, so `2 | 3` and `'2' | '3'` are not
//      byte-identical. "Does not descend into" means anything other than an
//      inline object literal, an intersection or a nested union — so a named
//      object type used as an arm (`RedisHashData | RedisListData | …`) is a
//      variant, recorded by its spelling, not a set of members.
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
// there is still caught. Members carry a prefix naming the namespace they were
// declared in, so the type side and the value side of one symbol cannot mask
// each other: `static:` for class statics, `type:` for anything declared on an
// interface or a type alias, bare for the value side. The prefix depends on the
// declaration a member was written on and on nothing else — see
// `MemberNamespace`.
//
// A symbol declared in an external library is pinned by name only. Nothing
// re-exports a dependency's type today, but doing so would otherwise snapshot
// that dependency's declared members, turning every version bump of it into
// baseline churn and its API changes into "breaks" in this package.
//
// A re-exported Node *builtin* never reaches that guard — it trips the
// unresolvable-declaration invariant first. The reason is narrow and worth
// stating exactly, because it has been described wrongly twice: the program
// below is created with no `types` and no `typeRoots`, so `@types/node` never
// enters it at all (`resolveModuleName('net')` is UNRESOLVED, 0 `@types/node`
// files in the program). Add `types: ['node']` and `Socket` resolves with one
// declaration. It is one config key away, not a property of
// `moduleResolution: Bundler` or of declaration-only inputs.
//
// `FEATURE_GATES.members` deliberately repeats `FeatureId.variants`. Measured
// cost: adding one feature gate is a 3-line baseline diff, not the wholesale
// churn that would make the file rubber-stampable. What the redundancy buys is
// narrower than it looks — a declaration file emits the *annotation*, so while
// `FEATURE_GATES` stays annotated `Record<FeatureId, VersionGate>` (or even
// `Partial<Record<…>>`) its recorded keys are just `FeatureId` again and a
// dropped key is invisible on both. It pays off when the annotation is dropped
// and the inferred object type is emitted instead: the keys then become
// independent surface and a drop is caught as `FEATURE_GATES.<key> — member
// gone`.

import ts from 'typescript'
import { fileURLToPath } from 'node:url'

export type ExportKind = 'value' | 'type'

export type ExportEntry = {
  readonly kind: ExportKind
  /**
   * Own members of the declaration, namespace-prefixed and sorted. Omitted when
   * there are none.
   */
  readonly members?: readonly string[]
  /**
   * Union arms the extractor does not descend into, by canonicalised source
   * text, sorted. Omitted when there are none.
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

/**
 * Which namespace a member belongs to. A symbol's type side and value side can
 * both carry a member called `name` and mean unrelated things, so the two are
 * recorded in disjoint namespaces.
 *
 * The prefix follows the *declaration the member was written on* and nothing
 * else. Making it conditional on what else the symbol declares — "prefix the
 * type half only when there is also a value half" — looks tighter but is
 * unstable: adding a factory const beside an existing exported type alias would
 * rename every one of its members, and the diff would report a strictly
 * additive change as BREAKING. For the same reason `interface` and `type` must
 * agree, so that converting one to the other stays invisible.
 */
type MemberNamespace = 'value' | 'static' | 'type'

const MEMBER_PREFIX: Record<MemberNamespace, string> = {
  value: '',
  static: 'static:',
  type: 'type:',
}

function memberName(
  member: ts.NamedDeclaration,
  namespace: MemberNamespace,
): string | undefined {
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

  const effective =
    namespace === 'value' && modifiers & ts.ModifierFlags.Static
      ? 'static'
      : namespace

  return `${MEMBER_PREFIX[effective]}${name.text}`
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
      const name = memberName(member, 'type')
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
    // Every arm contributes something. Arms the extractor descends into —
    // inline object literals, intersections, nested unions — contribute their
    // members, deliberately the *union* of them rather than the intersection: a
    // field present on only one arm still counts as published surface. The cost
    // is that moving a field between arms is invisible; the benefit is that
    // deleting it outright is not, and before this branch existed a union-typed
    // export recorded nothing at all (`CreateIoredisMockOptions`,
    // `CompatibilitySpec`, `SeedEntry`, …).
    //
    // Every other arm is recorded in `variants` by its source text. Recording
    // only string and numeric literals would have been worse than recording
    // nothing: the entry would look pinned while `-1` (a `PrefixUnaryExpression`,
    // and the canonical Redis sentinel), `1n`, `undefined` and template-literal
    // arms vanished silently next to their captured siblings. Partial-but-
    // plausible is the same failure shape as a half-resolved program.
    //
    // A descended arm that turns out to contribute nothing — an intersection of
    // two named types, an object with only an index signature — falls back to
    // its source text for the same reason. Otherwise deleting that whole arm,
    // which narrows the type for every consumer passing that shape, diffs to
    // nothing while its literal siblings sit there looking pinned.
    for (const constituent of node.types) {
      if (isMemberBearing(constituent)) {
        // Collected separately, not by watching `into` grow: two arms of the
        // same union routinely declare the same fields, and an arm whose
        // fields a previous arm already contributed still contributed them.
        const arm: Collected = { members: new Set(), variants: new Set() }
        collectFromTypeNode(constituent, arm)

        if (arm.members.size > 0 || arm.variants.size > 0) {
          for (const member of arm.members) {
            into.members.add(member)
          }
          for (const variant of arm.variants) {
            into.variants.add(variant)
          }
          continue
        }
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
 * A union arm, canonicalised to a stable string.
 *
 * Source text rather than `literal.text`, because `.text` strips the quotes and
 * makes `2 | 3` and `'2' | '3'` byte-identical in the baseline — so normalising
 * `RespVersion` from numbers to strings, which breaks every consumer passing
 * `{ version: 2 }`, would diff to nothing.
 *
 * But raw `getText()` copies the source's own quote characters and parentheses
 * into the baseline, which couples it to `.prettierrc`: flip `singleQuote` and
 * a pure reformat reports 87 breaking removals. So string literals are re-quoted
 * in one canonical style and parentheses are unwrapped. `2` stays `2`, and `-1`,
 * `1n`, `undefined` and template-literal arms keep their source spelling, which
 * has no such degree of freedom.
 *
 * A type *reference* arm is pinned by its spelling, so renaming a non-exported
 * internal type that appears as an arm reports a consumer-invisible refactor as
 * BREAKING. Not reachable today — every reference-shaped variant in the baseline
 * is either an exported type or a global (`Buffer`, `Promise<RedisResult>`).
 */
function sourceTextOf(node: ts.Node): string {
  const inner = ts.isTypeNode(node) ? unwrapParens(node) : node

  if (ts.isLiteralTypeNode(inner) && ts.isStringLiteral(inner.literal)) {
    const escaped = inner.literal.text
      .replace(/\\/g, '\\\\')
      .replace(/'/g, "\\'")

    return `'${escaped}'`
  }

  return inner.getText().replace(/\s+/g, ' ').trim()
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
 *  - `aliasSymbol` must not *be* a symbol this surface already exports. A type
 *    *alias* creates no distinct type, so `const DEFAULTS: Opts` where
 *    `type Opts = { alpha, beta }` is "anonymous" and would duplicate `Opts`'s
 *    own entry. Worse, it made the outcome depend on whether `Opts` was spelled
 *    `type` or `interface`: converting one to the other is invisible to every
 *    consumer, but flipped `DEFAULTS` between having members and not, which the
 *    diff then reported as BREAKING and sent the author off to write a
 *    changelog entry for a no-op.
 *
 *    Compared by symbol identity, not by name. By name the gate misses a
 *    renamed re-export (`export { type Opts as Options }` leaves `aliasSymbol`
 *    called `Opts` while the surface holds `Options`, so the flip-flop is still
 *    reachable), and fires wrongly when a const is annotated with a
 *    *non-exported* alias whose name happens to collide with an unrelated
 *    export — dropping members nothing else pins.
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
  exportedSymbols: ReadonlySet<ts.Symbol>,
): string[] {
  const type = checker.getTypeOfSymbolAtLocation(symbol, declaration)

  if (!(type.flags & ts.TypeFlags.Object)) {
    return []
  }

  const objectFlags = (type as ts.ObjectType).objectFlags

  if (!(objectFlags & (ts.ObjectFlags.Anonymous | ts.ObjectFlags.Mapped))) {
    return []
  }

  if (type.aliasSymbol !== undefined && exportedSymbols.has(type.aliasSymbol)) {
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
 *
 * Asked of the compiler rather than matched against `/node_modules/` in the
 * path. The path test describes where the *file* sits, not where the dependency
 * came from: read a `dist/` that itself lives under a `node_modules/` directory
 * — a linked or vendored checkout, a workspace install, CI running against an
 * installed copy — and every member and variant in the surface disappears while
 * it still looks entirely plausible. Neither invariant catches that, because
 * every declaration resolves fine. Run `export-baseline` from such a tree and
 * the emptied baseline gets committed, after which every real member removal
 * passes green forever.
 */
function isExternalDeclaration(
  program: ts.Program,
  declaration: ts.Declaration,
): boolean {
  return program.isSourceFileFromExternalLibrary(declaration.getSourceFile())
}

/** Named members of a `declare namespace` block. */
function namespaceMemberNames(body: ts.ModuleBlock): string[] {
  const names: string[] = []

  for (const statement of body.statements) {
    if (ts.isVariableStatement(statement)) {
      for (const declaration of statement.declarationList.declarations) {
        if (ts.isIdentifier(declaration.name)) {
          names.push(declaration.name.text)
        }
      }
      continue
    }

    if (
      (ts.isFunctionDeclaration(statement) ||
        ts.isClassDeclaration(statement) ||
        ts.isEnumDeclaration(statement) ||
        ts.isInterfaceDeclaration(statement) ||
        ts.isTypeAliasDeclaration(statement)) &&
      statement.name
    ) {
      names.push(statement.name.text)
    }
  }

  return names
}

function describeSymbol(
  program: ts.Program,
  checker: ts.TypeChecker,
  symbol: ts.Symbol,
  exportedSymbols: ReadonlySet<ts.Symbol>,
): ExportEntry {
  const collected: Collected = { members: new Set(), variants: new Set() }

  for (const declaration of symbol.declarations ?? []) {
    if (isExternalDeclaration(program, declaration)) {
      continue
    }

    if (ts.isInterfaceDeclaration(declaration)) {
      for (const member of declaration.members) {
        const name = memberName(member, 'type')
        if (name !== undefined) {
          collected.members.add(name)
        }
      }
      continue
    }

    if (ts.isClassDeclaration(declaration)) {
      for (const member of declaration.members) {
        const name = memberName(member, 'value')
        if (name !== undefined) {
          collected.members.add(name)
        }
      }
      continue
    }

    if (ts.isEnumDeclaration(declaration)) {
      for (const member of declaration.members) {
        const name = memberName(member, 'value')
        if (name === undefined) {
          continue
        }

        // With the initializer, not just the name: a `const enum` value is
        // inlined into a consumer's build, so changing `A = 0` to `A = 5` is a
        // break of the same class as `2 | 3` becoming `'2' | '3'` — which the
        // union handling goes out of its way to make visible.
        collected.members.add(
          member.initializer
            ? `${name}=${sourceTextOf(member.initializer)}`
            : name,
        )
      }
      continue
    }

    if (ts.isModuleDeclaration(declaration)) {
      // `declare namespace fn { … }` merged onto a function or class. Its
      // exported members are callable published surface.
      const body = declaration.body

      if (body && ts.isModuleBlock(body)) {
        for (const name of namespaceMemberNames(body)) {
          collected.members.add(name)
        }
      }
      continue
    }

    if (ts.isTypeAliasDeclaration(declaration)) {
      collectFromTypeNode(declaration.type, collected)
      continue
    }

    if (ts.isVariableDeclaration(declaration)) {
      for (const name of checkerMembersOfConst(
        checker,
        symbol,
        declaration,
        exportedSymbols,
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

  const resolve = (symbol: ts.Symbol): ts.Symbol =>
    symbol.flags & ts.SymbolFlags.Alias
      ? checker.getAliasedSymbol(symbol)
      : symbol

  // Resolved symbols, by identity — see `checkerMembersOfConst`'s second gate.
  const exportedSymbols = new Set(exported.map(resolve))

  const surface: ExportSurface = {}
  const unresolved: string[] = []

  for (const symbol of exported) {
    const resolved = resolve(symbol)

    if ((resolved.declarations?.length ?? 0) === 0) {
      // The symbol survived as a name but its declaration did not resolve —
      // typically a missing shared chunk. Everything derived from it (kind,
      // members) would be silently wrong.
      unresolved.push(symbol.getName())
      continue
    }

    surface[symbol.getName()] = describeSymbol(
      program,
      checker,
      resolved,
      exportedSymbols,
    )
  }

  if (unresolved.length > 0) {
    throw new Error(
      `${entryDeclarationFile}: ${unresolved.length} exported symbol(s) have no ` +
        `resolvable declaration, so the extracted surface would be wrong. ` +
        `Either the build is stale — rebuild with \`npm run build\` — or the ` +
        `entry re-exports something this program cannot resolve, such as a Node ` +
        `builtin (no \`types\`/\`typeRoots\` are configured, so \`@types/node\` ` +
        `is not in the program). First few: ${unresolved.slice(0, 5).join(', ')}`,
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
      gone.push(`${name} — union variant ${variant} gone`)
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
      added.push(`${name} — union variant ${variant}`)
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
