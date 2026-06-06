# SCAN Family + Keyspace Snapshot — Review Notes

Scope: uncommitted work on branch `codex/redis-architecture-refactor`.
Files: `src/commands/scan.ts`, `src/state/keyspace.ts` (`entriesSnapshot`),
`src/state/database.ts` (`entriesSnapshot` passthrough), plus scan tests.

Status at review: typecheck clean, 296/296 unit pass, scan unit 5/5,
scan integration (mock) 5/5.

Verdict: correct and tested. One real perf bug, two fidelity gaps, one glob edge.

---

## P1 (perf) — `entriesSnapshot()` deep-clones whole DB per SCAN/KEYS call

`src/state/keyspace.ts:176-192` clones every value via `cloneRedisDataValue`
(full copy of every hash/set/zset/list). But `src/commands/scan.ts` KEYS
(L36-40) and SCAN (L49-53) only read `entry.key` and `entry.value.type`.
Values are cloned and then discarded.

Cost: SCAN full iteration = `N/COUNT` calls x `O(N)` clone+filter+glob ->
`O(N^2)` plus `N^2/COUNT` deep copies. JS is single-threaded synchronous, so
the clone buys no correctness either — pure waste. Violates CLAUDE.md hot-path
guidance (minimize allocations).

Fix: add keys-only snapshot, no value clone.

```typescript
keysSnapshot(): { key: Buffer; type: RedisDataTypeName }[] {
  const out = []
  for (const entry of Array.from(this.entries.values())) {
    if (this.evictIfExpired(entry)) continue
    out.push({ key: entry.key, type: entry.value.type })
  }
  return out
}
```

HSCAN/SSCAN/ZSCAN already fine — `getTyped` returns a live ref
(`src/state/database.ts:137`), no clone.

## P2 (fidelity) — SCAN cursor = offset into freshly-rebuilt snapshot

`src/commands/scan.ts:273-302`. Cursor treated as positional offset; snapshot
order = Map insertion order. Add/del between calls shifts offsets -> keys
skipped or duplicated. Real Redis guarantees every key live for the full scan
is returned at least once (reverse-binary cursor, rehash-safe). Mock gap: fine
for single-threaded tests, breaks under concurrent mutation. Document as a
known limitation.

## P3 (fidelity) — COUNT applied AFTER match/type filter

`src/commands/scan.ts:278-282`. Redis applies COUNT to buckets scanned *before*
MATCH, so real Redis can return an empty page with a nonzero cursor. Here it is
filter-then-paginate, which never does that. More convenient, semantically
different. Does not break `while cursor != 0` client loops.

## P4 (glob edge) — `[a-]` trailing dash

`src/commands/scan.ts:414-431`: `[a-]` treats trailing `-` as a range start,
consuming `]` as the range end. Redis treats a trailing `-` as literal. Edge
case, unlikely in tests.

---

## Verified OK

- `getTyped` live ref -> no clone for hscan/sscan/zscan (`database.ts:137`)
- WRONGTYPE thrown on wrong-type HSCAN/SSCAN/ZSCAN (`database.ts:136`)
- type filter `'zset'` matches TYPE output
- `parseCount` <= 0 -> syntax error; `parseCursor` bad -> `invalid cursor`
- `normalizeScanCursor` huge cursor -> empty page, terminates
- itemWidth 1/2 pagination correct
- flags `['readonly','random']` correct

## Backlog (not this diff)

Legacy `src/commanders/custom` (~8.6k LOC) still ships dead — tracked in
`docs/REFACTOR-REVIEW-FINDINGS.md`.
