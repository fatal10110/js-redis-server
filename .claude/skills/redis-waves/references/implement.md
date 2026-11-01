# Implementing a js-redis-server issue

The project goal is a mock that answers exactly like the real server of the selected
compatibility profile, byte for byte on the wire. Every behavior claim is settled by a real
server, never by memory of Redis or by what the issue text says.

Priorities: the library stands in for Redis/Valkey in CI and tests. Functional parity and
per-version compatibility come first. Memory use, buffer retention, throughput and other
production hardening come last: do them only when cheap or when they would make a test run hang
or blow up, and never trade parity for them.

## 1. Pin the target behavior

- Read each issue and its comments once (`gh issue view <n> -R fatal10110/js-redis-server --comments`).
  Later comments often narrow or re-scope the body ("Still open: ..."); they win.
- Check the gap still exists on `origin/main`: reproduce it on the mock first (a failing test is
  the reproduction, see 3).
- Capture the real replies for every profile the issue touches with `probe.py run` (default: all
  seven profiles). Probe the edge cases too: wrong arity, wrong type, missing key, empty
  collections, RESP2 and RESP3 (`--resp 3`), inside MULTI, from a script
  (`EVAL "return redis.pcall(...)" 0`). Note where versions differ.
- Explain each difference from the server source at the version tags (`look.py
  redis@<tag>:src/<file>.c:fn:<function>`), so the gate lands on the release that changed it,
  not on the nearest profile you happened to probe. Profiles pin exact versions
  (`src/core/compatibility/profile.ts`): redis 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0, valkey 8.0.0,
  9.0.0.
- An issue already fixed on main, or stale: no code; comment the evidence (main SHA, file:line,
  the PR that fixed it) and close it as completed; leave it out of "Closes".

## 2. Design the change

- Read docs/ARCHITECTURE.md for the area you touch. Commands are pure `(args, ctx) => RedisResult`
  definitions (`src/commands/`), parsed through `schema` (`src/core/command-schema.ts`); routing,
  queueing and policies live in `src/core/` (executor, `execution-policies/`, `client-session.ts`);
  data in `src/state/`; the wire in `src/core/transports/`.
- Version differences are feature gates: add a `FeatureId` with its first redis / valkey version
  in `src/core/compatibility/feature-gates.ts` and test it with `ctx.server.profile.has(...)`.
  Never compare version strings in command code.
- A fixed message used from more than one place, or worded per profile, is a factory on the
  `errors` object (`src/core/redis-error.ts`); keep subclasses to the cases AGENTS.md lists.
- A new command follows AGENTS.md "Adding New Commands" (schema-derived arity, key specs from the
  captured command table via `scripts/capture-command-table.ts`, registration in
  `src/commands/index.ts`).
- Prefer the smallest change that makes every probed row match. Do not refactor beyond the lane.
  Performance is not a goal; correctness and Redis fidelity are.

## 3. Tests first

- Write the failing test before the fix, at the right tier (AGENTS.md "Client API Preference"):
  typed ioredis / node-redis client methods for client-visible behavior; `tests-integration/raw-tcp/`
  for exact bytes, protocol and wording; `tests-integration/compatibility/` for anything that
  varies by profile (expectations follow `REDIS_COMPAT` through `activeProfile`, so the same file
  runs against real servers of every version); `tests/` for pure units.
- Keys are unique per run (`randomKey()`); never assert on DBSIZE or a fixed key name. Cross-slot
  probes use `keyInAnotherSlot`.
- Prove each new integration test both ways: red on the mock before the fix, green on the mock
  after it, and green against the real server of each profile it covers:
  `probe.py up -p <profiles>` prints ports; then
  `TEST_BACKEND=real REDIS_COMPAT=<profile> REDIS_STANDALONE_PORT=<port> node --import tsx --no-warnings --test-timeout 60000 --test <file>`
  (standalone/raw-tcp files; cluster suites run against real 8.0 only, in CI).
- Containers you start, you remove: a `probe.py run/up` with `--args` or `PROFILE@TAG` starts
  one-off containers of your own (owner: your worktree's name) and prints the
  `probe.py down -p ... --args ... --owner ...` line that removes them; run it once you are done
  with them and before you return. Leave the shared default containers (pinned versions, no
  `--args`) running. Run `probe.py up` again right before each real-server test run, so the
  container counts as in use.
- A test fixed by your change that is listed in `tests-integration/socketless/known-gaps.ts` must
  leave that list (the list is strict).
- Put new tests in new or small files where you can, to avoid conflicts with other lanes.

## 4. Verify (scoped; CI runs everything)

All through `heavy.sh` (it serializes heavy runs on this box):
- `npx tsc --noEmit`, `npx eslint <changed files>`, `npx prettier --check <changed files>`;
- the unit suite (`node --import tsx --no-warnings --test-timeout 60000 --test "./tests/**/*.test.ts"`, about 20 s);
- the integration files you added or changed, plus the files covering the code you touched,
  with `TEST_BACKEND=mock`, and under each `REDIS_COMPAT` profile you gated
  (the compatibility profile loop is `for p in redis-6.2 ... valkey-9.0`);
- the real-server runs above for your new integration tests.
Never run the whole `test:integration:*` suites or anything against the shared docker-compose
stack (`clean:redis`, `test:integration:real*`): CI does that, and the stack is shared.

## 5. Document and open the PR

- `CHANGELOG.md` `[Unreleased]`: one entry per user-visible change, in the section that fits
  (Fixed / Added / Changed), in the project's voice (what now matches real Redis, per version).
  BREAKING changes to `js-redis-server/core` are marked as AGENTS.md says. New commands also go in
  `docs/COMMANDS.md`; structural changes in docs/ARCHITECTURE.md; test-harness changes in
  docs/TESTING.md / docs/TEST-INTEGRATION.md.
- Commit message: `fix: <what now matches> (#n, #m)` (or `feat:` / `refactor:` / `test:` /
  `docs:`), a body explaining the behavior per version and how it was verified, then a blank line
  and the Co-Authored-By trailer your lane instructions name.
- Push `wf/<slug>`, open ONE ready-for-review PR (`gh pr create -R fatal10110/js-redis-server
  --base main --head wf/<slug>`). Body sections: Summary (behavior per profile, what changed),
  Verification (the probes and real-server runs, with versions; the scoped tests you ran; "full
  suite: CI"), Decisions (where an issue offered options), Follow-ups (filed issues), then one
  `Closes #n` line per fully resolved issue and `Part of #n` for partial ones, and last
  `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. No model names anywhere.
- Out-of-scope gaps you find: search open issues first (`gh issue list -R fatal10110/js-redis-server
  --search "<terms>" --state open`), amend a matching one with a comment, else file a new issue with
  real-server evidence and acceptance criteria, in the style of the existing issues.
