# Reviewing a js-redis-server PR

You review one PR that claims to fix one or more issues. The question is not "does the code look
fine" but "does the mock now answer exactly like the real server of every profile, for
everything the issues ask, without breaking anything else". Every finding carries evidence.

## Round 1 (full review)

1. Scope. Read each issue the PR closes or is part of, with comments (later comments re-scope the
   body). List every acceptance item. A "Closes #n" whose acceptance is not fully met is a
   [must-fix] (downgrade it to "Part of" or finish the work).
2. Ground truth. Re-probe the behavior yourself with `probe.py run` on every profile the PR
   touches, including at least one edge case the author's tests skip (RESP3, inside MULTI, from a
   script, wrong type, empty or missing key, wrong arity). For gates, read the source at the
   boundary tags (`look.py redis@<tag>:...`) and confirm the first version is right, not merely
   consistent with the probed profiles.
3. Code. Review the diff (`git diff --stat origin/main...<head>` first, then the hunks), and the
   surrounding code in ranges. Check against the project rules in AGENTS.md and docs/ARCHITECTURE.md:
   commands stay pure; version differences go through `FeatureId` gates, never version compares;
   profile-dependent or shared messages are `errors` factories; no new error subclass without the
   AGENTS.md reason; flags and key specs come from the captured command table; the same command
   still works standalone, in cluster, in MULTI/EXEC and in Lua (check each path the change
   reaches); no leaked state, timers, listeners or parked waiters; async paths cannot crash the
   process (unhandled rejections) or wedge the turn queue.
4. Tests. They must fail on the base and pass on the head: run the new/changed test files on the
   head, and at least one of them with the fix reverted: in your review worktree,
   `git checkout <base> -- <changed src files>`, run the test (it must fail), then
   `git checkout <head> -- <same files>`. Never use `git stash` (the stash is shared by every
   worktree). Run each new integration file against real servers of the profiles it covers
   (`probe.py up` + `TEST_BACKEND=real REDIS_COMPAT=... REDIS_STANDALONE_PORT=...`; remove any
   one-off container you start with the `probe.py down` line it prints). Expectations
   that encode mock behavior rather than probed real behavior are a [blocker]. Check the tier
   (typed client method > raw-tcp > `.call`), key isolation (`randomKey()`), and that fixed
   entries left `socketless/known-gaps.ts`.
5. Docs. CHANGELOG `[Unreleased]` entry, `docs/COMMANDS.md` for commands, ARCHITECTURE for
   structure; PR body has Verification with versions, Closes / Part of lines, no model names.
6. Run the scoped gates yourself through `heavy.sh`: `npx tsc --noEmit`, eslint and prettier on
   the changed files, the unit suite, the changed integration files under each gated profile.
   Never the full integration suites (CI).

Parity first: the library stands in for Redis/Valkey in CI and tests. A memory, throughput or
other production-hardening concern is at most a [nit] unless it makes a test run hang, crash or
become impractically slow; never block a parity fix on it.

Tags: [blocker] wrong behavior or crash; [must-fix] acceptance gap, missing test, wrong gate,
test that would pass on the base; [should-fix] real defect of lower impact; [nit] style
(non-optional only if a project rule says so); [question] when you cannot decide. Only
[blocker] / [must-fix] / [should-fix] / non-optional [nit] count as action required. A finding
outside the lane's scope is not action required: file or amend an issue for it and note the
link in the review.

Submit one GitHub review with inline comments (each with its evidence: probe output, source
lines, a failing command). Use `gh api repos/fatal10110/js-redis-server/pulls/<n>/reviews` with a
JSON body `{"commit_id": "<head>", "event": "COMMENT", "body": "...", "comments": [{"path", "line",
"side": "RIGHT", "body"}]}` (pass it with `--input <file>`). The review body ends with a
**Bottom line**: "clean, ready to merge" or "N items need action".

## Later rounds (delta re-review)

Diff your last reviewed head to the new head. Check that each earlier item is fixed (or rebutted
with concrete evidence; accept a sound rebuttal), review the delta in full and other code only
where the delta reaches, re-run the tests the delta touches. Do not raise nits you could have
raised earlier unless they are real defects.
