---
name: redis-waves
description: >
  Plan and run a multi-PR wave of fatal10110/js-redis-server issues on the Workflow tool: group
  open issues into lanes (one PR each) and waves, then fan out. Each PR gets a Sonnet lane brief
  (issue text, real-server replies per profile, code anchors), an implementation side that
  implements and fixes every review round, CI failure and merge conflict (handing over to a fresh
  agent at context checkpoints), and one long-lived review agent that reviews the first push in
  full, re-reviews only each new delta, and requests the merge. A merger workflow runs the
  cheap-model CI/merge agent; a helper workflow runs the cheap helper agents (redis-researcher,
  ts-scout, runner) that workflow agents cannot spawn themselves. Use when the user says "run the
  waves", "fan out wave 1", "ship these issues in waves", "implement -> review -> merge loop", or
  asks to execute a grouping/waves plan. Merging is authorized by the user's invocation of this
  skill. Do not use for a single issue or for reviewing one PR.
---

# redis-waves

Ported from the acis-waves skill of fatal10110/acis_public. The orchestration is the same; the
prompts, tools and test policy are this repo's.

Runs a waves plan through `waves.js` (next to this file), launched as several Workflows over one
plan:

- **track** (one Workflow per track): works the track's lanes wave by wave. Per lane (one PR):
  0. **Lane brief.** A Sonnet `wave-brief` agent gathers the issue text (with the comments that
     re-scope it), the real replies per profile next to the mock's (`probe.py run --mock`), the
     source lines behind version boundaries, code anchors and test templates into
     `lanes/<slug>/brief.md`, which goes into the implementation agent's prompt, so the expensive
     agent starts coding instead of exploring (`brief: false` turns it off).
  1. **Implementation agent** (session model, or the lane's `implModel`). It implements and opens
     the PR, and the implementation side fixes every review round, CI failure and merge conflict.
     It hands over at checkpoints (right after the first push, and whenever its context has grown
     long): it returns `checkpoint` and a fresh agent continues from the worklog, because cost is
     turns × context.
  2. **Review agent.** It starts at the first push and stays alive until the PR is merged or
     given up. Round 1 is a full review (`references/review.md`). Every later round is a delta
     re-review. When the PR is clean, it asks the merger to merge and routes the result back.

  The two agents talk directly through the lane mailbox. Before the first push, a Haiku watcher
  holds the second slot instead of an idle review agent. The script replaces either agent if it
  dies; a replacement rebuilds its context once, from the worklog or the review files. At the end
  of each wave, every track meets at a file barrier, then the next wave starts.
- **merger** (one Workflow): one Haiku CI/merge agent per merge request, one at a time across all
  tracks. It checks the head is the one the reviewer approved, updates the branch when behind,
  waits for CI (reruns known flakes once), and squash-merges with `gh pr merge --squash
  --match-head-commit`. It never edits code or runs tests. A real CI failure or a conflict goes
  back to the review agent, which sends it to the implementation agent.
- **helper** (one Workflow): serves helper requests from every lane (`wave_mail.py ask`):
  - `redis-researcher` (`wave-researcher`, Sonnet; at most `researcherPerLane` per lane): batched
    real-behavior investigations across versions, from live probes and the servers' C sources;
  - `ts-scout` (`wave-scout`, Haiku): locate TypeScript code;
  - `runner` (`wave-runner`, Haiku): run scoped test or lint commands, return a compressed result.
  A Haiku deliver agent writes each answer back.

Per PR that is one implementation agent and one review agent (session model), one Haiku merge
agent per merge attempt, Haiku watcher, barrier and deliver agents, and any helpers.

All coordination goes through `scripts/agent-tools/wave_mail.py` under
`/home/zorg/workspace/js-redis-server-worktrees/.waves/<run>/` (mailboxes `box/impl` and
`box/review`, lane state, the helper and merge request queues and their answers, reviews, CI
notes, barrier markers). Lane worktrees are `/home/zorg/workspace/js-redis-server-worktrees/wf-<slug>`
(branch `wf/<slug>`) and `review-<slug>` (detached). The run directory survives restarts.

## Agent tools (built into every prompt)

All in `scripts/agent-tools/` of the checkout this skill was loaded from:

- `probe.py run [-p PROFILES] [--resp 3] [--args ...] [--raw] [--mock WORKTREE] -- 'CMD' ...` —
  exact reply bytes from real servers pinned to each profile's version (docker containers
  `wave-probe-*`, started on first use and shared), and with `--mock` the mock's reply from a
  checkout next to each, marked `=` or `≠`. `probe.py up` prints ports for running a standalone
  integration file against a real server (`TEST_BACKEND=real REDIS_COMPAT=<p>
  REDIS_STANDALONE_PORT=<port>`). `probe.py down` stops them.
- `look.py <spec>...` — ranges, TypeScript declarations by name (`ts:PATH:Name,Class.method`),
  bounded rg, and the real servers' sources by tag (`redis@7.2.4:src/t_stream.c:fn:xreadCommand`,
  cached under `.cache/src`); many lookups in one call.
- `heavy.sh <cmd>` — serializes heavy local runs (tsc, eslint, test files) across lanes.
- `wave_mail.py worktree --lane S [--review] [--remove]` — creates (with `npm ci`), moves or removes
  lane worktrees with absolute paths; `pr-state --pr N` and `ci-wait --head SHA` for the merge
  agent.
- STOP reaches agents through commands they run anyway: every `wave_mail.py` output of a stopping
  run carries `"stop": true`; `look.py`, `probe.py` and `heavy.sh` print `### STOP` (heavy.sh also
  refuses to run).

## Test policy (built into every prompt)

- No agent runs the whole integration suites or touches the shared docker-compose stack
  (`clean:redis`, `test:integration:real*`). PR CI runs every suite on every push.
- Agents run `tsc --noEmit`, eslint/prettier on changed files, the unit suite (about 20 s), and the
  integration files the change adds or touches against the mock (per gated profile), all through
  `heavy.sh`.
- Real servers are the oracle: every behavior claim is probed, and every new standalone
  integration test is run against the real server of each profile it covers (`probe.py up`).
  CI's real backend is redis 8.0 only (#522), so this local step is the only check of 6.2-7.4
  and Valkey expectations.
- The merge agent never runs tests; the `runner` helper refuses whole-suite commands.

## Concurrency budget

The Workflow tool caps concurrent agents per workflow at `min(16, CPUs - 2)`. This box has 4
CPUs, so the cap is **2**. A lane needs 2 slots (implementation + review, or + watcher before the
first push), so one track runs **one lane at a time**; parallel PRs come from several track
workflows. On 4 CPU / 7 GB: **2 tracks + merger + helper** (4 Workflows). On a bigger box pass
`slots` and more tracks.

## Step 1: build the plan

Start from a grouping analysis of the live open issues (`gh issue list -R
fatal10110/js-redis-server --state open`, bodies and comments: later comments often re-scope an
issue or say it was fixed). Load only the issues the user put in scope. Rules:

- **Lane** = one PR. Combine issues that touch the same function or file for the same reason,
  share an origin, or would conflict with each other. Keep a lane to one reviewable change.
- **Wave** = lanes that can start together. A dependency on another lane goes to an earlier wave,
  or to the same wave on the **same track** (it then waits for that lane to merge).
- **Track**: lanes that share a hot file (`hot` group) in the same wave go on one track; the
  validator refuses a hot group split across tracks. CHANGELOG.md and feature-gates.ts are touched
  by nearly every lane and resolve mechanically: do not make them hot groups. Balance tracks by
  expected size.
- Rank parity issues (replies, errors, protocol, per-version behavior) first. The library is a
  CI/test stand-in, so production-only concerns (memory retention, throughput, hardening) go
  last or are left out unless they make test runs hang or blow up.
- Leave out issues that need an owner decision, are blocked on another repo (lua-redis-wasm
  engine issues), belong to another session's uncommitted work, or are epics/trackers. Put them in
  `exclusions` where agents must not touch them, and list them for the user.
- A lane may be audit-only (verify done issues, close them with evidence, file follow-ups): say so
  in `note`; the implementation agent returns `no_change` with no PR.

Plan file (`/home/zorg/workspace/js-redis-server-worktrees/.waves/<run>/plan.json`); shape in
`plan.example.json`:

```json
{
  "tracks": ["A", "B"],
  "exclusions": "decision issues #390 #521; engine-blocked #540",
  "env": "optional extra environment rule lines for every agent",
  "waves": [
    {"name": "1", "lanes": [
      {"slug": "script-ctx", "track": "A", "issues": [563, 566, 568],
       "note": "one PR: the Lua call context's own db and park", "hot": ["lua-runtime"], "deps": []}
    ]}
  ]
}
```

`slug` is the branch `wf/<slug>` and worktree `wf-<slug>`; it must be unique across the run.
Optional per lane: `implModel` / `reviewModel` (a model alias such as `sonnet`) and `implEffort` /
`reviewEffort` (low|medium|high|xhigh|max). Use the cheaper model only for test-only, docs-only or
mechanical lanes; behavior lanes and every review stay on the session model.

Validate: `python3 <checkout>/scripts/agent-tools/wave_mail.py validate <plan.json>`.

Show the user the plan (waves, tracks, lanes, exclusions) before launching unless they already
approved a grouping in this conversation.

## Step 2: preflight

- `docker ps` works (the probe needs it) and `gh auth status` is logged in with push rights.
- Pull the probe images once, before the agents need them: `python3 <tool dir>/probe.py up`
  (all seven profiles; about a minute the first time).
- No leftover `wf-<slug>` worktree or `wf/<slug>` branch from another run uses a planned slug
  (`git -C /home/zorg/workspace/js-redis-server worktree list`). If one does, rename the lane or
  keep it on purpose: a fresh lane reuses and inspects an existing worktree.
- No open PR already covers a lane (`gh pr list -R fatal10110/js-redis-server --search '<issue>'`).
- `<tool>` is the absolute path of `scripts/agent-tools/wave_mail.py` in the checkout this skill
  was loaded from. Until this skill is merged, that checkout is the worktree it was written in,
  not `/home/zorg/workspace/js-redis-server`.
- Pick `run`: a short id such as `r1030`. Reusing a run id resumes it (Step 4).
- Cost check: each PR uses one implementation agent and one review agent on the session model,
  plus Haiku merge, watcher and deliver agents and a Sonnet brief. Say so if the user has not
  seen it.

## Step 3: launch

Launch one Workflow per track, one merger and one helper broker, all in the same turn, with the
same `run`, `epoch`, `plan` and `tool`. Pass `scriptPath` as the absolute path of `waves.js`. If
the tool refuses a path outside the session directory, Read the file once and pass its text as
`script`; then reuse the persisted path that result returns.

```text
Workflow({scriptPath: "<skill dir>/waves.js", args: {run, epoch, role: "merger", plan, tool}})
Workflow({scriptPath: "<skill dir>/waves.js", args: {run, epoch, role: "helper", plan, tool}})
Workflow({scriptPath: "<skill dir>/waves.js", args: {run, epoch, role: "track", track: "A", plan, tool}})
Workflow({scriptPath: "<skill dir>/waves.js", args: {run, epoch, role: "track", track: "B", plan, tool}})
```

`plan` is the parsed plan object (a JSON value, not a string). `epoch` names this launch: `"1"` for
the first, and a new value for every relaunch of the whole run. Optional args:

| Arg | Default | Meaning |
| --- | --- | --- |
| `slots` | 2 | the per-workflow agent cap; sets `lanesPerTrack` = floor(slots/2) |
| `lanesPerTrack` | from `slots` | lanes running at once in one track |
| `maxRounds` | 8 | review rounds per PR before the lane ends `unresolved` |
| `reviewCiFixes` | true | a CI fix gets a delta review before the next merge attempt |
| `waitCalls` | 6 | recv calls (~9.5 min each) per first-push watcher agent |
| `takeCalls` | 6 | take polls (~9.5 min each) per broker poller agent before it returns |
| `maxIdleMin` | 240 | minutes without a mailbox message before a lane ends `stalled` |
| `restarts` | 2 | replacements per lane for the review agent, and separately for the implementation side, when an agent dies or exits early (checkpoints do not count) |
| `researcherPerLane` | 2 | redis-researcher requests per lane (impl and review combined) |
| `agentTypes` | `{coord: 'wave-coord', deliver: 'wave-deliver', merge: 'wave-merge', impl: 'wave-impl', review: 'wave-review', runner: 'wave-runner', scout: 'wave-scout', brief: 'wave-brief', researcher: 'wave-researcher'}` | agent type per role; `null` falls back to the default workflow subagent |
| `models` | `{merge, coord, deliver, scout, runner: "haiku", brief, researcher: "sonnet"}` | model overrides per role (`impl`, `review` included, for every lane; a lane's `implModel`/`reviewModel` wins) |
| `retry` | [] | slugs to run again even though saved state says they finished; a retried lane gets a fresh `maxRounds` budget |
| `brief` | true | a `wave-brief` agent writes `lanes/<slug>/brief.md` before a fresh lane's implementation agent starts |
| `maxCheckpoints` | 6 | hand-overs per lane (implementation agent returns `checkpoint`; a fresh agent continues from the worklog) |
| `merge` | true | `false`: no merger workflow; a clean review comments "ready for a human merge" on the PR and ends the lane `merge_blocked` with the approved head (dependents are then skipped) |
| `helpers` | true | `false`: no helper broker; prompts offer no helper agents |
| `repo`, `worktrees`, `wavesRoot` | this machine's paths | the main checkout, the worktree root, and the run-state root |
| `sessionModel` | `Claude Opus 5.5` | the name in the commit trailer of agents on the session model |

The slim agent types live in `.claude/agents/wave-*.md`. A session loads that directory at
start, so in the session that first adds them the roles fall back to the default subagent (the
script notices once per role and logs it); they apply from the next session on.

When the permission system will not run the merger workflow (an auto-mode classifier may refuse
an agent that merges without a human), launch only the tracks with `merge: false` and
`helpers: false`; the PRs come out reviewed and approved, and a human merges them.

## Step 4: monitor, intervene, resume

- Progress: `python3 <tool> status --run <run>` prints one line per lane:
  `slug|status|stage|pr|round`. Stages are `impl`, `review`, `fix`, `merge`, `ci-fix` and
  `conflict`. Each workflow's result lists every lane's status, PR, rounds, history and follow-ups.
- Each lane directory has `brief.md`, the implementation agent's `worklog.md`, `reviews/r<N>.md`,
  `ci/c<N>.md` and the mailboxes.
- Terminal lane statuses: `merged`, `no_change`, `blocked`; `unresolved` (findings left after
  `maxRounds`; the PR stays open); `stalled` (idle for `maxIdleMin`); `merge_blocked` (the
  permission system denied the merge: merge it by hand, then `<tool> state --run R --lane S --set
  status=merged` so later deps pass); `failed`; `skipped` (a dep did not merge); `stopped`.
- Drain (finish what is under way, start nothing new): `<tool> stop --run <run> --drain`.
- Stop everything: `<tool> stop --run <run>` writes `STOP`. Every waiting agent returns, the
  implementation and review agents update their notes and return `stopped`, the brokers end, and
  the tracks stop at their next wait. A merge agent already waiting on CI finishes its merge first.
- **Resume** (after a restart, a stop or a usage limit): delete `<run dir>/STOP` if present, then
  launch the same four Workflows again with the same args and a **new `epoch`**. Never use
  `resumeFromRunId`: the concurrent lanes break its cache. Each track reads the saved lane state,
  skips finished lanes, and restarts active ones with a RESUME note. Put slugs in `retry` to run
  finished lanes again.
- A merger or helper workflow that died can be relaunched alone with the same `epoch`; its first
  poll re-serves the requests the dead instance took but never answered. Relaunch a broker only
  after it has died.
- A track that crashed never reaches the barrier, so the other tracks wait. Relaunch only that
  track with the **same** `epoch`, or stop the run.

## Step 5: wrap up

When all workflows have returned:

- Report per lane: status, PR, merge SHA, rounds, follow-ups filed, and for every lane that did
  not merge, the reason and what is left (open PR, worktree kept).
- Check that each "Closes" issue is closed, and that no `wf-<slug>` worktree remains for a merged
  lane. `probe.py down` stops the probe containers.
- Record the run id, the plan, the outcome and the next backlog in memory.

## Editing waves.js

The script is plain JavaScript run by the Workflow tool. It has no filesystem access and no
`Date.now()`/`Math.random()`. After any change, run the offline simulation and the tool tests:

```bash
node .claude/skills/redis-waves/waves_sim.mjs
python3 -m unittest scripts/agent-tools/test_wave_mail.py scripts/agent-tools/test_agent_tools.py
```

The simulation runs the script with fake agents that talk through an in-memory mailbox, under the
2-slot cap: review, CI and conflict rounds on one implementation and one review agent; `maxRounds`
ending a lane `unresolved` and skipping its dependents; resume from saved state; replacement of
dead agents; serial merges; the helper broker's researcher limit and helper agent types; the
example plan on both tracks; slim agent types with the fallback; STOP and DRAIN; the brief and
checkpoints.
