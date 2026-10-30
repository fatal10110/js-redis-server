export const meta = {
  name: 'redis-waves',
  description:
    'js-redis-server wave runner: per PR one long-lived implementation agent and one long-lived review agent that loop through the mailbox until the PR is clean and merged; merger and helper roles run the cheap-model merge and helper agents',
  whenToUse:
    'Run by the redis-waves skill: one Workflow per track (role track), one merger and one helper broker over the same plan',
  phases: [
    {
      title: 'Implement',
      detail:
        'long-lived implementation agent per PR; fixes every review, CI and conflict round',
    },
    {
      title: 'Review',
      detail:
        'long-lived review agent per PR; delta re-reviews; requests the merge',
    },
    {
      title: 'Merge',
      detail: 'merger: CI watch, branch update, squash merge',
      model: 'haiku',
    },
    {
      title: 'Coordinate',
      detail: 'first-push watch, final delivery, wave barrier, status',
      model: 'haiku',
    },
    {
      title: 'Helpers',
      detail: 'helper broker: redis-researcher, ts-scout, runner',
    },
  ],
}

// ---------------------------------------------------------------------------
// Arguments (see SKILL.md): {run, role: 'track'|'merger'|'helper', track, plan, tool, ...}
// ---------------------------------------------------------------------------
const A = args || {}
const REPO = A.repo || '/home/zorg/workspace/js-redis-server'
const GH = 'fatal10110/js-redis-server'
const WT = A.worktrees || '/home/zorg/workspace/js-redis-server-worktrees'
const RUN = A.run
const ROLE = A.role === 'broker' ? 'helper' : A.role || 'track'
const PLAN = A.plan || {}
const TRACKS = PLAN.tracks || []
const TRACK = A.track
const TOOL = A.tool || REPO + '/scripts/agent-tools/wave_mail.py'
const RUNDIR = (A.wavesRoot || WT + '/.waves') + '/' + RUN
// impl/review: null = the session model; a lane's implModel/reviewModel overrides it for that lane.
const M = Object.assign(
  {
    merge: 'haiku',
    coord: 'haiku',
    deliver: 'haiku',
    scout: 'haiku',
    runner: 'haiku',
    brief: 'sonnet',
    researcher: 'sonnet',
    impl: null,
    review: null,
  },
  A.models || {},
)
// Commit trailer of the model an implementation agent actually runs on (a lane's implModel, else the session model).
const SESSION_MODEL = A.sessionModel || 'Claude Opus 5.5'
const MODEL_NAMES = [
  [/fable/i, 'Claude Fable 5.1'],
  [/opus/i, 'Claude Opus 5.5'],
  [/sonnet/i, 'Claude Sonnet 5.5'],
  [/haiku/i, 'Claude Haiku 5.5'],
]
const coAuthor = model => {
  const hit = model && MODEL_NAMES.find(([re]) => re.test(model))
  return `Co-Authored-By: ${hit ? hit[1] : SESSION_MODEL} <noreply@anthropic.com>`
}
// Slim agent types (.claude/agents/wave-*.md): each role gets only the tools it uses, so no agent
// pays for the session's MCP tools, skill listing or deferred-tool list.
// agentTypes: {role: null} falls back to the default workflow subagent for that role.
const AT = Object.assign(
  {
    coord: 'wave-coord',
    deliver: 'wave-deliver',
    merge: 'wave-merge',
    impl: 'wave-impl',
    review: 'wave-review',
    runner: 'wave-runner',
    scout: 'wave-scout',
    brief: 'wave-brief',
    researcher: 'wave-researcher',
  },
  A.agentTypes || {},
)
// A session loads .claude/agents at start, so a type added later is missing until a restart:
// such a call reruns on the default subagent and the role stops asking for the type.
async function agentAs(role, prompt, opts) {
  if (AT[role]) {
    try {
      return await agent(
        prompt,
        Object.assign({}, opts, { agentType: AT[role] }),
      )
    } catch (e) {
      if (!/agent type .* not found/i.test(String(e))) throw e
      log(
        `agent type ${AT[role]} is not in this session's registry; ${role} agents use the default subagent`,
      )
      AT[role] = null
    }
  }
  return agent(prompt, opts)
}
const CHECKOUT = TOOL.replace(/\/scripts\/agent-tools\/wave_mail\.py$/, '')
const REFS = CHECKOUT + '/.claude/skills/redis-waves/references'
const refDoc = name =>
  `the procedure ${REFS}/${name}.md (Read it once with the Read tool and follow it)`
const SLOTS = A.slots || 2
const LANES = A.lanesPerTrack || Math.max(1, Math.floor(SLOTS / 2))
const MAX_ROUNDS = A.maxRounds || 8
const REVIEW_CI_FIXES = A.reviewCiFixes !== false
const WAIT_CALLS = A.waitCalls || 6
const TAKE_CALLS = A.takeCalls || 6
const MAX_IDLE_MIN = A.maxIdleMin || 240
const RESTARTS = A.restarts === undefined ? 2 : A.restarts
// A lane brief (a cheap scout gathers the lane's issues, real replies, anchors and templates)
// before the implementation agent starts, so the expensive agent does not explore with dozens of lookups.
const BRIEF = A.brief !== false
// An implementation agent returns "checkpoint" to hand over to a fresh agent with an empty context;
// checkpoints are not failures and do not use the restart budget.
const MAX_CHECKPOINTS = A.maxCheckpoints === undefined ? 6 : A.maxCheckpoints
const EPOCH = String(A.epoch || '1')
const RESEARCHER_LIMIT =
  A.researcherPerLane === undefined ? 2 : A.researcherPerLane
const RETRY = A.retry || []
// merge: false — no merger workflow: a clean review ends the lane merge_blocked ("approved, merge
// by hand") instead of asking for a merge. helpers: false — no helper broker: prompts offer none.
const MERGE = A.merge !== false
const USE_HELPERS = A.helpers !== false
const SATISFIED = ['merged', 'no_change']
const DONE_BEFORE = [
  'merged',
  'no_change',
  'blocked',
  'unresolved',
  'stalled',
  'merge_blocked',
  'failed',
  'skipped',
]
// Set once any lane of this track has seen STOP: lanes not started yet then return without starting.
let STOPPING = false
const FINAL = [
  'merged',
  'unresolved',
  'stalled',
  'merge_blocked',
  'failed',
  'stopped',
  'no_change',
  'blocked',
]

if (!RUN) throw new Error('args.run is required')
if (!['track', 'merger', 'helper'].includes(ROLE))
  throw new Error('args.role must be track, merger or helper')
if (ROLE === 'track' && !TRACKS.includes(TRACK))
  throw new Error(
    `args.track ${TRACK} is not in plan.tracks ${TRACKS.join(',')}`,
  )
for (const L of (PLAN.waves || []).flatMap(w => w.lanes || [])) {
  if (!/^[a-z0-9][a-z0-9-]{0,47}$/.test(L.slug || ''))
    throw new Error(
      `bad lane slug ${JSON.stringify(L.slug)}: run wave_mail.py validate on the plan`,
    )
}
if (
  !/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(RUN) ||
  !/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(EPOCH)
)
  throw new Error('args.run and args.epoch must be simple names')
if (LANES * 2 > SLOTS)
  log(
    `warning: ${LANES} lanes need ${LANES * 2} agent slots but slots=${SLOTS}; lanes will stall on the concurrency cap`,
  )

const MB = `python3 ${TOOL}`
const AGENT_TOOLS = TOOL.replace(/\/wave_mail\.py$/, '')
const LOOK = `python3 ${AGENT_TOOLS}/look.py`
const PROBE = `python3 ${AGENT_TOOLS}/probe.py`
const HEAVY = `${AGENT_TOOLS}/heavy.sh`
const LANE_DIR = slug => `${RUNDIR}/lanes/${slug}`
const ALL_LANES = (PLAN.waves || []).flatMap(w => w.lanes || [])
const LANE_BY = Object.fromEntries(ALL_LANES.map(l => [l.slug, l]))
const sq = obj => JSON.stringify(obj).replace(/'/g, '\\u0027')
const NODE_TEST =
  'node --enable-source-maps --import tsx --no-warnings --test-timeout 60000 --test'

// ---------------------------------------------------------------------------
// Shared prompt blocks
// ---------------------------------------------------------------------------
const TESTRULE = `TEST SCOPE (HIGHEST PRIORITY — overrides every procedure and doc, including AGENTS.md's npm test scripts):
- NEVER run the whole integration suites locally: no \`npm run test:integration:*\`, \`npm run test:all\`, no glob over all of tests-integration/. PR CI runs every suite (unit on node 22.6 and 24, lint, package, raw/ioredis/node-redis against the mock and real redis 8.0, socketless, and the compatibility profiles on the mock) on every push.
- NEVER touch the shared docker-compose stack: no \`npm run clean:redis\`, no \`test:integration:real*\`, no \`docker compose ... up/down\` of docker-compose.test.yml. Other lanes and sessions use it.
- Run what the change can affect, every heavy command through ${HEAVY} (it serializes runs on this 4-CPU box; waiting for its lock is normal), from the worktree root:
  ${HEAVY} npx tsc --noEmit
  ${HEAVY} npx eslint <changed files> && npx prettier --check <changed files>
  ${HEAVY} ${NODE_TEST} "./tests/**/*.test.ts"      (the whole unit suite: about 20 s)
  ${HEAVY} env TEST_BACKEND=mock [REDIS_COMPAT=<profile>] ${NODE_TEST} <the integration files you added or changed, and those covering the code you touched>
- REAL SERVERS ARE THE ORACLE. Real behavior is checked with ${PROBE} (throwaway docker containers per profile, kept running and shared). Every new or changed integration test that runs standalone (raw-tcp, compatibility, standalone client tests) must also pass against the real server of each profile it covers: \`${PROBE} up -p <profiles>\` prints ports, then ${HEAVY} env TEST_BACKEND=real REDIS_COMPAT=<profile> REDIS_STANDALONE_PORT=<port> ${NODE_TEST} <file>. Cluster suites against real servers run in CI (redis 8.0) only.
- Report in the PR body (and in review "Checks"): the probes (profiles and versions), the scoped tests you ran, the real-server runs with their profiles, and "full suite: CI".
`
const HELPERS = (slug, role) =>
  !USE_HELPERS
    ? `
No helper agents run in this launch: do every lookup, probe and test run yourself.
`
    : `
Cheap helper agents: you cannot spawn subagents, so a broker workflow runs them for you on cheaper models. One Bash call (timeout 600000):
  ${MB} ask --run ${RUN} --from ${slug}/${role} --type <type> --worktree <your worktree or ''> --question - <<'EOF_ASK'
  <self-contained question: what you need, the leads you already have (paths, symbols, commands), the answer shape you want>
  EOF_ASK
Types:
- redis-researcher: one batched investigation of real Redis/Valkey behavior across versions (probe sweeps of many commands and profiles, the source lines that produce each reply, the release that changed it). At most ${RESEARCHER_LIMIT} per lane: put the whole question in one request. A single probe or source lookup is cheaper inline.
- ts-scout: locate TypeScript code in a given worktree (read-only).
- runner: run the commands you list, in the given worktree, through heavy.sh, and return a compressed result (pass/fail, failing tests, first error lines with file:line). Use it for long test runs whose raw output you do not need.
The call prints {"answer": ...}, or {"pending": true, "id": ...} (keep waiting with \`${MB} answer-wait --run ${RUN} --id <id> --wait 570\`, or work on and check later with --wait 0), or {"error": ...} (broker down: do it inline), or {"stop": true} (the run is stopping: wrap up as your stop rule says). Add --nowait to fire a request and keep working. Answer inline when one probe, one source lookup or a targeted read settles it. Verify a helper's evidence before relying on it.
`
const ENV = `
Environment rules (all agents):
- Main checkout: ${REPO} (GitHub ${GH}, default branch main). Never write there; edits happen only in linked worktrees under ${WT}/, based on origin/main. Always use absolute paths; your inherited cwd may be an unrelated worktree: ignore it.
- Working directory: start every shell call in your worktree with \`cd <worktree> &&\` (the worktree ROOT) and write every path root-relative (src/..., tests/..., tests-integration/...), or use \`git -C <worktree>\` / \`--root <worktree>\`. Agent tools are always called by the absolute paths given in this prompt.
- Never guess a path. When you do not know a file's exact location, find it in the same call before using it (\`rg --files src tests tests-integration -g '<name>*'\`, or ${LOOK} rgl:...).
- Path map: commands src/commands/<type>.ts (+ geo/, lists/, streams/, zsets/), registered in src/commands/index.ts; pipeline src/core/ (command-executor.ts, command-schema.ts, client-session.ts, lua-runtime.ts, redis-error.ts, resp-encoder.ts, execution-policies/, transports/resp2/); gates and profiles src/core/compatibility/ (feature-gates.ts, profile.ts, command-table-data.ts, subcommand-gates.ts); state src/state/; client mocks src/client-mocks/; unit tests tests/; integration tests tests-integration/{raw-tcp,compatibility,ioredis,node-redis}/ (helpers tests-integration/utils.ts, raw-tcp/helpers.ts, raw-tcp/raw-connection.ts, test-config.ts); socketless known gaps tests-integration/socketless/known-gaps.ts; docs docs/{ARCHITECTURE,COMMANDS,TESTING,TEST-INTEGRATION}.md; CHANGELOG.md. Project rules: AGENTS.md (read the sections your change touches).
- Real servers: \`${PROBE} run [-p redis-6.2,redis-8.0|redis|valkey|all] [--resp 3] [--args '--requirepass pw'] [--raw] -- 'CMD a b' 'CMD ...'\` prints the exact reply bytes per profile (pinned versions: redis 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0; valkey 8.0.0, 9.0.0; also valkey-7.2 / valkey-8.1, and any patch as e.g. redis-7.2@7.2.16). Use {u} in key names for a per-call unique prefix. Add --mock <your worktree> to start the mock from your checkout per profile and mark every reply "= mock" or "≠ mock: <bytes>": the fastest way to see what is still wrong. Batch every command and profile you need into one call.
- Server sources by tag (cached): ${LOOK} redis@7.2.4:src/t_stream.c:fn:xreadCommand  valkey@9.0.0:src/networking.c:rg:PATTERN  redis@8.0.0:src/server.c:4100-4180 .
- GitHub: the gh CLI, always with -R ${GH} (or repos/${GH} in gh api), Bash timeout 300000, up to two retries on HTTP errors. Issues: gh issue view <n> --comments; search: gh issue list --search '<terms>' --state open; PRs: gh pr view/diff/checks/create/edit; inline reviews and thread replies: gh api (see the procedures).
- Cost model: every tool call re-sends your WHOLE context (often 100k+ tokens), so the number of calls matters far more than their size. Put every lookup you need right now into ONE call, and never spend a call on something another call can carry.
- Batched lookups: ${LOOK} <spec>... (run from the worktree, or pass --root <worktree>) does several in one call with capped output: FILE:A-B | ts:PATH:Name,Class.method (TypeScript declarations with doc, PATH a file or directory) | rg:PATH:PATTERN | rgl:PATH:PATTERN (files + counts) | rgg:GLOB:PATH:PATTERN | the server-source specs above.
  Example: ${LOOK} ts:src/core:ClientSession.close,createLuaCallContext rgl:src/commands:blockOnKeys src/core/turn-queue.ts:1-40
- Output caps (every output stays in your context and is re-sent on every later turn): rg with \`-m 5\` and \`| head -60\`; git diff of named files or \`--stat\` first; test output \`| tail -40\`; never print a generated file (command-table-data.ts) or a whole test file; reads of at most 80 lines; never re-read a range you already have.
- Batching: before you look anything up, list every lookup the next step needs and fetch them all in ONE call. Two single-lookup calls in a row is a mistake; so is a lookup the lane brief already answers.
- Semantic search when you do not know where something lives: \`cd <worktree> && grepai search "<intent>" --json --compact --limit 5\` (the index reflects the main checkout, not your edits); on a grepai error fall back to rg without retrying.
- Editing: the Edit tool needs a Read of that file first (a look/sed view does not count): Read the range, then Edit. For several mechanical edits at once a python heredoc in one call is fine.
- Waiting: never use sleep loops; wait only through the ${MB} commands named in your instructions, each in one Bash call with timeout 600000.
- Hot shared files: CHANGELOG.md [Unreleased] and src/core/compatibility/feature-gates.ts are touched by most lanes. Add your entries at the end of the relevant list; a conflict there is mechanical (keep both sides) and not semantic.
- Where an issue offers several options, choose the one matching real Redis, else the conservative one, stated in a "Decisions" section of the PR body.
- Merged PRs close their issues: one "Closes #n" line per fully resolved issue, "Part of #n" for partial ones.
- Commit messages end with a blank line then the Co-Authored-By trailer your lane instructions name (it names the model you run on).
- PR bodies end with: 🤖 Generated with [Claude Code](https://claude.com/claude-code)
- Never put model names or IDs in commits, PR text or code (the trailer is the only exception).
- Never use git stash (the stash is shared by every worktree), never rebase a pushed branch, never force-push.
- Put new tests in new or small test files to avoid conflicts between lanes.
${PLAN.exclusions ? '- Never touch (other sessions / owner decisions): ' + PLAN.exclusions + '\n' : ''}${PLAN.env ? PLAN.env + '\n' : ''}- Other lanes run concurrently or later; stay within your lane and do not implement what other lanes own: ${ALL_LANES.map(l => l.slug + '=' + l.issues.join('/')).join('; ')}.
`
const issuesText = L => L.issues.map(i => '#' + i).join(', ')

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------
const IMPL_SCHEMA = {
  type: 'object',
  properties: {
    status: {
      type: 'string',
      enum: [
        'merged',
        'no_change',
        'blocked',
        'unresolved',
        'stalled',
        'merge_blocked',
        'failed',
        'stopped',
        'idle_exit',
        'checkpoint',
      ],
    },
    pr_number: { type: 'integer' },
    closes: { type: 'array', items: { type: 'integer' } },
    summary: { type: 'string' },
    followups_filed: { type: 'array', items: { type: 'string' } },
  },
  required: ['status', 'summary'],
}
const REVIEWER_SCHEMA = {
  type: 'object',
  properties: {
    status: {
      type: 'string',
      enum: [
        'merged',
        'unresolved',
        'stalled',
        'merge_blocked',
        'failed',
        'stopped',
        'idle_exit',
        'no_change',
        'blocked',
      ],
    },
    pr_number: { type: 'integer' },
    rounds: { type: 'integer' },
    merge_sha: { type: 'string' },
    summary: {
      type: 'string',
      description:
        'one line per round: verdict and item count; merge attempts and outcomes',
    },
  },
  required: ['status', 'summary'],
}
const WAIT_SCHEMA = {
  type: 'object',
  properties: {
    messages: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          seq: { type: 'integer' },
          kind: { type: 'string' },
          data: {
            type: 'string',
            description: 'the message data object, as a JSON string',
          },
        },
        required: ['seq', 'kind', 'data'],
      },
    },
    timeout: { type: 'boolean' },
    stop: { type: 'boolean' },
  },
  required: ['messages', 'timeout'],
}
const MERGE_SCHEMA = {
  type: 'object',
  properties: {
    status: {
      type: 'string',
      enum: [
        'merged',
        'conflict',
        'ci_failed',
        'head_moved',
        'merge_blocked',
        'retry',
      ],
    },
    answered: {
      type: 'boolean',
      description: 'true once the answer file was written',
    },
    merge_sha: { type: 'string' },
    notes: { type: 'string' },
  },
  required: ['status', 'answered', 'notes'],
}
const LINES_SCHEMA = {
  type: 'object',
  properties: {
    passed: { type: 'boolean' },
    stop: { type: 'boolean' },
    lines: { type: 'array', items: { type: 'string' } },
  },
  required: ['lines'],
}
const TAKE_SCHEMA = {
  type: 'object',
  properties: {
    events: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          id: { type: 'string' },
          type: { type: 'string' },
          from: { type: 'string' },
        },
        required: ['id', 'type', 'from'],
      },
    },
    timeout: { type: 'boolean' },
    stop: { type: 'boolean' },
  },
  required: ['events', 'stop'],
}

// ---------------------------------------------------------------------------
// Prompts: the two long-lived agents of a lane
// ---------------------------------------------------------------------------
function implPrompt(L, resume, brief, canCheckpoint) {
  const D = LANE_DIR(L.slug)
  const W = `${WT}/wf-${L.slug}`
  const checkpointRules = canCheckpoint
    ? `CHECKPOINTS (your cost is turns x context, so hand over instead of growing):
- Right after you send the first ready (reason initial): bring ${D}/worklog.md fully up to date (state, PR number, decisions and why, files changed, the exact test and probe commands, open questions) and return status checkpoint. A fresh agent handles the review, CI and conflict rounds from the worklog.
- Any other time your conversation has grown long (about 60 tool calls since you started, or a context warning) and you are at a clean point (edits saved, worklog listing the exact remaining steps), or you are about to wait on recv after a long round: update the worklog and return status checkpoint. Never checkpoint in the middle of an edit, with uncommitted work the worklog does not describe, or with a message read but not handled.`
    : `CHECKPOINTS: none are left for this lane. Never return status checkpoint; keep working until the final message or another return the protocol names.`
  return `${TESTRULE}
You are the long-lived implementation agent for lane "${L.slug}" (wave ${L.wave}, track ${TRACK}) of ${GH}: issues ${issuesText(L)}.
${L.note || ''}

You own this PR's code: you implement it and fix every review round, CI failure and merge conflict, handing over to a fresh implementation agent at the CHECKPOINTS below so no agent carries a huge context. One long-lived review agent reviews every push and routes merge results back through the lane mailbox. You never merge.
${brief ? `\nLANE BRIEF (a scout prepared it from origin/main; it is also in ${D}/brief.md). Rely on it instead of repeating its searches and probes; verify only what you change or what looks wrong:\n${brief}\n` : resume ? `\nThe lane brief is in ${D}/brief.md: Read it once if the lane is still at stage impl; review, CI and conflict rounds rarely need it.\n` : ''}

PHASE 1 — implement. Follow ${refDoc('implement')}: ${
    brief
      ? 'the LANE BRIEF above already holds the issue text, the real replies and the code anchors, so start from it: read live only what it lacks or marks open (issue comments newer than the brief included), and probe only what it does not cover'
      : 'read each issue live (gh issue view --comments), verify each gap still exists on origin/main, and pin the real behavior per profile with the probe'
  } (through the helpers below where they fit), implement the smallest complete change for this lane test-first, run the scoped gates, then open ONE PR, ready for review (not draft), with "Closes #n" per fully resolved issue and the verification evidence the procedure requires.
- Worktree: ${MB} worktree --run ${RUN} --lane ${L.slug} (one call: creates or reuses ${W} on branch wf/${L.slug} from origin/main, installs node_modules, records it in the lane state, and prints head, dirty files and how far it is behind main). Push with git -C ${W} push -u origin wf/${L.slug}.
${(L.deps || []).length ? `- This lane builds on lane(s) ${L.deps.join(', ')}, already merged to origin/main: read their merged PRs and reuse what they added.` : ''}
- An issue already fixed on main or stale: no code for it; comment with evidence (main SHA, file:line, merging PR), close it (gh issue close --reason completed), leave it out of "Closes".
- Keep ${D}/worklog.md current: decisions and why, probe evidence per profile, what each round changed, open questions. A replacement agent resumes from it if you stop.
- STOP: once the run is stopping, every ${MB} call prints "stop": true, ${LOOK} and the probe print "### STOP", and heavy.sh prints "### STOP" and refuses to run. When you see any of them: update the worklog (where you are, what is left), push nothing, and return status stopped. Never spend a call of its own checking for it.

MAILBOX (each command in its own Bash call, timeout 600000):
- To the review agent: ${MB} send --run ${RUN} --lane ${L.slug} --box review --kind <kind> --data '<json object>' [--set key=value ...]
  * ready {"reason":"initial|review-fix|ci-fix|conflict","pr":<n>,"head":"<pushed sha>","semantic":<bool>} after every push that should move on; add --set stage=review --set pr=<n> --set head=<sha>.
  * no_change {"summary":"..."} with --set status=no_change, or blocked {"reason":"..."} with --set status=blocked (after commenting the blocker on the issue): only before a PR exists; then return at once.
- Wait for the review agent: ${MB} recv --run ${RUN} --lane ${L.slug} --box impl --wait 570 ; run it again while it prints "timeout": true. Before working on a review, ci or conflict message, run git -C ${W} pull --no-rebase origin wf/${L.slug} : the merge agent may have pushed a branch-update merge commit, and your push must stay a fast-forward (not for final: a merged branch is already deleted). Messages:
  * review {"round":N,"file":"..."}: read the file (and the PR's unresolved threads), then follow ${refDoc('review-comments')}: verify each item against the code and real servers, fix every confirmed in-scope finding with tests, reply on every thread with evidence (fixed-in SHA, or rebuttal with probe/source evidence), resolve fixed threads, file follow-ups for valid out-of-scope items, push (never force-push), send ready reason review-fix. You already know the code: re-read only what the findings point at.
  * ci {"file":"...","head":"..."}: CI failed for a real reason; read the file, fix, scoped-verify, push, send ready reason ci-fix.
  * conflict {"notes":"..."}: the branch cannot be updated automatically. git -C ${W} fetch origin; merge origin/main into your branch (never rebase, never force-push); resolve (CHANGELOG.md and feature-gates.ts conflicts: keep both sides); npx tsc --noEmit, the unit suite and the scoped tests for what the resolution touched; push; send ready reason conflict with "semantic": true when the resolution changed the PR's logic, else false.
  * final {"reason":"merged|unresolved|stalled|merge_blocked|failed|stopped"}: if merged, run ${MB} worktree --run ${RUN} --lane ${L.slug} --remove (worktree and local branch); otherwise keep both. Update the worklog and return your result with status = the final reason.
  * A message identical to one you already handled (same kind and round/file): ignore it.
  * recv printing "stop": true: the run is stopping; update the worklog and return status stopped.
- If recv times out for ${MAX_IDLE_MIN} consecutive minutes (since the last message you received), update the worklog and return status idle_exit.
${checkpointRules}
- Procedures: Read each one once per agent and keep what you need in your notes; never re-read or \`cat\` it again.
${HELPERS(L.slug, 'impl')}
${ENV}- Your commit trailer: ${coAuthor(L.implModel || M.impl)}
${resume ? '\nRESUME: ' + resume : ''}
Return: status, pr_number, closes, summary (what changed, key files, decisions), followups_filed.`
}

function implResume(L, why) {
  const D = LANE_DIR(L.slug)
  return `${why} First run ${MB} state --run ${RUN} --lane ${L.slug} --set status=active ; the state it prints (stage, pr, round) is authoritative. Rebuild your context from ${D}/worklog.md, the PR if there is one (gh pr view <n> -R ${GH} --comments, and its unresolved review threads; for the code, \`git -C ${WT}/wf-${L.slug} diff --stat origin/main...HEAD\`, then only the hunks a review item or the worklog points at; never the whole PR diff, which can carry generated files), ${D}/reviews/ and ${D}/ci/. Reuse worktree ${WT}/wf-${L.slug} (recreate it from origin/wf/${L.slug} if missing). Then by stage:
- impl: finish the implementation and continue with the protocol.
- review or merge: the review agent is working; go straight to recv.
- fix: the newest review in ${D}/reviews/ may be partly addressed; compare what is pushed with what was asked, finish it, push, send ready reason review-fix.
- ci-fix or conflict: the triggering message may already be consumed; read the newest file in ${D}/ci/ or merge origin/main as the protocol says, fix, push, send ready with the matching reason.`
}

function briefPrompt(L) {
  const D = LANE_DIR(L.slug)
  const W = `${WT}/wf-${L.slug}`
  return `You prepare the LANE BRIEF for lane "${L.slug}" of ${GH} (issues ${issuesText(L)}). An expensive implementation agent starts right after you with your brief in its prompt; every fact you hand it saves it a lookup. Gather facts; do not design the change, do not write code, do not run tests.
Lane note: ${L.note || '(none)'}
Steps (batch: several lookups per Bash call):
1. ${MB} worktree --run ${RUN} --lane ${L.slug}   (creates ${W} on origin/main; all repo paths below are relative to it).
2. Issues: gh issue view <n> -R ${GH} --comments for each lane issue (timeout 300000). Later comments re-scope the body ("still open", "fixed by #..."): record what is actually still open. Check every issue the text names as a blocker or prerequisite with gh issue view <b> -R ${GH} --json state,title.
3. Real behavior: ${PROBE} run with the exact commands from the issues (and their obvious edge cases: RESP3, inside MULTI, from EVAL via redis.pcall, wrong type, missing key) on the profiles the lane note or issues name (default all). Copy the reply bytes per profile. Where versions differ, find the source lines at the tags with ${LOOK} redis@<tag>:src/<file>.c:fn:<function> (or valkey@<tag>) and name the first release with the new behavior.
4. Mock behavior: run the same probe again with --mock ${W} (it starts the mock from the worktree per profile and marks every reply "= mock" or "≠ mock: <bytes>"); in practice fold steps 3 and 4 into one call with --mock.
5. Code anchors (worktree-relative, with line numbers): the command definitions, policies, state methods, gates (src/core/compatibility/feature-gates.ts) and error factories (src/core/redis-error.ts) the issues point at or the behavior runs through (${LOOK} ts:... and rg); the existing tests that cover them and the test file to copy for each tier (tests/, tests-integration/raw-tcp/, tests-integration/compatibility/, ioredis/, node-redis/); entries in tests-integration/socketless/known-gaps.ts that mention these commands; recent merged PRs touching the same files (gh pr list -R ${GH} --state merged --search '<file or command>' --limit 5) as templates.
6. Commands: the scoped test commands for this lane (from the TEST SCOPE rules below, with the exact file paths), and the profiles to run against real servers.
Write the brief as markdown, at most 250 lines: sections BLOCKER (only if any), Issues (what is still open, acceptance, decisions already made in comments), Real behavior (per profile, exact bytes, version boundaries with source refs), Mock today, Code anchors, Tests and commands, Open questions. Paths with line numbers; short excerpts (at most 15 lines) only where the exact text matters. Save it with one Bash call: mkdir -p ${D} && cat > ${D}/brief.md <<'EOF_BRIEF' ... EOF_BRIEF . Your final message is the brief itself, verbatim.
STOP: once the run is stopping (or draining), ${MB} calls print "stop": true and ${LOOK} prints "### STOP". When you see either, do nothing more and make your final message exactly STOP.
${TESTRULE}${ENV}`
}

function reviewerPrompt(L, resume, roundLimit) {
  const D = LANE_DIR(L.slug)
  const RW = `${WT}/review-${L.slug}`
  return `${TESTRULE}
You are the long-lived review agent for lane "${L.slug}" (issues ${issuesText(L)}) of ${GH}. You stay alive for the PR's whole life: you review the first push in full, then wait for the implementation agent's fixes and re-review ONLY what changed, request the merge when the PR is clean, and route CI failures and conflicts back to the implementation agent. You keep your context between rounds: never re-read a file, issue, thread or source you already read unless the new delta touches it.
${L.note || ''}
Review worktree: ${RW}, detached. At the start of every round run ${MB} worktree --run ${RUN} --lane ${L.slug} --review in a Bash call of its own (creates it the first time, with node_modules, then moves it to the current origin/wf/${L.slug}, and prints the head). Remove it when you return: the same command with --remove.
Cost rules: Read the review procedure ONCE (round 1) with the Read tool and keep its checklist in your notes; never re-read or \`cat\` it again. Review from the diff: \`git -C ${RW} diff --stat <base>..<head>\`, then the hunks of the files you review, then surrounding code in ranges of at most 80 lines. Do not print whole files.

MAILBOX (each command in its own Bash call, timeout 600000):
- Wait for the implementation agent: ${MB} recv --run ${RUN} --lane ${L.slug} --box review --wait 570 ; run it again while it prints "timeout": true. You get ready {"reason","pr","head","semantic"}, or rarely no_change/blocked (then return that status).
- To the implementation agent: ${MB} send --run ${RUN} --lane ${L.slug} --box impl --kind <review|ci|conflict|final> --data '<json object>' [--set key=value ...]
${MERGE ? '' : `- NO MERGES in this launch: a human merges the PRs. Step 4 below ends the lane instead of asking for a merge.\n`}- Merge requests go to the merge agent (cheap model) through the merger broker:
  ${MB} ask --run ${RUN} --from ${L.slug}/review --type merge --question '{"pr":<n>,"head":"<sha>"}' --wait 570
  The merge takes a while (CI): while it prints "pending": true, run ${MB} answer-wait --run ${RUN} --id <id> --wait 570 again, always with the same id (a pending request is still being served; never ask again while one is pending). The answer is a JSON object {"status":"merged|conflict|ci_failed|head_moved|merge_blocked|retry","merge_sha","file","head","notes"}. "stop": true: return status stopped. An "error" (the merge broker is not running): ${MB} pause --secs 300, then ask again; after 6 such errors, send final {"reason":"failed"} --set status=failed and return status failed.

LOOP:
1. Round 1 (the first ready): follow ${refDoc('review')} end to end: check against real servers (probe) and the acceptance criteria of every issue the PR closes or is part of; run the PR's new/changed tests in the review worktree, against the mock and against real servers of the profiles they cover; submit one GitHub review with inline comments; decide the Bottom line. Only [blocker]/[must-fix]/[should-fix]/non-optional [nit] items count as action required.
2. Every round N: write ${D}/reviews/r<N>.md (verdict, the reviewed head sha, the numbered action list '[tag] path:line — what to do' or "none", and for N > 1 the status of each earlier item), then ${MB} state --run ${RUN} --lane ${L.slug} --set round=<N> .
   - Action required and N < ${roundLimit}: send review {"round":N,"file":"${D}/reviews/r<N>.md"} --set stage=fix, then wait.
   - Action required and N >= ${roundLimit}: send final {"reason":"unresolved"} --set status=unresolved and return status unresolved.
   - Clean: go to step 4.
3. On ready (round N+1), by reason:
   - review-fix${REVIEW_CI_FIXES ? ', ci-fix' : ''}, or conflict with "semantic": true: a DELTA re-review. Diff your last reviewed head to the new head in the review worktree; verify each earlier item is fixed; review the delta in full and other code only where the delta reaches. Read the author's thread replies; do not re-raise a finding rebutted with concrete probe/source evidence unless you can show the rebuttal wrong with better evidence, and do not add nits you could have raised earlier unless they are real defects. Submit the re-review on GitHub, then step 2.
   - ${REVIEW_CI_FIXES ? '' : 'ci-fix, or '}conflict with "semantic": false: no review; go to step 4.
${MERGE ? '' : `4. Approved (no merges in this launch): comment on the PR (gh pr comment <n> -R ${GH} --body '<body>') that the review loop is clean at <head> and it is ready for a human merge, with one line per round; then send final {"reason":"merge_blocked","note":"approved at <head>; merge by hand"} --set status=merge_blocked --set approved_head=<head> and return status merge_blocked with that note in the summary. Ignore the merge rules below.\n`}4. Merge: ${MB} state --run ${RUN} --lane ${L.slug} --set stage=merge , then ask for the merge with head = the head of the latest ready you accepted (the head you reviewed, or for a ready you took to merge without review, the head it names), and act on the answer:
   - merged: send final {"reason":"merged","merge_sha":"<sha>"} --set status=merged; return status merged.
   - ci_failed: send ci {"file":"<file>","head":"<head>"} --set stage=ci-fix; wait (step 3).
   - conflict: send conflict {"notes":"<notes>"} --set stage=conflict; wait (step 3).
   - head_moved: the branch changed without a ready (an unexpected push). Fetch it and do a delta re-review of the new head as the next round (step 3 rules, then step 2).
   - merge_blocked: send final {"reason":"merge_blocked"} --set status=merge_blocked; return status merge_blocked.
   - retry: ask again; after 6 merge requests in total, send final {"reason":"failed"} --set status=failed and return status failed.
- If recv times out for ${MAX_IDLE_MIN} consecutive minutes (since the last message you received): send final {"reason":"stalled"} --set status=stalled; return status stalled.
- recv or answer-wait printing "stop": true: return status stopped (no final message).
${HELPERS(L.slug, 'review')}
${ENV}
${resume ? '\nRESUME: ' + resume : ''}
Return: status, pr_number, rounds, merge_sha, summary.`
}

function reviewerResume(L, why) {
  const D = LANE_DIR(L.slug)
  return `${why} You have no memory of earlier rounds: rebuild it once. First run ${MB} state --run ${RUN} --lane ${L.slug} ; the state it prints (stage, pr, round) is authoritative. Read every review in ${D}/reviews/ and the PR (gh pr view <n> -R ${GH} --comments, and gh api repos/${GH}/pulls/<n>/comments). Your next round number is the highest r<N>.md number + 1, and the last reviewed head is the one in the newest review file. Then by stage:
- review (or impl with a PR open): a ready message may already be consumed. If the PR head differs from your last reviewed head (or there is no review yet), review it now (round 1 in full if there is none, otherwise a delta re-review); else wait.
- fix, ci-fix or conflict: wait for the implementation agent's ready.
- merge: check the PR first. MERGED: send final {"reason":"merged","merge_sha":"<sha>"} --set status=merged and return status merged. Otherwise look for an earlier merge request of this lane still pending (ls ${RUNDIR}/requests/taken/ | rg '^${L.slug}-review-' , newest first, with no matching ${RUNDIR}/answers/<id>.md): if one exists, keep waiting on it with answer-wait; else ask for the merge again.`
}

// ---------------------------------------------------------------------------
// Prompts: small coordination agents
// ---------------------------------------------------------------------------
function watchPrompt(L, drain) {
  return `You are a mailbox watcher. Run only ${drain ? 'these commands, each' : 'this command'} in one Bash call with timeout 600000, copied exactly; do nothing else:
${
  drain
    ? `1. ${MB} drain --run ${RUN} --lane ${L.slug} --box review --kinds no_change,blocked   (discards answers left from an earlier run)
2. `
    : ''
}${MB} recv --run ${RUN} --lane ${L.slug} --box review --peek --wait 570
While its output has "timeout": true and no "stop", run it again: at most ${WAIT_CALLS} recv runs in total.
Return the last output: messages (seq, kind, and data re-encoded as a JSON string), timeout, stop.`
}

function deliverFinalPrompt(L, data) {
  return `Run only this command in one Bash call with timeout 600000, copied exactly, and do nothing else; then return messages [], timeout true:
${MB} send --run ${RUN} --lane ${L.slug} --box impl --kind final --data '${sq(data)}' --set status=${data.reason}`
}

function statusPrompt() {
  return `Run exactly this Bash command (timeout 600000) and return its "lines" array verbatim: ${MB} status --run ${RUN}`
}

function barrierPrompt(wi, finish) {
  return `You are a wave barrier. Run only these commands, each in its own Bash call with timeout 600000, copied exactly:
1. ${MB} barrier --run ${RUN} --epoch ${EPOCH} --wave ${wi} --track ${TRACK} --tracks ${TRACKS.join(',')} --wait 570
   While it prints "passed": false without "stop", run it again: at most 30 runs.
${finish ? `2. If it passed: ${MB} finish-track --run ${RUN} --epoch ${EPOCH} --track ${TRACK}\n` : ''}Return passed, stop and lines from the last barrier output (lines verbatim; empty if absent).`
}

// One take agent polls up to TAKE_CALLS times (~9.5 min each) before it returns, so an idle
// broker pays one agent's context per ~TAKE_CALLS × 9.5 min instead of one per poll.
function takePrompt(broker, first) {
  const cmd = `${MB} take --run ${RUN} --broker ${broker} --epoch ${EPOCH} --tracks ${TRACKS.join(',')} --wait 570`
  return `You are a request poller. Run only this Bash command (timeout 600000), copied exactly:
${cmd}${first ? '\nOn the FIRST run only, append " --requeue" to it.' : ''}
While it prints "events": [] with "timeout": true and no "stop": true, run it again: at most ${TAKE_CALLS} runs in total. Do nothing else between runs.
Return the events (id, type, from only), timeout and stop fields of the last run.`
}

function helperPrompt(ev) {
  const file = `${RUNDIR}/requests/taken/${ev.id}.json`
  const body = {
    'redis-researcher': `Answer it with live probes (${PROBE} run ...: batch every command and profile into as few calls as possible; use {u} in key names) and the servers' sources (${LOOK} redis@<tag>:src/<file>.c:fn:<name> / rg: / ranges; valkey@<tag>:...). Report per version the exact reply bytes, the source lines behind them, and the first release with each behavior; mark what is probed versus read from source only. Never edit anything.`,
    'ts-scout': `Locate the TypeScript code the question asks about, in the worktree it names (read-only; ${LOOK} ts:/rg:/rgl: specs batch several lookups in one call). Report file:line locations with one line each on what is there; no file dumps.`,
    runner: `Run the commands the question lists, in the worktree it names; tests, tsc and eslint always through ${HEAVY}. Refuse whole-suite integration commands (npm run test:integration:*, test:all, a glob over all of tests-integration/) and anything touching the shared docker-compose stack (clean:redis, test:integration:real*): answer that CI runs them and ask for the files. Do not edit, commit or push anything. Report per command: pass/fail, the failing test names, and the first error lines with file:line (at most 60 lines in total).`,
  }[ev.type]
  return `Helper request ${ev.id} from ${ev.from}. Read the request file ${file} with the Read tool; its "question" field is your task and its "worktree" field is the requester's worktree. ${body}
Your final message is delivered verbatim to the requester: lead with the answer, cite evidence (probe output, file:line, source refs), say what is confirmed versus inferred.`
}

function deliverPrompt(id, text) {
  const path = `${RUNDIR}/answers/${id}.md`
  return `Deliver a helper answer. Use the Write tool to create ${path}.tmp with exactly the text between the markers (not the markers), then run (Bash) mv ${path}.tmp ${path} . Do nothing else.
<<<ANSWER
${text}
ANSWER>>>`
}

function mergePrompt(ev, L) {
  const file = `${RUNDIR}/requests/taken/${ev.id}.json`
  const slug = L.slug
  const D = LANE_DIR(slug)
  return `You are the CI and merge agent for lane "${slug}" (branch wf/${slug}, issues ${issuesText(L)}) in ${GH}. Read the request file ${file} with the Read tool: its "question" field is a JSON object {"pr": <number>, "head": "<sha the reviewer approved>"}. The PR passed review with no action required, and the user explicitly authorized merging PRs that passed this review loop. Be mechanical: you never edit code, never resolve conflicts and never run tests locally (CI runs every suite).
Every Bash call to ${MB} uses timeout 600000; every gh call uses -R ${GH} (or repos/${GH}) and timeout 300000. You answer exactly once, at the end, with:
  ${MB} answer --run ${RUN} --id ${ev.id} --text '<json object>'
where the object is {"status":"merged|conflict|ci_failed|head_moved|merge_blocked|retry","merge_sha":"","file":"","head":"","notes":""} (no single quotes inside).
PR state and CI come from two compact commands (use them instead of polling gh pr view/checks):
  ${MB} pr-state --pr <n>   -> {state, merged, merge_sha, draft, head, mergeable_state, behind_by}
  ${MB} ci-wait --run ${RUN} --broker merge --head <sha> --wait 570   -> waits in-process; {state: success|failure|pending|error, failed: [{name, conclusion, run_id, job_id}], pending, missing}
1. Head check: ${MB} pr-state --pr <n>. If the PR is already MERGED (an earlier merge agent merged it, then stopped before answering), do step 5's checks and answer merged with its merge SHA. If the PR head is the requested head, continue. Otherwise git -C ${REPO} fetch origin wf/${slug} main and list the extra commits: git -C ${REPO} log --format='%H %P %s' <requested head>..origin/wf/${slug} . Continue only if the requested head is an ancestor (git -C ${REPO} merge-base --is-ancestor <requested head> origin/wf/${slug}) and every listed commit is a merge commit (two parent hashes) whose subject starts with "Merge branch 'main'": a branch update an earlier merge attempt made. Anything else is a push the reviewer has not seen: answer head_moved with head = the PR head.
2. Update: only when pr-state shows behind_by > 0: gh pr update-branch <n> -R ${GH} (a merge commit, never --rebase), then pr-state again for the new head. A merge conflict: answer conflict with the conflicting files in notes. behind_by 0: continue.
3. CI: ${MB} ci-wait --run ${RUN} --broker merge --head <head> --wait 570 ; while it prints state pending, run it again (at most 10 runs, about 90 minutes; longer: answer retry). It returns success only when every check run on the head finished green and the required jobs (lint, unit, package, raw/ioredis/node-redis mock and real, socketless, compatibility-profiles-mock) are present. state error: run it once more, then fall back to gh pr checks <n> -R ${GH}.
   On a failure: list the failing jobs and read their logs (gh run view <run-id> -R ${GH} --log-failed | tail -n 200). It is a flake only if the failing test has an open flaky-test issue (gh issue list -R ${GH} --state open --search 'Flaky <test file or name>') or it fails the same way on the latest main CI run (gh run list -R ${GH} --branch main --workflow CI --limit 3); then rerun the failed jobs once (gh run rerun <run-id> -R ${GH} --failed), comment the test name on that issue (or file a "Flaky test: ..." issue with the run link and failing lines), and go back to waiting. Otherwise write ${D}/ci/c<next number>.md (mkdir -p first) with the head sha, each failing check, the failing test names and about 40 log lines per failure, and answer ci_failed with file = that path and head = the sha.
   Checks that never start or fail for GitHub Actions infrastructure reasons (cancelled, queued > 30 min, runner errors) twice in a row: answer retry with notes "ci outage".
4. Merge: never edit the PR title or body (its Closes/Part of lines are the implementation agent's decision). If the PR is a draft, mark it ready (gh pr ready <n> -R ${GH}). gh pr merge <n> -R ${GH} --squash --match-head-commit <the head verified green>. "Head branch was modified" or "not up to date": back to step 2 (at most 3 times). If the permission system denies the merge, do not try another route: answer merge_blocked.
5. After the merge: the merge SHA is gh pr view <n> -R ${GH} --json mergeCommit -q .mergeCommit.oid ; git -C ${REPO} push origin --delete wf/${slug} (ignore "remote ref does not exist"); confirm every "Closes" issue is closed (gh issue view <i> -R ${GH} --json state; close any that did not auto-close with gh issue close <i> -R ${GH} --reason completed --comment 'Fixed by #<n> (<sha>)'); ${MB} state --run ${RUN} --lane ${slug} --set status=merged --set merge_sha=<sha> ; answer merged with merge_sha.
Return: status, answered (true once the answer command printed "written": true), merge_sha, notes.`
}

// ---------------------------------------------------------------------------
// Concurrency helpers
// ---------------------------------------------------------------------------
function sem(n) {
  let active = 0
  const q = []
  return async fn => {
    if (active < n) active++
    else await new Promise(r => q.push(r))
    try {
      return await fn()
    } finally {
      const nx = q.shift()
      if (nx) nx()
      else active--
    }
  }
}
const hotLocks = {}
for (const L of ALL_LANES)
  for (const h of L.hot || []) if (!hotLocks[h]) hotLocks[h] = sem(1)
function withHot(groups, fn) {
  const gs = [...groups].sort()
  const step = i => (i >= gs.length ? fn() : hotLocks[gs[i]](() => step(i + 1)))
  return step(0)
}
function parseLines(lines) {
  const st = {}
  for (const ln of lines || []) {
    const [slug, status, stage, pr, round] = String(ln).split('|')
    if (slug)
      st[slug] = {
        status: status || 'active',
        stage: stage || '',
        pr: pr ? Number(pr) : undefined,
        round: round ? Number(round) : 0,
      }
  }
  return st
}
function parseData(s) {
  if (s && typeof s === 'object') return s
  try {
    return JSON.parse(s || '{}')
  } catch {
    return {}
  }
}

// ---------------------------------------------------------------------------
// Track role: one lane = one long-lived implementation agent + one long-lived review agent
// ---------------------------------------------------------------------------
async function lifecycle(L, prev, retried) {
  const slug = L.slug
  const history = []
  const resuming =
    prev && !DONE_BEFORE.includes(prev.status) && prev.stage ? prev : null
  // A retried lane gets a fresh round budget on top of the rounds it already used.
  const roundLimit =
    (retried && prev && prev.round ? prev.round : 0) + MAX_ROUNDS
  const runs = { impl: 0, review: 0 }
  const fails = { impl: 0, review: 0 } // early exits that are not checkpoints
  let checkpoints = 0
  let brief = null
  // Set once the PR exists; agents started after it get a pointer to the brief, not the brief.
  let pushed = !!(resuming && resuming.stage !== 'impl')
  const live = { impl: null, review: null }
  const done = { impl: null, review: null }
  const seen = { impl: 0, review: 0 } // the run number whose exit the loop below has handled

  const start = (role, resume) => {
    const n = ++runs[role]
    done[role] = null
    const model =
      role === 'impl' ? L.implModel || M.impl : L.reviewModel || M.review
    const opts =
      role === 'impl'
        ? {
            label: `impl:${slug}${n > 1 ? '#' + n : ''}`,
            phase: 'Implement',
            schema: IMPL_SCHEMA,
          }
        : {
            label: `review:${slug}${n > 1 ? '#' + n : ''}`,
            phase: 'Review',
            schema: REVIEWER_SCHEMA,
          }
    if (model) opts.model = model
    const effort = role === 'impl' ? L.implEffort : L.reviewEffort
    if (effort) opts.effort = effort
    const prompt =
      role === 'impl'
        ? implPrompt(
            L,
            resume,
            pushed ? null : brief,
            checkpoints < MAX_CHECKPOINTS,
          )
        : reviewerPrompt(L, resume, roundLimit)
    live[role] = agentAs(role, prompt, opts).then(r => {
      if (r && r.pr_number) pushed = true
      done[role] = r || { status: 'died', summary: `${role} agent died` }
      return { role, n, r: done[role] }
    })
  }
  const why = (role, r) =>
    `The previous ${role === 'impl' ? 'implementation' : 'review'} agent of this lane exited early (${r.status}: ${(r.summary || '').slice(0, 300)}).`
  const sendFinal = async data => {
    await agentAs('coord', deliverFinalPrompt(L, data), {
      label: `final:${slug}`,
      phase: 'Coordinate',
      model: M.coord,
      effort: 'low',
    })
  }

  if (BRIEF && !resuming) {
    const b = await agentAs('brief', briefPrompt(L), {
      label: `brief:${slug}`,
      phase: 'Implement',
      model: M.brief,
    })
    if (typeof b === 'string' && b.trim() === 'STOP') {
      STOPPING = true
      return { slug, status: 'stopped', history }
    }
    if (typeof b === 'string' && b.trim()) brief = b.trim().slice(0, 24000)
    else if (b && typeof b === 'object' && b.brief)
      brief = String(b.brief).slice(0, 24000)
  }
  start(
    'impl',
    resuming
      ? implResume(
          L,
          'A previous implementation agent for this lane stopped (run restart).',
        )
      : null,
  )
  // A checkpoint hands the lane to a fresh implementation agent; anything else counts as an early exit.
  const checkpointed = r =>
    r.status === 'checkpoint' && checkpoints < MAX_CHECKPOINTS && ++checkpoints
  const ckptWhy =
    'The previous implementation agent of this lane checkpointed to keep its context small; you continue its work with an empty context.'

  // Until the first push only the implementation agent runs; a cheap watcher holds the second slot.
  const reviewing = pushed
  let idleMin = 0
  let watchFails = 0
  let firstWatch = true
  // An implementation agent's exit (a checkpoint above all) posts no mailbox message, so the watcher
  // races it instead of being waited out; a watcher still in flight is reused on the next pass.
  let watching = null
  const settle = async res => {
    if (watching) await watching
    return res
  }
  while (!reviewing) {
    if (!watching)
      watching = agentAs('coord', watchPrompt(L, firstWatch), {
        label: `watch:${slug}`,
        phase: 'Coordinate',
        model: M.coord,
        effort: 'low',
        schema: WAIT_SCHEMA,
      }).then(w => ({ w }))
    const ev = await Promise.race([watching, live.impl.then(() => null)])
    const w = ev && ev.w
    if (ev) watching = null
    if (w) firstWatch = false
    if (w && w.stop) {
      STOPPING = true
      await live.impl
      return { slug, status: 'stopped', history, impl: done.impl }
    }
    const msgs = w
      ? (w.messages || []).map(m => ({ kind: m.kind, data: parseData(m.data) }))
      : []
    const term = msgs.find(m => m.kind === 'no_change' || m.kind === 'blocked')
    if (term) {
      await live.impl
      log(
        `${slug}: ${term.kind} — ${(term.data.summary || term.data.reason || '').slice(0, 160)}`,
      )
      return { slug, status: term.kind, history, impl: done.impl }
    }
    if (msgs.some(m => m.kind === 'ready')) {
      pushed = true
      break
    }
    if (done.impl) {
      seen.impl = runs.impl
      if (done.impl.status === 'stopped') STOPPING = true
      if (['no_change', 'blocked', 'stopped'].includes(done.impl.status))
        return settle({
          slug,
          status: done.impl.status,
          history,
          impl: done.impl,
        })
      if (checkpointed(done.impl)) {
        history.push(`impl#${runs.impl} checkpoint`)
        start('impl', implResume(L, ckptWhy))
        idleMin = 0
        continue
      }
      if (++fails.impl > RESTARTS) {
        // The last exit may follow a first push whose ready the watcher has not reported yet: then
        // the review side still finishes the lane, as it does for an exit after the push.
        const late = watching ? (await watching).w : null
        watching = null
        if (!(late && (late.messages || []).some(m => m.kind === 'ready')))
          return {
            slug,
            status: 'failed',
            notes: 'implementation agent exited too often',
            history,
            impl: done.impl,
          }
        history.push(`impl gave up after the first push: ${done.impl.status}`)
        pushed = true
        break
      }
      history.push(
        `impl#${runs.impl} exited before the first push: ${done.impl.status}`,
      )
      log(
        `${slug}: implementation agent exited (${done.impl.status}); starting a replacement`,
      )
      start('impl', implResume(L, why('impl', done.impl)))
      idleMin = 0
      continue
    }
    if (!w) {
      if (++watchFails > 3) {
        await sendFinal({ reason: 'failed' })
        await live.impl
        return {
          slug,
          status: 'failed',
          notes: 'mailbox watcher failed repeatedly',
          history,
          impl: done.impl,
        }
      }
      continue
    }
    watchFails = 0
    idleMin += WAIT_CALLS * 9.5
    if (idleMin > MAX_IDLE_MIN) {
      await sendFinal({ reason: 'stalled' })
      await live.impl
      return { slug, status: 'stalled', history, impl: done.impl }
    }
  }

  log(`${slug}: first push ready; review agent starting`)
  start(
    'review',
    resuming && resuming.stage !== 'impl'
      ? reviewerResume(
          L,
          'A previous review agent for this lane stopped (run restart).',
        )
      : null,
  )

  for (let guard = 0; guard < 50; guard++) {
    // Handle an exit the loop has not seen yet before waiting on anything: an agent can finish
    // while the other one is still being started, or both in the same tick.
    const exitOf = role =>
      done[role] && seen[role] < runs[role]
        ? { role, n: runs[role], r: done[role] }
        : null
    let ev = exitOf('review') || exitOf('impl')
    if (!ev) {
      const pending = ['impl', 'review']
        .filter(role => !done[role])
        .map(role => live[role])
      if (!pending.length) break
      ev = await Promise.race(pending)
    }
    const { role, r } = ev
    seen[role] = ev.n
    if (role === 'review') {
      if (FINAL.includes(r.status)) {
        history.push(`review: ${r.status} — ${(r.summary || '').slice(0, 300)}`)
        if (!done.impl) {
          if (r.status !== 'stopped')
            await sendFinal({ reason: r.status, merge_sha: r.merge_sha || '' })
          await live.impl
        }
        log(
          `${slug}: ${r.status}${r.pr_number ? ' (PR #' + r.pr_number + ')' : ''}${r.merge_sha ? ' ' + r.merge_sha : ''}`,
        )
        return {
          slug,
          status: r.status,
          pr: r.pr_number,
          rounds: r.rounds,
          merge_sha: r.merge_sha,
          history,
          impl: done.impl,
          review: r,
        }
      }
      const implGone =
        done.impl &&
        seen.impl === runs.impl &&
        (FINAL.includes(done.impl.status) || fails.impl > RESTARTS)
      if (++fails.review > RESTARTS || implGone) {
        if (!done.impl) {
          await sendFinal({ reason: 'failed' })
          await live.impl
        }
        return {
          slug,
          status: 'failed',
          notes: 'review agent exited too often',
          history,
          impl: done.impl,
          review: r,
        }
      }
      history.push(`review#${runs.review} exited: ${r.status}`)
      log(`${slug}: review agent exited (${r.status}); starting a replacement`)
      start('review', reviewerResume(L, why('review', r)))
      continue
    }
    // The implementation agent returned.
    if (FINAL.includes(r.status)) continue // it got its final message; the review agent returns next
    if (checkpointed(r)) {
      history.push(`impl#${runs.impl} checkpoint`)
      start('impl', implResume(L, ckptWhy))
      continue
    }
    if (++fails.impl > RESTARTS) {
      history.push(`impl gave up: ${r.status}`)
      if (!done.review) continue // the review agent ends the lane (stalled at worst)
      return {
        slug,
        status: 'failed',
        notes: 'implementation agent exited too often',
        history,
        impl: r,
        review: done.review,
      }
    }
    history.push(`impl#${runs.impl} exited: ${r.status}`)
    log(
      `${slug}: implementation agent exited (${r.status}); starting a replacement`,
    )
    start('impl', implResume(L, why('impl', r)))
  }
  return {
    slug,
    status: (done.review && done.review.status) || 'failed',
    history,
    impl: done.impl,
    review: done.review,
  }
}

async function trackMain() {
  phase('Coordinate')
  const st0 = await agentAs('coord', statusPrompt(), {
    label: 'status',
    phase: 'Coordinate',
    model: M.coord,
    effort: 'low',
    schema: LINES_SCHEMA,
  })
  let STATE = parseLines(st0 && st0.lines)
  const laneSem = sem(LANES)
  const results = []
  const waves = PLAN.waves || []
  log(
    `track ${TRACK}: ${waves.length} waves, ${LANES} lane(s) at a time, ${Object.keys(STATE).length} lanes with saved state`,
  )
  for (let wi = 0; wi < waves.length; wi++) {
    const mine = (waves[wi].lanes || [])
      .filter(L => L.track === TRACK)
      .map(L => Object.assign({ wave: waves[wi].name || String(wi + 1) }, L))
    const settled = {}
    for (const L of mine) {
      let res
      settled[L.slug] = {
        p: new Promise(r => {
          res = r
        }),
        res: s => res(s),
      }
    }
    const runOne = async L => {
      const prev = STATE[L.slug]
      if (prev && DONE_BEFORE.includes(prev.status) && !RETRY.includes(L.slug))
        return { slug: L.slug, status: prev.status, pr: prev.pr, resumed: true }
      const same = (L.deps || []).filter(d => settled[d])
      const sameSt = await Promise.all(same.map(d => settled[d].p))
      const bad = (L.deps || []).filter(d => {
        const s = settled[d]
          ? sameSt[same.indexOf(d)]
          : STATE[d] && STATE[d].status
        return !SATISFIED.includes(s)
      })
      if (bad.length) {
        log(`${L.slug}: skipped, deps not merged: ${bad.join(', ')}`)
        return {
          slug: L.slug,
          status: 'skipped',
          notes: 'deps not merged: ' + bad.join(', '),
        }
      }
      const retryPrev =
        prev && RETRY.includes(L.slug)
          ? Object.assign({}, prev, { status: 'active' })
          : prev
      const notStarted = {
        slug: L.slug,
        status: 'stopped',
        notes: 'run stopping: lane not started',
      }
      if (STOPPING) return notStarted
      return withHot(L.hot || [], () =>
        laneSem(() =>
          STOPPING
            ? notStarted
            : lifecycle(L, retryPrev, RETRY.includes(L.slug)),
        ),
      )
    }
    const rs = await parallel(
      mine.map(L => async () => {
        let r
        try {
          r = await runOne(L)
        } catch (e) {
          r = { slug: L.slug, status: 'failed', notes: String(e) }
        }
        if (r.status === 'stopped') STOPPING = true
        settled[L.slug].res(r.status)
        STATE[L.slug] = Object.assign({}, STATE[L.slug], {
          status: r.status,
          pr: r.pr,
        })
        return r
      }),
    )
    results.push(...rs.filter(Boolean))
    log(
      `track ${TRACK}: wave ${wi + 1} done (${rs.filter(r => r && r.status === 'merged').length}/${mine.length} merged); waiting for the other tracks`,
    )
    let b = null
    for (let tries = 0; tries < 20; tries++) {
      b = await agentAs('coord', barrierPrompt(wi, wi === waves.length - 1), {
        label: `barrier:w${wi + 1}`,
        phase: 'Coordinate',
        model: M.coord,
        effort: 'low',
        schema: LINES_SCHEMA,
      })
      if (b && (b.passed || b.stop)) break
    }
    if (!b || b.stop || !b.passed) {
      log(
        `track ${TRACK}: stopping after wave ${wi + 1} (${b && b.stop ? 'STOP' : 'barrier did not pass'})`,
      )
      break
    }
    STATE = Object.assign(STATE, parseLines(b.lines))
  }
  return results.map(r => ({
    slug: r.slug,
    status: r.status,
    pr: r.pr,
    rounds: r.rounds,
    resumed: r.resumed,
    merge_sha: r.merge_sha,
    notes: r.notes,
    history: r.history,
    summary: r.impl ? r.impl.summary : undefined,
    closes: r.impl ? r.impl.closes : undefined,
    followups: r.impl ? r.impl.followups_filed : undefined,
    review: r.review ? r.review.summary : undefined,
  }))
}

// ---------------------------------------------------------------------------
// Merger role: one merge agent at a time, for every track
// ---------------------------------------------------------------------------
async function mergerMain() {
  phase('Merge')
  const handled = []
  let requeued = false
  log(`merger: serving merge requests for tracks ${TRACKS.join(',')}`)
  for (let i = 0; i < 900; i++) {
    const t = await agentAs('coord', takePrompt('merge', !requeued), {
      label: 'take',
      phase: 'Merge',
      model: M.coord,
      effort: 'low',
      schema: TAKE_SCHEMA,
    })
    if (!t) continue
    requeued = true // only a take that actually ran has re-served the orphans
    if (t.stop) break
    for (const ev of t.events || []) {
      const slug = String(ev.from).split('/')[0]
      const L = LANE_BY[slug] || { slug, issues: [] }
      const m = await agentAs('merge', mergePrompt(ev, L), {
        label: `merge:${slug}`,
        phase: 'Merge',
        model: M.merge,
        schema: MERGE_SCHEMA,
      })
      if (!m || !m.answered) {
        await agentAs(
          'deliver',
          deliverPrompt(
            ev.id,
            JSON.stringify({
              status: 'retry',
              notes: 'merge agent ' + (m ? 'did not answer' : 'failed'),
            }),
          ),
          {
            label: `deliver:${ev.id}`,
            phase: 'Merge',
            model: M.deliver,
            effort: 'low',
          },
        )
      }
      handled.push(`${slug}: ${m ? m.status : 'failed'}`)
      log(
        `merger: ${slug} ${m ? m.status : 'failed'}${m && m.merge_sha ? ' ' + m.merge_sha : ''}`,
      )
    }
  }
  return { handled }
}

// ---------------------------------------------------------------------------
// Helper role: serve helper requests from every track
// ---------------------------------------------------------------------------
async function helperMain() {
  phase('Helpers')
  const pending = []
  const perLane = {}
  const served = []
  let requeued = false
  const deliver = (id, text) =>
    agentAs('deliver', deliverPrompt(id, text), {
      label: `deliver:${id}`,
      phase: 'Helpers',
      model: M.deliver,
      effort: 'low',
    })
  const HELPER_ROLE = {
    'redis-researcher': 'researcher',
    'ts-scout': 'scout',
    runner: 'runner',
  }
  const helperRun = ev => {
    const role = HELPER_ROLE[ev.type] || 'runner'
    const opts = {
      label: `${ev.type}:${ev.from}`,
      phase: 'Helpers',
      model: M[role],
    }
    if (role === 'runner') opts.effort = 'low'
    return agentAs(role, helperPrompt(ev), opts)
  }
  log(`helper broker: serving helper requests for tracks ${TRACKS.join(',')}`)
  for (let i = 0; i < 900; i++) {
    const t = await agentAs('coord', takePrompt('helper', !requeued), {
      label: 'take',
      phase: 'Helpers',
      model: M.coord,
      effort: 'low',
      schema: TAKE_SCHEMA,
    })
    if (!t) continue
    requeued = true // only a take that actually ran has re-served the orphans
    if (t.stop) break
    for (const ev of t.events || []) {
      const lane = String(ev.from).split('/')[0]
      if (ev.type === 'redis-researcher') {
        perLane[lane] = (perLane[lane] || 0) + 1
        if (perLane[lane] > RESEARCHER_LIMIT) {
          pending.push(
            deliver(
              ev.id,
              `DENIED: the redis-researcher limit (${RESEARCHER_LIMIT} per lane) is used up for ${lane}. Probe and read the sources inline.`,
            ),
          )
          continue
        }
      }
      served.push(`${ev.type} ${ev.from}`)
      pending.push(
        helperRun(ev).then(ans =>
          deliver(
            ev.id,
            ans
              ? String(ans)
              : `ERROR: the ${ev.type} helper failed. Research inline.`,
          ),
        ),
      )
    }
  }
  await Promise.all(pending)
  log(`helper broker: served ${served.length} requests`)
  return { served }
}

if (ROLE === 'merger') return await mergerMain()
if (ROLE === 'helper') return await helperMain()
return await trackMain()
