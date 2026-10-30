# Addressing review findings on a js-redis-server PR

1. Read the review file you were sent and the PR's unresolved review threads
   (`gh api graphql` reviewThreads of the PR, or `gh api repos/fatal10110/js-redis-server/pulls/<n>/comments`).
2. Verify each item before touching code: re-probe the real servers (`probe.py run`), read the
   code it points at, and the server source when the item is about a version boundary. A finding
   is not right because a reviewer wrote it, and not wrong because it is inconvenient.
3. Confirmed and in scope: fix it with a test that fails without the fix. Confirmed but out of
   the lane's scope: file or amend an issue with the evidence, and say so in the reply. Wrong:
   reply with the counter-evidence (probe output per version, source lines, a test run).
4. Run the scoped gates for what you changed (implement.md section 4), commit (never amend a
   pushed commit, never force-push), push.
5. Reply on every thread: "Fixed in <sha>: ..." or the rebuttal with evidence. Resolve the
   threads you fixed (`gh api graphql -f query='mutation { resolveReviewThread(input: {threadId:
   "<id>"}) { thread { isResolved } } }'`). Leave rebutted threads open for the reviewer.
