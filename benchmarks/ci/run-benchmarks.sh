#!/usr/bin/env bash
# Benchmark comparison script for pull requests.
#
# Called by .github/workflows/benchmark.yml (run-benchmark job) after the repo
# has been checked out at the PR's merge commit (refs/pull/<N>/merge -- PR head
# merged into base). Writes the formatted markdown comparison
# to /tmp/bench-comment.md; the companion benchmark-post-comment.yml workflow
# downloads it as an artifact and posts the PR comment in base-branch context.
#
# Expects the following environment variables:
#
#   BASE_REF   - base branch ref (e.g. "main")
#   HEAD_SHA   - full SHA of the PR head commit
#   COMMENT    - (optional) /bench PR comment body. Unset under the
#                pull_request auto-trigger path; set to the comment
#                body under the issue_comment path.
#   TRIGGER    - (optional) human-readable label for what kicked off the run
#                ("auto-push" or "/bench"). Used in the comment header.
#   BENCH_IGNORE_FAILURE - (optional) "true" when the PR carries the
#                ignore-benchmark-failure label. Passed through to
#                parse_critcmp.py so the comment's verdict line says the
#                regression gate is overridden.

set -euo pipefail
shopt -s extglob

# ---------------------------------------------------------------------------
# 1. Parse the /bench comment
#    Syntax: /bench [--tags <csv>] [--filter <regex>]
#      --tags    sets BENCH_TAGS (comma-separated tag list); defaults to "base"
#                when the comment is just /bench (or COMMENT is unset, i.e.
#                the auto-trigger path)
#      --filter  Criterion name regex passed as a positional arg to cargo bench
# ---------------------------------------------------------------------------

# Auto-trigger path leaves COMMENT unset; treat that the same as a bare /bench.
COMMENT="${COMMENT:-}"

ARGS="${COMMENT#/bench}"
ARGS="${ARGS##+( )}"

TAGS=""
FILTER=""

if [[ -z "$ARGS" ]]; then
  # Bare /bench with no args: default to the "base" tag
  TAGS="base"
else
  # Normalize: strip /bench prefix, collapse all whitespace (including newlines)
  # to spaces, then strip to a safe allowlist before parsing
  ARGS=$(printf '%s' "$ARGS" | tr '\n\r\t' ' ' | tr -s ' ' | tr -cd 'a-zA-Z0-9,_./|*+?()[]^$ -')
  ARGS="${ARGS## }"   # strip leading space
  ARGS="${ARGS%% }"   # strip trailing space

  read -ra TOKENS <<< "$ARGS"
  i=0
  while [[ $i -lt ${#TOKENS[@]} ]]; do
    case "${TOKENS[$i]}" in
      --tags)   i=$((i + 1)); TAGS="${TOKENS[$i]:-}"   ;;
      --filter) i=$((i + 1)); FILTER="${TOKENS[$i]:-}" ;;
      *)        echo "Unknown token: '${TOKENS[$i]}'" >&2; exit 1 ;;
    esac
    i=$((i + 1))
  done
fi

# Default: if nothing was parsed, run with BENCH_TAGS=base
if [[ -z "$TAGS" && -z "$FILTER" ]]; then
  TAGS="base"
fi

echo "Parsed tags:   ${TAGS:-<none>}"
echo "Parsed filter: ${FILTER:-<none>}"

[[ -n "$TAGS" ]] && export BENCH_TAGS="$TAGS"

# ---------------------------------------------------------------------------
# 2. Log the runner environment
#    GitHub-hosted runners draw from a heterogeneous pool (varying CPU model,
#    cache size, neighbor load). Logging hardware and load per run gives us the
#    data to attribute run-to-run variance to the machine vs. the code.
# ---------------------------------------------------------------------------
echo "=== Runner environment ==="
lscpu | grep -E '^(Model name|CPU\(s\)|Thread|CPU( max| min)? MHz|L[123][a-z]* cache)' || true
echo "loadavg: $(cat /proc/loadavg)"
echo "mem: $(free -m | awk '/^Mem:/ {print $2 " MB total, " $7 " MB available"}')"
echo "==========================="

# ---------------------------------------------------------------------------
# 3. Benchmark the integration commit (PR head merged into base)
# ---------------------------------------------------------------------------
# HEAD is the merge commit checked out by the workflow; capture it so step 5 can
# restore this tree after the base checkout below.
MERGE_SHA=$(git rev-parse HEAD)
(cd benchmarks && cargo bench --locked --bench workload_bench -- --save-baseline changes "$FILTER")

# ---------------------------------------------------------------------------
# 4. Switch to the base branch and benchmark it
#    The benchmarks/target/ directory is not tracked by git, so the
#    "changes" baseline files are preserved across the branch switch.
# ---------------------------------------------------------------------------
git fetch origin -- "$BASE_REF"
git checkout FETCH_HEAD
BASE_SHA=$(git rev-parse HEAD)
(cd benchmarks && cargo bench --locked --bench workload_bench -- --save-baseline base "$FILTER")

# ---------------------------------------------------------------------------
# 5. Compare baselines with critcmp and format as a markdown comment
#    (per-tier summary line + collapsed per-benchmark table; see
#    benchmarks/ci/parse_critcmp.py for the format and tier thresholds).
#      - Parses actual duration values (not rank factors) to compute a ratio
# ---------------------------------------------------------------------------
# Step 4 left the working tree on the base branch, so parse_critcmp.py on disk
# is the base version. Restore the merge tree so the comment is formatted by the
# PR's own formatter (a formatter change is then exercised on the PR that
# introduces it). The raw PR-head SHA is not reachable in the shallow merge-ref
# checkout, so restore the merge commit captured in step 3. Only tracked sources
# move; the `base`/`changes` criterion baselines live in gitignored
# benchmarks/target/ and survive the checkout.
git checkout "$MERGE_SHA"
# Use `critcmp` to compare the criterion output for `base` and `changes`. We use `critcmp` instead of manually
# parsing criterion outputs because criterion may update its output format. By using `critcmp`, we inherit all
# updated criterion output parsing.
#
# parse_critcmp.py records whether any benchmark regressed past its fail
# threshold in this file; benchmark.yml's gate step reads it to fail the job.
# It also records whether a non-overridden regression is below the automatic
# retry threshold, allowing one fresh measurement pair to replace a noisy run.
export BENCH_REGRESSION_FILE=/tmp/bench-regression.txt
export BENCH_RETRY_FILE=/tmp/bench-retry.txt
ATTEMPT=1
MAX_ATTEMPTS=2

while true; do
  COMPARISON=$((cd benchmarks && critcmp base changes) | python3 benchmarks/ci/parse_critcmp.py)
  RETRY=$(tr -d '[:space:]' < "$BENCH_RETRY_FILE" 2>/dev/null || echo false)

  if [[ "$RETRY" != "true" || "$ATTEMPT" -ge "$MAX_ATTEMPTS" ]]; then
    break
  fi

  ATTEMPT=$((ATTEMPT + 1))
  echo "::notice::Benchmark regression is below 1.50x; starting attempt $ATTEMPT/$MAX_ATTEMPTS."
  (
    cd benchmarks
    cargo bench --locked --bench workload_bench -- --save-baseline changes "$FILTER"
  )
  git checkout "$BASE_SHA"
  (cd benchmarks && cargo bench --locked --bench workload_bench -- --save-baseline base "$FILTER")
  git checkout "$MERGE_SHA"
done

# ---------------------------------------------------------------------------
# 6. Write results to /tmp/bench-comment.md
#    benchmark.yml uploads this as an artifact; benchmark-post-comment.yml
#    downloads it and posts the PR comment in base-branch context.
# ---------------------------------------------------------------------------
SHORT_SHA="${HEAD_SHA:0:7}"

# Metadata footer shows commit + what fired this run + the active tags/filter +
# when the comment was last refreshed, so a reviewer can tell at a glance which
# configuration produced the displayed numbers (the same comment is reused
# across auto-trigger and /bench runs).
SUMMARY="Commit: \`${SHORT_SHA}\` &middot; Trigger: ${TRIGGER:-auto-push}"
[[ -n "$TAGS" ]]   && SUMMARY+=" &middot; Tags: \`${TAGS}\`"
[[ -n "$FILTER" ]] && SUMMARY+=" &middot; Filter: \`${FILTER}\`"
if [[ "$ATTEMPT" -gt 1 ]]; then
  SUMMARY+=" &middot; Automatic retry: attempt ${ATTEMPT}/${MAX_ATTEMPTS}"
fi
SUMMARY+=" &middot; Updated: $(TZ=America/Los_Angeles date '+%Y-%m-%d %H:%M %Z')"

# Leading marker is an HTML comment, invisible to readers; the post-comment
# job uses it as a stable identifier for find-and-update so each push reuses
# the same comment instead of stacking new ones.
{
  echo "<!-- delta-kernel-bench-comment -->"
  echo "$COMPARISON"
  echo "<sub>${SUMMARY}</sub>"
} > /tmp/bench-comment.md
