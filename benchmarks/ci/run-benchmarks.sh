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
# 3. Benchmark both sides, compare with critcmp, retry once on a noisy regression
#    Each attempt benchmarks the merge tree ("changes") and the base tree
#    ("base"), then compares them. HEAD is the merge commit (PR head merged into
#    base) checked out by the workflow; capture it so each attempt can restore
#    this tree. The base SHA is resolved from the fetch without a checkout -- the
#    loop checks it out itself.
#
#    After measuring, restore the merge tree so the comment is formatted by the
#    PR's own parse_critcmp.py (a formatter change is then exercised on the PR
#    that introduces it). The raw PR-head SHA is not reachable in the shallow
#    merge-ref checkout, so restore the captured merge commit. Only tracked
#    sources move; the `base`/`changes` criterion baselines live in gitignored
#    benchmarks/target/ and survive the checkout.
#
#    We use `critcmp` to compare the criterion output for `base` and `changes`
#    instead of parsing criterion output ourselves: criterion may change its
#    output format, and critcmp tracks it, so we inherit any format updates.
#
#    parse_critcmp.py records whether any benchmark regressed past its fail
#    threshold in BENCH_REGRESSION_FILE (benchmark.yml's gate step reads it to
#    fail the job). It also records whether a non-overridden regression is below
#    the automatic retry threshold in BENCH_RETRY_FILE, allowing one fresh
#    measurement pair to replace a noisy run.
# ---------------------------------------------------------------------------
run_bench() { (cd benchmarks && cargo bench --locked --bench workload_bench -- --save-baseline "$1" "$FILTER"); }

MERGE_SHA=$(git rev-parse HEAD)
git fetch origin -- "$BASE_REF"
BASE_SHA=$(git rev-parse FETCH_HEAD)

export BENCH_REGRESSION_FILE=/tmp/bench-regression.txt
export BENCH_RETRY_FILE=/tmp/bench-retry.txt
ATTEMPT=1
MAX_ATTEMPTS=2

while true; do
  run_bench changes
  git checkout "$BASE_SHA"
  run_bench base
  git checkout "$MERGE_SHA"

  COMPARISON=$((cd benchmarks && critcmp base changes) | python3 benchmarks/ci/parse_critcmp.py)
  RETRY=$(tr -d '[:space:]' < "$BENCH_RETRY_FILE" 2>/dev/null || echo false)

  if [[ "$RETRY" != "true" || "$ATTEMPT" -ge "$MAX_ATTEMPTS" ]]; then
    break
  fi

  ATTEMPT=$((ATTEMPT + 1))
  echo "::notice::Benchmark regression within retry band; starting attempt $ATTEMPT/$MAX_ATTEMPTS."
done

# ---------------------------------------------------------------------------
# 4. Write results to /tmp/bench-comment.md
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
