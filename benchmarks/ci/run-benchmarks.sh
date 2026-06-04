#!/usr/bin/env bash
# Benchmark comparison script for pull requests.
#
# Called by .github/workflows/benchmark.yml (run-benchmark job) after the repo
# has been checked out at the PR head. Writes the formatted markdown comparison
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
# 2. Benchmark the PR branch (already checked out by the workflow)
# ---------------------------------------------------------------------------
(cd benchmarks && cargo bench --locked --bench workload_bench -- --save-baseline changes "$FILTER")

# ---------------------------------------------------------------------------
# 3. Switch to the base branch and benchmark it
#    The benchmarks/target/ directory is not tracked by git, so the
#    "changes" baseline files are preserved across the branch switch.
# ---------------------------------------------------------------------------
git fetch origin -- "$BASE_REF"
git checkout FETCH_HEAD
(cd benchmarks && cargo bench --locked --bench workload_bench -- --save-baseline base "$FILTER")

# ---------------------------------------------------------------------------
# 4. Compare baselines with critcmp and format as a markdown table.
#      - Parses actual duration values (not rank factors) to compute a ratio
#      - Bolds the Change cell when the difference is statistically
#        significant (error bounds do not overlap)
# ---------------------------------------------------------------------------
# Use `critcmp` to compare the criterion output for `base` and `changes`. We use `critcmp` instead of manually
# parsing criterion outputs because criterion may update its output format. By using `critcmp`, we inherit all
# updated criterion output parsing.
COMPARISON=$((cd benchmarks && critcmp base changes) | python3 benchmarks/ci/parse_critcmp.py)

# ---------------------------------------------------------------------------
# 5. Write results to /tmp/bench-comment.md
#    benchmark.yml uploads this as an artifact; benchmark-post-comment.yml
#    downloads it and posts the PR comment in base-branch context.
# ---------------------------------------------------------------------------
SHORT_SHA="${HEAD_SHA:0:7}"

# Metadata line shows commit + what fired this run + the active tags/filter so
# a reviewer can tell at a glance which configuration produced the displayed
# numbers (the same comment is reused across auto-trigger and /bench runs).
SUMMARY="Commit: \`${SHORT_SHA}\` &middot; Trigger: ${TRIGGER:-auto-push}"
[[ -n "$TAGS" ]]   && SUMMARY+=" &middot; Tags: \`${TAGS}\`"
[[ -n "$FILTER" ]] && SUMMARY+=" &middot; Filter: \`${FILTER}\`"

# Leading marker is an HTML comment, invisible to readers; the post-comment
# job uses it as a stable identifier for find-and-update so each push reuses
# the same comment instead of stacking new ones.
{
  echo "<!-- delta-kernel-bench-comment -->"
  echo "## Benchmark results"
  echo ""
  echo "<sub>${SUMMARY}</sub>"
  echo ""
  echo "$COMPARISON"
} > /tmp/bench-comment.md
