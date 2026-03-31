#!/usr/bin/env bash
# Benchmark comparison script for pull requests.
#
# Called by .github/workflows/benchmark.yml (run-benchmark job) after the repo
# has been checked out at the PR head. Writes the formatted markdown comparison
# to /tmp/bench-comment.md; the post-comment job picks it up and posts it.
#
# Expects the following environment variables:
#
#   COMMENT    - the /bench PR comment body
#   BASE_REF   - base branch ref (e.g. "main")
#   HEAD_SHA   - full SHA of the PR head commit

set -euo pipefail
shopt -s extglob

# ---------------------------------------------------------------------------
# 1. Parse the /bench comment
#    Syntax: /bench [--tags <csv>] [--filter <regex>]
#      --tags    sets BENCH_TAGS (comma-separated tag list); defaults to "base"
#                when the comment is just /bench
#      --filter  Criterion name regex passed as a positional arg to cargo bench
# ---------------------------------------------------------------------------

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
#      - Parses actual duration values (not rank factors) for the % column
#      - Bolds the faster duration and % cell when the difference is
#        statistically significant (error bounds do not overlap)
# ---------------------------------------------------------------------------
# Use `critcmp` to compare the criterion output for `base` and `changes`. We use `critcmp` instead of manually
# parsing criterion outputs because criterion may update its output format. By using `critcmp`, we inherit all
# updated criterion output parsing.
COMPARISON=$((cd benchmarks && critcmp base changes) | python3 benchmarks/ci/parse_critcmp.py)

# ---------------------------------------------------------------------------
# 5. Write results to /tmp/bench-comment.md
#    The post-comment job in benchmark.yml downloads this file as an artifact
#    and posts it as a PR comment using a step that holds GH_TOKEN.
# ---------------------------------------------------------------------------
SHORT_SHA="${HEAD_SHA:0:7}"

SUMMARY=""
[[ -n "$TAGS" ]]   && SUMMARY="tags: \`${TAGS}\`"
[[ -n "$FILTER" ]] && SUMMARY+="${SUMMARY:+ | }filter: \`${FILTER}\`"

{
  echo "## Benchmark for ${SHORT_SHA}"
  echo "<details>"
  echo "<summary>${SUMMARY}</summary>"
  echo ""
  echo "$COMPARISON"
  echo ""
  echo "</details>"
} > /tmp/bench-comment.md
