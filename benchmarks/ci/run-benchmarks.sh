#!/usr/bin/env bash
# Benchmark comparison script for pull requests.
#
# Called by .github/workflows/benchmark.yml (run-benchmark job) after the repo
# has been checked out at the PR head. Writes the formatted markdown comparison
# to /tmp/bench-comment.md; the companion benchmark-post-comment.yml workflow
# downloads it as an artifact and posts the PR comment in base-branch context.
#
# Methodology: the bench harness is compiled once per branch, then the two
# binaries are executed in alternating rounds (base, PR, base, PR, ...). With
# a single pass per side, slow machine-state drift (noisy neighbors, frequency
# scaling) lands entirely on one side and reads as a direction-consistent bias
# across every benchmark; alternating rounds spread the drift over both sides.
# parse_critcmp.py compares the best time per side across rounds, reports a
# change only when it clears a noise band, and reports the round-to-round
# spread of identical code as the run's measured noise floor.
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
#    Syntax: /bench [--tags <csv>] [--filter <regex>] [--rounds <n>]
#                   [--sample-size <n>] [--measurement-time <secs>]
#                   [--warm-up-time <secs>]
#      --tags    sets BENCH_TAGS (comma-separated tag list); defaults to "base"
#                when the comment is just /bench (or COMMENT is unset, i.e.
#                the auto-trigger path)
#      --filter  Criterion name regex passed as a positional arg to the bench
#                binary
#      --rounds  number of alternating base/PR measurement rounds (1 to 5)
#      --sample-size, --measurement-time, --warm-up-time
#                passed through to Criterion to trade per-pass cost against
#                per-pass precision
# ---------------------------------------------------------------------------

# Auto-trigger path leaves COMMENT unset; treat that the same as a bare /bench.
COMMENT="${COMMENT:-}"

ARGS="${COMMENT#/bench}"
ARGS="${ARGS##+( )}"

TAGS=""
FILTER=""
ROUNDS="2"
SAMPLE_SIZE=""
MEASUREMENT_TIME=""
WARM_UP_TIME=""

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
      --tags)             i=$((i + 1)); TAGS="${TOKENS[$i]:-}"             ;;
      --filter)           i=$((i + 1)); FILTER="${TOKENS[$i]:-}"           ;;
      --rounds)           i=$((i + 1)); ROUNDS="${TOKENS[$i]:-}"           ;;
      --sample-size)      i=$((i + 1)); SAMPLE_SIZE="${TOKENS[$i]:-}"      ;;
      --measurement-time) i=$((i + 1)); MEASUREMENT_TIME="${TOKENS[$i]:-}" ;;
      --warm-up-time)     i=$((i + 1)); WARM_UP_TIME="${TOKENS[$i]:-}"     ;;
      *)        echo "Unknown token: '${TOKENS[$i]}'" >&2; exit 1 ;;
    esac
    i=$((i + 1))
  done
fi

# Default: if no workload selection was parsed, run with BENCH_TAGS=base
if [[ -z "$TAGS" && -z "$FILTER" ]]; then
  TAGS="base"
fi

# Validate numeric arguments. ROUNDS is capped to keep a /bench comment from
# requesting an arbitrarily long CI run; the Criterion knobs are validated as
# plain numbers and any range enforcement is left to Criterion itself.
[[ "$ROUNDS" =~ ^[1-5]$ ]] \
  || { echo "Invalid --rounds (expected 1 to 5): $ROUNDS" >&2; exit 1; }
[[ -z "$SAMPLE_SIZE" || "$SAMPLE_SIZE" =~ ^[0-9]+$ ]] \
  || { echo "Invalid --sample-size: $SAMPLE_SIZE" >&2; exit 1; }
[[ -z "$MEASUREMENT_TIME" || "$MEASUREMENT_TIME" =~ ^[0-9]+([.][0-9]+)?$ ]] \
  || { echo "Invalid --measurement-time: $MEASUREMENT_TIME" >&2; exit 1; }
[[ -z "$WARM_UP_TIME" || "$WARM_UP_TIME" =~ ^[0-9]+([.][0-9]+)?$ ]] \
  || { echo "Invalid --warm-up-time: $WARM_UP_TIME" >&2; exit 1; }

CRIT_ARGS=()
[[ -n "$SAMPLE_SIZE" ]]      && CRIT_ARGS+=(--sample-size "$SAMPLE_SIZE")
[[ -n "$MEASUREMENT_TIME" ]] && CRIT_ARGS+=(--measurement-time "$MEASUREMENT_TIME")
[[ -n "$WARM_UP_TIME" ]]     && CRIT_ARGS+=(--warm-up-time "$WARM_UP_TIME")

echo "Parsed tags:   ${TAGS:-<none>}"
echo "Parsed filter: ${FILTER:-<none>}"
echo "Rounds:        ${ROUNDS}"
echo "Criterion args: ${CRIT_ARGS[*]:-<defaults>}"

[[ -n "$TAGS" ]] && export BENCH_TAGS="$TAGS"

# ---------------------------------------------------------------------------
# 2. Log the runner environment
#    GitHub-hosted runners draw from a heterogeneous pool (different CPU
#    models, cache sizes, and neighbor load). Recording the hardware and load
#    alongside each run lets a reader correlate noisy comparisons with the
#    machine they ran on.
# ---------------------------------------------------------------------------
echo "=== Runner environment ==="
lscpu | grep -E '^(Model name|CPU\(s\)|Thread|CPU( max| min)? MHz|L[123][a-z]* cache)' || true
echo "loadavg: $(cat /proc/loadavg)"
echo "mem: $(free -m | awk '/^Mem:/ {print $2 " MB total, " $7 " MB available"}')"
echo "==========================="

# ---------------------------------------------------------------------------
# 3. Compile one bench binary per branch
#    Compiling once and stashing the binaries lets the measurement rounds
#    alternate between branches without recompiling on every switch.
#    CRITERION_HOME pins where the binaries store baselines no matter which
#    tree is checked out when they run; critcmp reads the same location via
#    --target-dir below.
# ---------------------------------------------------------------------------
TARGET_DIR=$( (cd benchmarks && cargo metadata --format-version 1 --no-deps --locked) | jq -r .target_directory)
export CRITERION_HOME="$TARGET_DIR/criterion"
echo "Criterion home: $CRITERION_HOME"

# Compiles the bench harness for the currently checked-out tree and echoes the
# binary path.
compile_bench_binary() {
  (cd benchmarks && cargo bench --locked --bench workload_bench --no-run --message-format=json) \
    | jq -r 'select(.reason == "compiler-artifact" and .target.name == "workload_bench") | .executable // empty' \
    | tail -n 1
}

echo "Compiling PR bench binary"
PR_BIN=$(compile_bench_binary)
[[ -n "$PR_BIN" ]] || { echo "Failed to locate PR bench binary" >&2; exit 1; }
cp "$PR_BIN" /tmp/bench-bin-changes

git fetch origin -- "$BASE_REF"
git checkout FETCH_HEAD
echo "Compiling base bench binary"
BASE_BIN=$(compile_bench_binary)
[[ -n "$BASE_BIN" ]] || { echo "Failed to locate base bench binary" >&2; exit 1; }
cp "$BASE_BIN" /tmp/bench-bin-base

# Restore the PR-head tree: the rounds below execute the stashed binaries, and
# the comment is formatted by the PR's own parse_critcmp.py (a formatter change
# is then exercised on the PR that introduces it).
git checkout "$HEAD_SHA"

# ---------------------------------------------------------------------------
# 4. Run alternating measurement rounds
#    Each round measures base then PR, saving baselines named base<round> and
#    changes<round>.
# ---------------------------------------------------------------------------
for (( round=1; round<=ROUNDS; round++ )); do
  for side in base changes; do
    echo "=== Round ${round}/${ROUNDS}: ${side} ==="
    "/tmp/bench-bin-${side}" --bench --save-baseline "${side}${round}" "${CRIT_ARGS[@]}" "$FILTER"
  done
done

# ---------------------------------------------------------------------------
# 5. Compare baselines with critcmp and format as a markdown comment
#    (summary block + collapsed per-benchmark tables; see
#    benchmarks/ci/parse_critcmp.py for the format and significance rules).
# ---------------------------------------------------------------------------
# Use `critcmp` to compare the criterion output for each round. We use `critcmp` instead of manually
# parsing criterion outputs because criterion may update its output format. By using `critcmp`, we inherit all
# updated criterion output parsing.
ROUND_FILES=()
for (( round=1; round<=ROUNDS; round++ )); do
  OUT="/tmp/critcmp-round-${round}.txt"
  critcmp --target-dir "$TARGET_DIR" "base${round}" "changes${round}" > "$OUT"
  echo "=== critcmp round ${round} ==="
  cat "$OUT"
  ROUND_FILES+=("$OUT")
done
COMPARISON=$(python3 benchmarks/ci/parse_critcmp.py "${ROUND_FILES[@]}")

# ---------------------------------------------------------------------------
# 6. Write results to /tmp/bench-comment.md
#    benchmark.yml uploads this as an artifact; benchmark-post-comment.yml
#    downloads it and posts the PR comment in base-branch context.
# ---------------------------------------------------------------------------
SHORT_SHA="${HEAD_SHA:0:7}"

# Metadata line shows commit + what fired this run + the active configuration
# and runner hardware so a reviewer can tell at a glance which setup produced
# the displayed numbers (the same comment is reused across auto-trigger and
# /bench runs).
SUMMARY="Commit: \`${SHORT_SHA}\` &middot; Trigger: ${TRIGGER:-auto-push}"
[[ -n "$TAGS" ]]   && SUMMARY+=" &middot; Tags: \`${TAGS}\`"
[[ -n "$FILTER" ]] && SUMMARY+=" &middot; Filter: \`${FILTER}\`"
SUMMARY+=" &middot; Rounds: ${ROUNDS}"
[[ ${#CRIT_ARGS[@]} -gt 0 ]] && SUMMARY+=" &middot; Criterion: \`${CRIT_ARGS[*]}\`"
CPU_MODEL=$(lscpu | sed -n 's/^Model name:[[:space:]]*//p' | head -n 1)
[[ -n "$CPU_MODEL" ]] && SUMMARY+=" &middot; Runner: ${CPU_MODEL}"

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
