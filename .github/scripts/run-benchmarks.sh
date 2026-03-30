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
  # Parse --tags and --filter flags; each takes the next whitespace-delimited
  # token as its value. Unknown tokens are silently ignored.
  while [[ -n "$ARGS" ]]; do
    TOKEN="${ARGS%% *}"
    ARGS="${ARGS#"$TOKEN"}"
    ARGS="${ARGS##+( )}"

    if [[ "$TOKEN" == "--tags" ]]; then
      TAGS="${ARGS%% *}"
      ARGS="${ARGS#"$TAGS"}"
      ARGS="${ARGS##+( )}"
    elif [[ "$TOKEN" == "--filter" ]]; then
      FILTER="${ARGS%% *}"
      ARGS="${ARGS#"$FILTER"}"
      ARGS="${ARGS##+( )}"
    fi
  done
fi

# Sanitize tags: strict allowlist (alphanumeric, comma, underscore, hyphen)
TAGS=$(printf '%s' "$TAGS" | tr -cd 'a-zA-Z0-9,_-')

# Sanitize filter: strip control characters only, preserving regex metacharacters.
# The filter is always passed double-quoted to cargo bench.
FILTER=$(printf '%s' "$FILTER" | tr -d '\000-\037\177')

# If nothing was parsed (unrecognized tokens, typos, missing values), default to "base"
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
#    Replicates criterion-compare-action's output:
#      - Parses actual duration values (not rank factors) for the % column
#      - Bolds the faster duration and % cell when the difference is
#        statistically significant (error bounds do not overlap)
# ---------------------------------------------------------------------------
cat > /tmp/parse_critcmp.py << 'PYEOF'
import sys, re

def to_seconds(value, units):
    u = units.strip()
    if u == 's':   return value
    if u == 'ms':  return value / 1e3
    if u in ('µs', 'us', 'μs'): return value / 1e6
    if u == 'ns':  return value / 1e9
    return value

def is_significant(chg_dur, chg_err, base_dur, base_err):
    if chg_dur < base_dur:
        return chg_dur + chg_err < base_dur or base_dur - base_err > chg_dur
    else:
        return chg_dur - chg_err > base_dur or base_dur + base_err < chg_dur

def parse_duration(s):
    m = re.match(r'([0-9.]+)±([0-9.]+)(.+)', s.strip())
    if not m:
        return None
    return float(m.group(1)), float(m.group(2)), m.group(3).strip()

lines = sys.stdin.read().splitlines()
print("| Test | Base         | PR               | % |")
print("|------|--------------|------------------|---|")

for line in lines[2:]:  # skip critcmp header rows
    if not line.strip():
        continue
    # critcmp columns (split on 2+ spaces):
    #   with throughput:    name, baseFactor, baseDuration, baseBandwidth, changesFactor, changesDuration, changesBandwidth
    #   without throughput: name, baseFactor, baseDuration, changesFactor, changesDuration
    # Locate duration fields by the presence of "±" rather than hardcoding indices,
    # so the script works correctly regardless of whether bandwidth columns are present.
    fields = re.split(r'  +', line)
    name = fields[0].strip().replace('|', r'\|') if fields else ''
    dur_fields = [f.strip() for f in fields[1:] if '±' in f]
    base_dur_str = dur_fields[0] if len(dur_fields) > 0 else None
    chg_dur_str  = dur_fields[1] if len(dur_fields) > 1 else None

    if not name and not base_dur_str and not chg_dur_str:
        continue

    base_display = base_dur_str or 'N/A'
    chg_display  = chg_dur_str  or 'N/A'
    difference   = 'N/A'

    if base_dur_str and chg_dur_str:
        base_p = parse_duration(base_dur_str)
        chg_p  = parse_duration(chg_dur_str)
        if base_p and chg_p:
            base_secs     = to_seconds(base_p[0], base_p[2])
            base_err_secs = to_seconds(base_p[1], base_p[2])
            chg_secs      = to_seconds(chg_p[0],  chg_p[2])
            chg_err_secs  = to_seconds(chg_p[1],  chg_p[2])

            pct    = -(1 - chg_secs / base_secs) * 100
            prefix = '' if chg_secs <= base_secs else '+'
            difference = f'{prefix}{pct:.2f}%'

            if is_significant(chg_secs, chg_err_secs, base_secs, base_err_secs):
                if chg_secs < base_secs:
                    chg_display = f'**{chg_dur_str}**'
                elif chg_secs > base_secs:
                    base_display = f'**{base_dur_str}**'
                difference = f'**{difference}**'

    print(f'| {name} | {base_display} | {chg_display} | {difference} |')
PYEOF

COMPARISON=$((cd benchmarks && critcmp base changes) | python3 /tmp/parse_critcmp.py)

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
