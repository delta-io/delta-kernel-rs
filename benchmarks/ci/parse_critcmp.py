#!/usr/bin/env python3
"""
Parse critcmp output and format it as a GitHub-flavoured Markdown comment.

Reads the output of `critcmp base changes` from stdin and writes:
  1. The comment's header line, ending with a pass/fail verdict that mirrors
     the regression-gate outcome (BENCH_IGNORE_FAILURE=true renders the
     label-override variant).
  2. A one-line summary of how many benchmarks fell into each tier, ordered
     fastest to slowest.
  3. A `<details>` block (closed by default) containing the full
     per-benchmark table with columns: Test | Change | Base | PR.
  4. The tier legend as a small-text footer line.

Each benchmark is bucketed by its displayed PR/base multiplier into a tier with
a marker emoji (see the threshold constants). When any benchmark lands in the
slowest tier, the run records a regression in the file named by
BENCH_REGRESSION_FILE (if set) so the workflow can fail the job.

When a non-overridden regression is below the automatic retry threshold, the
run records that it is retryable in BENCH_RETRY_FILE (if set). Thresholds use
the displayed two-decimal multiplier so the retry decision matches the comment.

Usage:
    critcmp base changes | python3 benchmarks/ci/parse_critcmp.py
"""
import os
import re
import sys

# Tiers compare the 2-decimal multiplier the table displays ("1.15x slower",
# "1.15x faster"), so a row's marker always matches its Change cell.
ROCKET_THRESHOLD = 1.15
GRAY_THRESHOLD = 1.03
FAIL_THRESHOLD = 1.15
RETRY_THRESHOLD = 1.50

# |ratio - 1| within this renders as "1.00x" with no faster/slower suffix
# (half of the 2-decimal display step).
NEUTRAL_THRESHOLD = 0.005

# Tier markers for the Change cell, fastest to slowest.
ROCKET = '🚀'         # at least 1.15x faster
GREEN_CHECK = '✅'    # faster or unchanged
GRAY_CHECK = '☑️'     # at most 1.03x slower
CONSTRUCTION = '🚧'   # between 1.03x and 1.15x slower
RED_X = '❌'          # at least 1.15x slower (fails the job)

# Fastest to slowest; drives both the summary order and the legend.
TIERS = [ROCKET, GREEN_CHECK, GRAY_CHECK, CONSTRUCTION, RED_X]

def to_ms(value, units):
    """Convert a critcmp duration to milliseconds.

    Matches exactly the units critcmp can emit (see `time()` in critcmp's
    output.rs): ns, µs (U+00B5), ms, s. An unrecognized unit means the critcmp
    output format changed; raise so the run fails loudly instead of rendering a
    silently wrong table.
    """
    u = units.strip()
    if u == 's':
        return value * 1e3
    if u == 'ms':
        return value
    if u == 'µs':
        return value / 1e3
    if u == 'ns':
        return value / 1e6
    raise ValueError(f'unrecognized critcmp time unit: {units!r}')

def parse_duration(s):
    m = re.match(r'([0-9.]+)±([0-9.]+)(.+)', s.strip())
    if not m:
        return None
    return float(m.group(1)), float(m.group(2)), m.group(3).strip()

def parse_rows(lines):
    """Parse critcmp stdout into a list of row dicts.

    Each row dict contains:
      name:         sanitized benchmark name (no backticks/pipes), unwrapped
      base_display: base duration string or 'N/A'
      chg_display:  changes duration string or 'N/A'
      ratio:        chg_ms / base_ms, or None if either side is missing/zero
    """
    rows = []
    for line in lines[2:]:  # skip critcmp header rows
        if not line.strip():
            continue
        # critcmp columns (split on 2+ spaces):
        #   with throughput:    name, baseFactor, baseDuration, baseBandwidth, changesFactor, changesDuration, changesBandwidth
        #   without throughput: name, baseFactor, baseDuration, changesFactor, changesDuration
        # Locate duration fields by the presence of "±" rather than hardcoding indices,
        # so the script works correctly regardless of whether bandwidth columns are present.
        fields = re.split(r'  +', line)
        name = fields[0].strip() if fields else ''
        dur_fields = [f.strip() for f in fields[1:] if '±' in f]
        base_dur_str = dur_fields[0] if len(dur_fields) > 0 else None
        chg_dur_str  = dur_fields[1] if len(dur_fields) > 1 else None

        if not name and not base_dur_str and not chg_dur_str:
            continue

        # N/A when a benchmark only exists in one of the two runs (added or removed).
        base_display = base_dur_str or 'N/A'
        chg_display  = chg_dur_str  or 'N/A'
        ratio = None

        if base_dur_str and chg_dur_str:
            base_p = parse_duration(base_dur_str)
            chg_p  = parse_duration(chg_dur_str)
            if base_p and chg_p:
                base_ms = to_ms(base_p[0], base_p[2])
                chg_ms  = to_ms(chg_p[0],  chg_p[2])

                # Float-equality on zero is safe here: to_ms only multiplies/divides
                # by powers of ten, so a zero output strictly implies a zero input.
                # Do NOT replace with an epsilon -- that would tag legitimately fast
                # benches (sub-nanosecond rounding) as N/A.
                if base_ms != 0 and chg_ms != 0:
                    ratio = chg_ms / base_ms

        rows.append({
            'name': name,
            'base_display': base_display,
            'chg_display': chg_display,
            'ratio': ratio,
        })
    return rows

def format_difference(ratio):
    """Render a ratio as e.g. '1.00x', '1.50x slower', or '2.00x faster'."""
    if ratio is None:
        return 'N/A'
    if abs(ratio - 1.0) < NEUTRAL_THRESHOLD:
        return '1.00x'
    if ratio > 1:
        return f'{ratio:.2f}x slower'
    return f'{1.0 / ratio:.2f}x faster'

def change_emoji(ratio):
    """Pick the tier marker for the Change cell from the displayed 2-decimal
    multiplier. Empty string for N/A rows."""
    if ratio is None:
        return ''
    if ratio < 1.0:
        return ROCKET if round(1.0 / ratio, 2) >= ROCKET_THRESHOLD else GREEN_CHECK
    shown = round(ratio, 2)
    if shown <= 1.0:
        return GREEN_CHECK
    if shown <= GRAY_THRESHOLD:
        return GRAY_CHECK
    if shown < FAIL_THRESHOLD:
        return CONSTRUCTION
    return RED_X

def render_verdict(regressed, ignored):
    """Render the pass/fail verdict shown in the comment's header line,
    mirroring the job's regression-gate outcome."""
    mult = f'{FAIL_THRESHOLD:.2f}x'
    if not regressed:
        return '✅ Pass'
    if ignored:
        return f'⚠️ Pass (≥{mult} slowdown ignored)'
    return f'❌ Fail (a benchmark is ≥{mult} slower)'

def render_summary(rows):
    """Render the per-tier counts on one line, fastest to slowest. N/A rows
    (added/removed benchmarks) are appended only when present."""
    counts = {tier: 0 for tier in TIERS}
    na = 0
    for r in rows:
        emoji = change_emoji(r['ratio'])
        if emoji:
            counts[emoji] += 1
        else:
            na += 1
    parts = [f"{tier} {counts[tier]}" for tier in TIERS]
    if na:
        parts.append(f"N/A {na}")
    return "**Summary:** " + " &nbsp;·&nbsp; ".join(parts)

def render_legend():
    """Render the tier legend as a small-text line for the comment footer."""
    fail = f'{FAIL_THRESHOLD:.2f}x'
    gray = f'{GRAY_THRESHOLD:.2f}x'
    rocket = f'{ROCKET_THRESHOLD:.2f}x'
    return (
        f"<sub>Legend: {ROCKET} ≥{rocket} faster &nbsp;·&nbsp;"
        f"{GREEN_CHECK} faster or unchanged &nbsp;·&nbsp;"
        f"{GRAY_CHECK} ≤{gray} slower &nbsp;·&nbsp;"
        f"{CONSTRUCTION} {gray}-{fail} slower &nbsp;·&nbsp;"
        f"{RED_X} ≥{fail} slower</sub>"
    )

def render_table(rows):
    """Render the per-benchmark table wrapped in a closed-by-default <details> block."""
    out = []
    out.append("<details>")
    out.append(f"<summary>Per-benchmark results ({len(rows)} rows)</summary>")
    out.append("")
    out.append("| Test | Change | Base         | PR               |")
    out.append("|------|--------|--------------|------------------|")
    for r in rows:
        name_cell = f"`{r['name']}`" if r['name'] else ''
        difference = format_difference(r['ratio'])
        emoji = change_emoji(r['ratio'])
        # Non-breaking spaces keep the marker and ratio on one line so the
        # Change cell renders without wrapping.
        change_cell = f"{emoji} {difference}".strip().replace(" ", "&nbsp;")
        out.append(f"| {name_cell} | {change_cell} | {r['base_display']} | {r['chg_display']} |")
    out.append("")
    out.append("</details>")
    return "\n".join(out)

def main():
    lines = sys.stdin.read().splitlines()
    rows = parse_rows(lines)
    regressed = any(change_emoji(r['ratio']) == RED_X for r in rows)
    ignored = os.environ.get('BENCH_IGNORE_FAILURE') == 'true'
    max_slowdown = max(
        (round(r['ratio'], 2) for r in rows if r['ratio'] is not None and r['ratio'] > 1),
        default=1.0,
    )
    retryable = regressed and not ignored and max_slowdown < RETRY_THRESHOLD

    print(f'## Benchmark results: {render_verdict(regressed, ignored)}')
    print("")
    print(render_summary(rows))
    print("")
    print(render_table(rows))
    print("")
    print(render_legend())

    # Record whether any benchmark crossed FAIL_THRESHOLD so the workflow can
    # fail the job (unless overridden by the ignore-benchmark-failure label).
    flag_path = os.environ.get('BENCH_REGRESSION_FILE')
    if flag_path:
        with open(flag_path, 'w') as f:
            f.write('true' if regressed else 'false')

    retry_path = os.environ.get('BENCH_RETRY_FILE')
    if retry_path:
        with open(retry_path, 'w') as f:
            f.write('true' if retryable else 'false')

if __name__ == "__main__":
    main()
