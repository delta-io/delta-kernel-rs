#!/usr/bin/env python3
"""
Parse critcmp output and format it as a GitHub-flavoured Markdown comment.

Reads the output of `critcmp base changes` from stdin and writes:
  1. A summary block listing the largest slowdown, the fastest speedup,
     and the count of significant slowdowns/speedups.
  2. A `<details>` block (closed by default) containing the full
     per-benchmark table with columns: Test | Base | PR | Change.

A change is "significant" when it is at least 2x in either direction
(ratio >= 2.0 for slowdown, ratio <= 0.5 for speedup). The summary includes
every slowdown and speedup; significant ones get a 🐌/🚀 marker in the
Change cell.

Usage:
    critcmp base changes | python3 benchmarks/ci/parse_critcmp.py
"""
import re
import sys

# Significance threshold for slowdowns/speedups (2x in either direction).
SIGNIFICANCE_THRESHOLD = 2.0

# Ratios within this of 1.0 count as no change: rendered "1.00x" with no marker
# and excluded from the summary's slowdown/speedup counts.
NEUTRAL_THRESHOLD = 1e-3

# Emoji markers for the Change cell, by side and severity.
SIGNIFICANT_SLOWDOWN = '🐌'  # ratio >= SIGNIFICANCE_THRESHOLD
SLIGHT_SLOWDOWN = '🚧'  # 1.0 < ratio < SIGNIFICANCE_THRESHOLD
SLIGHT_SPEEDUP = '✅'  # 1.0 / SIGNIFICANCE_THRESHOLD < ratio < 1.0
SIGNIFICANT_SPEEDUP = '🚀'  # ratio <= 1.0 / SIGNIFICANCE_THRESHOLD

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
      significant:  bool, False if ratio is None
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
        significant = False

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
                    significant = (
                        ratio >= SIGNIFICANCE_THRESHOLD
                        or ratio <= 1.0 / SIGNIFICANCE_THRESHOLD
                    )

        rows.append({
            'name': name,
            'base_display': base_display,
            'chg_display': chg_display,
            'ratio': ratio,
            'significant': significant,
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

def change_emoji(ratio, significant):
    """Pick an emoji indicator for the Change cell.

    See the module-level emoji constants for the marker assignments. Returns
    an empty string for ratios near 1.0 or N/A rows.
    """
    if ratio is None or abs(ratio - 1.0) < NEUTRAL_THRESHOLD:
        return ''
    if ratio > 1.0:
        return SIGNIFICANT_SLOWDOWN if significant else SLIGHT_SLOWDOWN
    return SIGNIFICANT_SPEEDUP if significant else SLIGHT_SPEEDUP

def render_summary(rows):
    """Render the summary block. The counts and the largest-slowdown/fastest-speedup
    lines include every slowdown and speedup regardless of the 2x significance
    threshold; the per-row emoji marker is what distinguishes significant from
    slight changes.

    Rows within NEUTRAL_THRESHOLD of 1.0 are excluded since they are neither slowdowns
    nor speedups. N/A rows (ratio is None) are excluded for the same reason.
    """
    slow = [r for r in rows if r['ratio'] is not None and r['ratio'] - 1.0 >= NEUTRAL_THRESHOLD]
    fast = [r for r in rows if r['ratio'] is not None and 1.0 - r['ratio'] >= NEUTRAL_THRESHOLD]

    if slow:
        worst = max(slow, key=lambda r: r['ratio'])
        largest_slowdown = f"`{worst['name']}` ({format_difference(worst['ratio'])})"
    else:
        largest_slowdown = "no benchmarks slowed down"

    if fast:
        best = min(fast, key=lambda r: r['ratio'])
        fastest_speedup = f"`{best['name']}` ({format_difference(best['ratio'])})"
    else:
        fastest_speedup = "no benchmarks sped up"

    lines = [
        "**Summary**",
        "",
        f"- Largest slowdown: {largest_slowdown}",
        f"- Fastest speedup: {fastest_speedup}",
        f"- Benchmarks slowed down: {len(slow)}",
        f"- Benchmarks sped up: {len(fast)}",
    ]
    return "\n".join(lines)

def render_table(rows):
    """Render the per-benchmark table wrapped in a closed-by-default <details> block."""
    out = []
    out.append("<details>")
    out.append(f"<summary>Per-benchmark results ({len(rows)} rows)</summary>")
    out.append("")
    out.append(
        f"**Legend:** {SIGNIFICANT_SLOWDOWN} ≥ 2x slower &nbsp;·&nbsp;"
        f"{SLIGHT_SLOWDOWN} < 2x slower &nbsp;·&nbsp;"
        f"{SLIGHT_SPEEDUP} < 2x faster &nbsp;·&nbsp;"
        f"{SIGNIFICANT_SPEEDUP} ≥ 2x faster"
    )
    out.append("")
    out.append("| Test | Base         | PR               | Change |")
    out.append("|------|--------------|------------------|--------|")
    for r in rows:
        name_cell = f"`{r['name']}`" if r['name'] else ''
        difference = format_difference(r['ratio'])
        emoji = change_emoji(r['ratio'], r['significant'])
        # Non-breaking spaces keep the marker and ratio on one line so the
        # Change cell renders without wrapping.
        change_cell = f"{emoji} {difference}".strip().replace(" ", "&nbsp;")
        out.append(f"| {name_cell} | {r['base_display']} | {r['chg_display']} | {change_cell} |")
    out.append("")
    out.append("</details>")
    return "\n".join(out)

def main():
    lines = sys.stdin.read().splitlines()
    rows = parse_rows(lines)
    print(render_summary(rows))
    print("")
    print(render_table(rows))

if __name__ == "__main__":
    main()
