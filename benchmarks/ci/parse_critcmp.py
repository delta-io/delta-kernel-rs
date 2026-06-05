#!/usr/bin/env python3
"""
Parse critcmp output and format it as a GitHub-flavoured Markdown comment.

Reads the output of `critcmp base changes` from stdin and writes:
  1. A summary block listing the largest slowdown, the fastest speedup,
     and the count of significant slowdowns/speedups.
  2. A `<details>` block (closed by default) containing the full
     per-benchmark table with columns: Test | Base | PR | Change.

The Change cell is bolded when the difference is statistically significant
(i.e. the error bounds do not overlap). The summary counts only significant
changes -- non-significant deltas are noise.

Usage:
    critcmp base changes | python3 benchmarks/ci/parse_critcmp.py
"""
import sys, re

def to_ms(value, units):
    u = units.strip()
    if u == 's':   return value * 1e3
    if u == 'ms':  return value
    if u in ('µs', 'us', 'μs'): return value / 1e3
    if u == 'ns':  return value / 1e6
    return value

def is_significant(chg_dur, chg_err, base_dur, base_err):
    """Return True if the difference between two measurements is statistically significant.

    Significance is determined by whether either center value (the `X` in `X±err`) lies outside the other's error bar.
    Concretely, if chg is faster than base, the change is significant if:
      - base's center value is above chg's entire error bar range (chg + chg_err < base), OR
      - chg's center value is below base's entire error bar range (base - base_err > chg).
    The symmetric conditions apply when chg is slower. This is an OR test: only one side
    needs to show clear separation for the difference to be considered significant.
    """
    if chg_dur < base_dur:
        return chg_dur + chg_err < base_dur or base_dur - base_err > chg_dur
    else:
        return chg_dur - chg_err > base_dur or base_dur + base_err < chg_dur

def parse_duration(s):
    m = re.match(r'([0-9.]+)±([0-9.]+)(.+)', s.strip())
    if not m:
        return None
    return float(m.group(1)), float(m.group(2)), m.group(3).strip()

def sanitize_name(raw):
    """Sanitize an attacker-controllable benchmark name for safe markdown embedding.

    Strips backticks (so the name can't break out of the code span) then escapes pipes
    (which would otherwise terminate a table cell). Returns the bare string -- the
    caller wraps in backticks where appropriate.
    """
    return raw.replace('`', '').replace('|', r'\|')

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
        raw_name = fields[0].strip() if fields else ''
        name = sanitize_name(raw_name)
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
                base_ms     = to_ms(base_p[0], base_p[2])
                base_err_ms = to_ms(base_p[1], base_p[2])
                chg_ms      = to_ms(chg_p[0],  chg_p[2])
                chg_err_ms  = to_ms(chg_p[1],  chg_p[2])

                # Float-equality on zero is safe here: to_ms only multiplies/divides
                # by powers of ten, so a zero output strictly implies a zero input.
                # Do NOT replace with an epsilon -- that would tag legitimately fast
                # benches (sub-nanosecond rounding) as N/A.
                if base_ms != 0 and chg_ms != 0:
                    ratio = chg_ms / base_ms
                    significant = is_significant(chg_ms, chg_err_ms, base_ms, base_err_ms)

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
    if abs(ratio - 1.0) < 1e-9:
        return '1.00x'
    if ratio > 1:
        return f'{ratio:.2f}x slower'
    return f'{1.0 / ratio:.2f}x faster'

def render_summary(rows):
    """Render the summary block. Counts and extrema use only statistically significant rows.

    Rows with ratio == 1.00 (or near-1) are excluded from both counts and extrema
    regardless of significance, since they are not slowdowns or speedups.
    """
    significant_slow = [r for r in rows if r['significant'] and r['ratio'] is not None and r['ratio'] > 1.0]
    significant_fast = [r for r in rows if r['significant'] and r['ratio'] is not None and r['ratio'] < 1.0]

    if significant_slow:
        worst = max(significant_slow, key=lambda r: r['ratio'])
        largest_slowdown = f"`{worst['name']}` ({format_difference(worst['ratio'])})"
    else:
        largest_slowdown = "no benchmarks slowed down"

    if significant_fast:
        best = min(significant_fast, key=lambda r: r['ratio'])
        fastest_speedup = f"`{best['name']}` ({format_difference(best['ratio'])})"
    else:
        fastest_speedup = "no benchmarks sped up"

    lines = [
        "**Summary**",
        "",
        f"- Largest slowdown: {largest_slowdown}",
        f"- Fastest speedup: {fastest_speedup}",
        f"- Benchmarks slowed down: {len(significant_slow)}",
        f"- Benchmarks sped up: {len(significant_fast)}",
    ]
    return "\n".join(lines)

def render_table(rows):
    """Render the per-benchmark table wrapped in a closed-by-default <details> block."""
    out = []
    out.append(f"<details>")
    out.append(f"<summary>Per-benchmark results ({len(rows)} rows)</summary>")
    out.append("")
    out.append("| Test | Base         | PR               | Change |")
    out.append("|------|--------------|------------------|--------|")
    for r in rows:
        name_cell = f"`{r['name']}`" if r['name'] else ''
        difference = format_difference(r['ratio'])
        if r['significant'] and r['ratio'] is not None and abs(r['ratio'] - 1.0) >= 1e-9:
            difference = f"**{difference}**"
        out.append(f"| {name_cell} | {r['base_display']} | {r['chg_display']} | {difference} |")
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
