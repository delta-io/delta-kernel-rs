#!/usr/bin/env python3
"""
Parse per-round critcmp output and format it as a GitHub-flavoured Markdown
comment.

Each input file holds the output of `critcmp base<r> changes<r>` for one
alternating measurement round (see benchmarks/ci/run-benchmarks.sh). Writes:
  1. A summary block: counts of changes beyond the noise band, the largest
     such slowdown and speedup, and the measured noise floor (round-to-round
     spread of identical code).
  2. A `<details>` block with the per-benchmark table comparing the best
     (lowest) time per side across rounds.
  3. A `<details>` block with the raw per-round times.

A change is reported only when the ratio between the best times is at least
NOISE_BAND away from 1.0 AND the absolute difference exceeds the two sides'
combined error bars; everything else renders unmarked, as within noise.
Ratios of at least SIGNIFICANCE_THRESHOLD get the strong marker.

Usage:
    python3 benchmarks/ci/parse_critcmp.py round1.txt [round2.txt ...]
"""
import re
import statistics
import sys

# Minimum relative change that counts as beyond noise; wall clock on shared CI
# runners routinely jitters a few percent. The error bars must also separate.
NOISE_BAND = 0.05

# Ratios at least this far from 1.0 in either direction get the strong marker.
SIGNIFICANCE_THRESHOLD = 2.0

# Emoji markers for the Change cell, by side and severity. Rows within the
# noise band carry no marker.
SIGNIFICANT_SLOWDOWN = '🐌'  # ratio >= SIGNIFICANCE_THRESHOLD
SLOWDOWN = '🚧'  # beyond noise, below SIGNIFICANCE_THRESHOLD
SPEEDUP = '✅'  # beyond noise, below SIGNIFICANCE_THRESHOLD
SIGNIFICANT_SPEEDUP = '🚀'  # ratio <= 1 / SIGNIFICANCE_THRESHOLD

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

def parse_measurement(dur_str):
    """Parse a critcmp duration cell like '26.8±0.45ms'.

    Returns a dict with `ms` (point estimate), `err_ms` (error bar), and
    `display` (the original string), or None when the cell is missing or
    unparseable.
    """
    if not dur_str:
        return None
    m = re.match(r'([0-9.]+)±([0-9.]+)(.+)', dur_str.strip())
    if not m:
        return None
    unit = m.group(3).strip()
    return {
        'ms': to_ms(float(m.group(1)), unit),
        'err_ms': to_ms(float(m.group(2)), unit),
        'display': dur_str.strip(),
    }

def parse_round(lines):
    """Parse one critcmp output into {benchmark name: {'base': m, 'changes': m}}.

    Measurement values are None when a benchmark only exists in one of the two
    baselines (added or removed benchmarks).
    """
    out = {}
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
        if not name and not dur_fields:
            continue
        out[name] = {
            'base': parse_measurement(dur_fields[0] if len(dur_fields) > 0 else None),
            'changes': parse_measurement(dur_fields[1] if len(dur_fields) > 1 else None),
        }
    return out

def best(measurements):
    """Lowest-time measurement, or None when the list is empty.

    The minimum is the standard combiner for wall-clock rounds: scheduler and
    neighbor interference only ever add time, so the lowest observation is the
    closest to the machine's true speed.
    """
    return min(measurements, key=lambda m: m['ms']) if measurements else None

def spread(measurements):
    """Round-to-round relative spread (max/min - 1) for one side of one
    benchmark, or None with fewer than two rounds."""
    if len(measurements) < 2:
        return None
    lo = min(m['ms'] for m in measurements)
    hi = max(m['ms'] for m in measurements)
    return hi / lo - 1.0 if lo > 0 else None

def combine_rounds(rounds):
    """Combine per-round parses into one row per benchmark.

    Each row carries the best measurement per side, the ratio between them,
    whether the change clears the noise band and the combined error bars, the
    per-side round-to-round spreads, and the raw per-round measurements.
    """
    names = {}
    for rnd in rounds:
        for name in rnd:
            names.setdefault(name, None)

    rows = []
    for name in names:
        base_all = [r[name]['base'] for r in rounds if name in r and r[name]['base']]
        chg_all = [r[name]['changes'] for r in rounds if name in r and r[name]['changes']]
        b = best(base_all)
        c = best(chg_all)

        ratio = None
        beyond_noise = False
        if b and c and b['ms'] > 0 and c['ms'] > 0:
            ratio = c['ms'] / b['ms']
            in_band = 1.0 / (1.0 + NOISE_BAND) < ratio < 1.0 + NOISE_BAND
            separated = abs(c['ms'] - b['ms']) > (b['err_ms'] + c['err_ms'])
            beyond_noise = not in_band and separated

        spreads = [s for s in (spread(base_all), spread(chg_all)) if s is not None]
        rows.append({
            'name': name,
            'base': b,
            'chg': c,
            'ratio': ratio,
            'beyond_noise': beyond_noise,
            'spreads': spreads,
            'per_round': [
                (r.get(name, {}).get('base'), r.get(name, {}).get('changes'))
                for r in rounds
            ],
        })
    return rows

def format_difference(ratio):
    """Render a ratio as e.g. '1.00x', '1.50x slower', or '2.00x faster'."""
    if ratio is None:
        return 'N/A'
    shown = ratio if ratio >= 1.0 else 1.0 / ratio
    if f'{shown:.2f}' == '1.00':
        return '1.00x'
    side = 'slower' if ratio > 1.0 else 'faster'
    return f'{shown:.2f}x {side}'

def change_emoji(row):
    """Pick the Change-cell marker; rows within the noise band get none."""
    if not row['beyond_noise']:
        return ''
    if row['ratio'] > 1.0:
        return SIGNIFICANT_SLOWDOWN if row['ratio'] >= SIGNIFICANCE_THRESHOLD else SLOWDOWN
    return SIGNIFICANT_SPEEDUP if row['ratio'] <= 1.0 / SIGNIFICANCE_THRESHOLD else SPEEDUP

def render_summary(rows):
    """Render the summary block: beyond-noise counts with the largest change on
    each side, the within-noise count, and the measured noise floor."""
    slow = [r for r in rows if r['beyond_noise'] and r['ratio'] > 1.0]
    fast = [r for r in rows if r['beyond_noise'] and r['ratio'] < 1.0]
    measured = [r for r in rows if r['ratio'] is not None]
    within = len(measured) - len(slow) - len(fast)

    slow_line = f'- Slowdowns beyond noise: {len(slow)}'
    if slow:
        worst = max(slow, key=lambda r: r['ratio'])
        slow_line += f" (largest: `{worst['name']}`, {format_difference(worst['ratio'])})"
    fast_line = f'- Speedups beyond noise: {len(fast)}'
    if fast:
        best_row = min(fast, key=lambda r: r['ratio'])
        fast_line += f" (largest: `{best_row['name']}`, {format_difference(best_row['ratio'])})"

    all_spreads = [s for r in rows for s in r['spreads']]
    if all_spreads:
        floor_line = (
            f'- Measured noise floor (identical code, round-to-round): '
            f'median {statistics.median(all_spreads):.1%}, max {max(all_spreads):.1%}'
        )
    else:
        floor_line = '- Measured noise floor: n/a (single round)'

    return '\n'.join([
        '**Summary**',
        '',
        slow_line,
        fast_line,
        f'- Within noise: {within}',
        floor_line,
    ])

def nbsp(text):
    """Join with non-breaking spaces so a table cell renders on one line."""
    return text.strip().replace(' ', '&nbsp;')

def render_table(rows):
    """Render the per-benchmark comparison wrapped in a closed `<details>` block."""
    out = []
    out.append('<details>')
    out.append(f'<summary>Per-benchmark results ({len(rows)} rows)</summary>')
    out.append('')
    out.append(
        f'**Legend:** {SIGNIFICANT_SLOWDOWN} ≥ 2x slower &nbsp;·&nbsp;'
        f'{SLOWDOWN} beyond noise, slower &nbsp;·&nbsp;'
        f'{SPEEDUP} beyond noise, faster &nbsp;·&nbsp;'
        f'{SIGNIFICANT_SPEEDUP} ≥ 2x faster &nbsp;·&nbsp;'
        'unmarked: within noise. '
        'Base and PR cells show the best time across rounds; self-noise is the '
        'worst per-side spread between rounds of identical code.'
    )
    out.append('')
    out.append('| Test | Base | PR | Change | Self-noise |')
    out.append('|------|------|----|--------|------------|')
    for r in rows:
        name_cell = f"`{r['name']}`" if r['name'] else ''
        base_cell = r['base']['display'] if r['base'] else 'N/A'
        chg_cell = r['chg']['display'] if r['chg'] else 'N/A'
        change_cell = nbsp(f"{change_emoji(r)} {format_difference(r['ratio'])}")
        noise_cell = f"±{max(r['spreads']):.1%}" if r['spreads'] else ''
        out.append(f'| {name_cell} | {base_cell} | {chg_cell} | {change_cell} | {noise_cell} |')
    out.append('')
    out.append('</details>')
    return '\n'.join(out)

def render_rounds_table(rows, num_rounds):
    """Render the raw per-round times wrapped in a closed `<details>` block."""
    out = []
    out.append('<details>')
    out.append('<summary>Per-round raw times</summary>')
    out.append('')
    header = '| Test |'
    divider = '|------|'
    for round_idx in range(1, num_rounds + 1):
        header += f' Base r{round_idx} | PR r{round_idx} |'
        divider += '------|------|'
    out.append(header)
    out.append(divider)
    for r in rows:
        cells = [f"`{r['name']}`" if r['name'] else '']
        for base_m, chg_m in r['per_round']:
            cells.append(base_m['display'] if base_m else 'N/A')
            cells.append(chg_m['display'] if chg_m else 'N/A')
        out.append('| ' + ' | '.join(cells) + ' |')
    out.append('')
    out.append('</details>')
    return '\n'.join(out)

def main():
    paths = sys.argv[1:]
    if not paths:
        print('usage: parse_critcmp.py <critcmp-round-output>...', file=sys.stderr)
        sys.exit(2)
    rounds = []
    for path in paths:
        with open(path, encoding='utf-8') as f:
            rounds.append(parse_round(f.read().splitlines()))
    rows = combine_rounds(rounds)
    print(render_summary(rows))
    print('')
    print(render_table(rows))
    if len(rounds) > 1:
        print('')
        print(render_rounds_table(rows, len(rounds)))

if __name__ == '__main__':
    main()
