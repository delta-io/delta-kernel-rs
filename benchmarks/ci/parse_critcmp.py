#!/usr/bin/env python3
"""
Parse critcmp output and format it as a GitHub-flavoured Markdown table.

Reads the output of `critcmp base changes` from stdin and writes a table
with columns: Test | Base | PR | %, where the faster side and the % cell
are bolded when the difference is statistically significant (i.e. the
error bounds do not overlap).

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

def main():
    # Expected critcmp input format (2-space-separated columns):
    #
    # group                               base                         changes
    # -----                               ----                         -------
    # bench_name                  1.00    1.2±0.01µs          1.05    1.3±0.02µs
    # bench_name/with_throughput  1.00    1.2±0.01µs  1.2 MB/s  1.05  1.3±0.02µs  1.1 MB/s
    #
    # Expected output (GitHub-flavored markdown table):
    # Throughput/bandwidth columns from critcmp are ignored; output is the same either way.
    #
    # | Test                       | Base       | PR              | %      |
    # |----------------------------|------------|-----------------|--------|
    # | bench_name                 | 1.2±0.01µs | **1.3±0.02µs**  | **+8.33%** |
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

        # N/A when a benchmark only exists in one of the two runs (added or removed).
        base_display = base_dur_str or 'N/A'
        chg_display  = chg_dur_str  or 'N/A'
        difference   = 'N/A'

        # Only compute a percentage change when both runs have a measurement for this benchmark.
        if base_dur_str and chg_dur_str:
            # Parse each duration string into (mean, error, units), e.g. "1.2±0.01µs" -> (1.2, 0.01, "µs").
            base_p = parse_duration(base_dur_str)
            chg_p  = parse_duration(chg_dur_str)
            if base_p and chg_p:
                # Normalise both measurements to milliseconds so they can be compared directly.
                base_ms     = to_ms(base_p[0], base_p[2])
                base_err_ms = to_ms(base_p[1], base_p[2])
                chg_ms      = to_ms(chg_p[0],  chg_p[2])
                chg_err_ms  = to_ms(chg_p[1],  chg_p[2])

                # Compute relative change: negative means faster, positive means slower.
                pct    = -(1 - chg_ms / base_ms) * 100
                prefix = '' if chg_ms <= base_ms else '+'
                difference = f'{prefix}{pct:.2f}%'

                # Bold the slower of the two durations to draw attention to what changed,
                # and always bold the difference column when the change is significant.
                if is_significant(chg_ms, chg_err_ms, base_ms, base_err_ms):
                    if chg_ms < base_ms:
                        chg_display = f'**{chg_dur_str}**'
                    elif chg_ms > base_ms:
                        base_display = f'**{base_dur_str}**'
                    difference = f'**{difference}**'

        print(f'| {name} | {base_display} | {chg_display} | {difference} |')

if __name__ == "__main__":
    main()
