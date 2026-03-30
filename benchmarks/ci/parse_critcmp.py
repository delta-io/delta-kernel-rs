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
