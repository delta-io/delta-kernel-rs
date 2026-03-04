# Snapshot Loading: Java vs Rust Case Comparison

This document maps every case in Java's `SnapshotManager.getStartCheckpointVersion` to its
equivalent in Rust's `LogSegment::for_snapshot_impl`, and explains where the two diverge and why.

---

## Java (`getStartCheckpointVersion`)

Java determines the start checkpoint version in one function, then does a single forward listing
from that version. The logic is:

```
if time-travel version present:
    → ALWAYS backward-scan from (travel_version + 1)      [cases 5a/5b/5c below]
else (latest query):
    read _last_checkpoint hint
    if no hint:
        → list from version 0                              [case 1]
    if hint present, no maxCatalogVersion:
        → use hint directly                                [case 2]
    if hint present, hint <= maxCatalogVersion:
        → use hint directly                                [case 3]
    if hint present, hint > maxCatalogVersion (race):
        → backward-scan from maxCatalogVersion             [case 4]
```

Key point: **for time-travel, Java never looks at `_last_checkpoint` at all.** It always
backward-scans, regardless of whether a hint exists.

---

## Rust (`for_snapshot_impl` match arms)

Rust reads `_last_checkpoint` upfront and then dispatches on `(checkpoint_hint, time_travel_version)`:

| Arm | Condition | Action |
|-----|-----------|--------|
| `(Some(cp), None)` | Latest query, hint present | Use hint → `list_with_checkpoint_hint` |
| `(Some(cp), Some(tv)) if cp.version <= tv` | Time-travel, hint valid | Use hint → `list_with_checkpoint_hint` (**Rust optimization**) |
| `(_, Some(tv))` | Time-travel, hint absent or stale (hint > tv) | Backward-scan from `tv + 1` |
| `_ (None, None)` | Latest query, no hint | List from version 0 |

---

## Case-by-Case Mapping

| # | Java condition | Java action | Rust arm | Notes |
|---|----------------|-------------|----------|-------|
| 1 | Latest, no hint | List from 0 | `(None, None)` | Identical |
| 2 | Latest, hint present, no catalog | Use hint | `(Some(cp), None)` | Identical |
| 3 | Latest, hint present, hint ≤ maxCatalogVersion | Use hint | `(Some(cp), None)` | **Gap:** Rust doesn't check hint vs catalog version |
| 4 | Latest, hint present, hint > maxCatalogVersion (race condition) | Backward-scan from maxCatalogVersion | Excluded | Catalog support not yet implemented |
| 5a | Time-travel, hint present, hint ≤ travel version | Backward-scan from travel version | `(Some(cp), Some(tv)) if cp.version <= tv` → `list_with_checkpoint_hint` | **Divergence:** see below |
| 5b | Time-travel, hint present, hint > travel version | Backward-scan from travel version | `(_, Some(tv))` | Identical outcome |
| 5c | Time-travel, no hint | Backward-scan from travel version | `(_, Some(tv))` | Identical |

---

## The Divergence: Case 5a

This is the only case where Rust and Java take genuinely different paths.

**Java** doesn't even check the hint for time-travel — it goes straight to `findLastCompleteCheckpointBefore(travel_version + 1)`.

**Rust** has an optimization: if the hint version is ≤ the travel version, the hint is likely
still valid, so Rust uses it to skip the backward scan. Instead of scanning backwards from
`travel_version`, it calls `list_with_checkpoint_hint` which lists forward from the hint version.

This is a performance win in the normal case: if `_last_checkpoint` says v50 and we're
time-traveling to v60, there's no need to scan backwards — just list from v50 to v60.

### The failure mode

`list_with_checkpoint_hint` lists forward from the hint version and then checks whether a complete
checkpoint actually appeared in the results. If it didn't (the checkpoint file was vacuumed or
never written to storage), it currently errors.

Java never hits this because it backward-scans from the travel version directly — it finds
whatever checkpoint actually exists, not what the hint claims exists.

### Proposed fix

When `list_with_checkpoint_hint` finds no checkpoint and `end_version` is `Some` (time-travel),
fall back to `find_last_checkpoint_before(travel_version + 1)` instead of erroring. This makes
Rust match Java's outcome for this case: graceful recovery to the nearest valid checkpoint before
the travel version.

Latest queries (no `end_version`) keep the existing error — there's no upper bound to scan
backwards from when loading the latest snapshot.

---

## `list_with_checkpoint_hint` internal behaviour

When called, `list_with_checkpoint_hint`:

1. Lists all files from `[hint_version, end_version]`
2. Checks whether a complete checkpoint appeared:
   - **No checkpoint found:** currently errors; proposed fix is to backward-scan if `end_version` is Some
   - **Checkpoint found at a different (newer) version than the hint:** hint was stale, log a
     warning and proceed with the newer checkpoint (same behaviour as Java's Step 6 in
     `SnapshotManager`)
   - **Checkpoint found at hint version, wrong part count:** hard error (broken table)
   - **Checkpoint found at hint version, correct:** success
