# Scan Metadata Struct Columns Test Design

## Goal

Strengthen imperative `Scan::scan_metadata` coverage for
`StatsOptions::struct_columns` by exercising requested statistics columns and data-skipping
predicates together. The tests must verify exact pruning outcomes and meaningful returned
statistics rather than only checking that `stats_parsed` exists.

## Scope

This change covers the imperative `scan_metadata` path using the existing `parsed-stats` fixture
and adds a Criterion comparison to the existing metadata benchmark. Declarative metadata plans,
parallel scans, and production behavior are out of scope. Existing overlapping imperative tests
may be consolidated into the new matrix.

The fixture has six files of 100 rows. Its `id` and `salary` statistics form non-overlapping ranges,
which makes exact data-skipping outcomes deterministic. Assertions must not depend on scan output
order.

## Test Matrix

Use one named `rstest` whose cases provide:

- the columns passed to `StatsOptions::struct_columns`;
- an optional predicate;
- the expected number of selected files;
- a requested column whose returned min/max values are checked;
- the exact sorted min/max pairs expected from selected files.

Include cases covering:

- a single requested column without a predicate;
- a predicate on the same column as the requested statistics;
- a predicate on a different column from the requested statistics;
- multiple requested statistics columns with a predicate on another column;
- a predicate that selects no files.

The cross-column cases verify that Kernel reads the predicate's statistics even when that column is
not in `struct_columns`. Predicate-only statistics are allowed to appear in the returned struct;
their presence or absence is not part of this test's contract.

## Benchmark

Extend `metadata_bench` with a focused `scan_metadata` comparison over its existing 300k-file,
100-column fixture. Both variants use `col_0 > 0` and request statistics for `col_1`:

- predicate column excluded: `struct_columns([col_1])`;
- predicate column included: `struct_columns([col_0, col_1])`.

Build and consume a fresh scan in each iteration, matching the existing benchmark's measurement
boundary. Call `without_row_transforms()` in both variants to isolate statistics processing from
partition transform construction. Both variants must otherwise use the same snapshot, engine, and
predicate so the requested output schema is the only intentional difference.

Run Criterion long enough to report confidence intervals. If including `col_0` is measurably
faster, cross-column test cases should include their predicate columns in `struct_columns` where
doing so does not undermine the behavior being tested. If the result is indistinguishable or
slower, keep predicate columns excluded. Preserve at least one test case with the predicate column
excluded, because that behavior is the primary correctness gap identified by the survey.

## Assertions

For every emitted metadata batch, apply its selection vector before checking returned rows. Assert:

- the total selected-file count exactly matches the case;
- every requested column exists under `minValues`, `maxValues`, and `nullCount`;
- each selected row has a non-null `stats_parsed` value and `numRecords == 100`;
- the selected rows' sorted min/max pairs for the case's probe column exactly match expectations.

Inspect the unfiltered schema as well, so the no-match case still verifies that requested columns
were projected even though its selection vector contains no surviving rows. Avoid asserting on
predicate-only output columns or batch/file ordering.

## Consolidation and Verification

Replace the existing shallow predicate, single-column, and multiple-column tests where the matrix
subsumes their public-constructor coverage. Keep unrelated JSON-synthesis, validation, checkpoint,
and all-struct tests intact.

Run the focused matrix, the remaining scan-metadata stats tests, the Criterion comparison,
formatting, and the `delta_kernel` library test suite with all features.
