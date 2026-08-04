# Scan Metadata Struct Columns Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add exact imperative `scan_metadata` coverage for `StatsOptions::struct_columns`, then
benchmark whether requesting a predicate column is faster than keeping it internal.

**Architecture:** Consolidate three shallow unit tests into one named `rstest` over the existing
six-file `parsed-stats` fixture. Extend the existing Criterion metadata benchmark with two otherwise
identical 300k-file scans that differ only in whether the predicate column is requested in
`stats_parsed`.

**Tech Stack:** Rust, rstest, Arrow arrays, Criterion, cargo-nextest.

## Global Constraints

- Exercise only the imperative `Scan::scan_metadata` path.
- Add tests before changing the benchmark.
- Do not assert that predicate-only statistics are absent from scan metadata.
- Assert exact selected-file counts and sorted min/max values without depending on batch order.
- Preserve at least one case whose predicate column is excluded from `struct_columns`.
- Use the existing `parsed-stats` and `300k-add-files-100-col-partitioned` fixtures.
- Preserve unrelated untracked files under `.isaac/` and `docs/superpowers/`.

---

### Task 1: Exact `scan_metadata` rstest matrix

**Files:**
- Modify: `kernel/src/scan/tests.rs:1024-1097`
- Modify: `kernel/src/scan/tests.rs:2092-2243`
- Modify: `kernel/tests/README.md:89`

**Interfaces:**
- Consumes: `StatsOptions::struct_columns(Vec<ColumnName>)`, `Scan::scan_metadata`, and the
  `parsed-stats` fixture.
- Produces: `scan_metadata_struct_columns_returns_expected_stats`, a named `rstest` whose cases
  carry requested columns, an optional predicate, expected selected-file count, a probe column,
  and expected min/max strings.

- [ ] **Step 1: Add the named cases and exact assertions**

Import `STATS_PARSED` from `crate::actions` and `array_value_to_string` from
`crate::arrow::util::display`. Replace
`test_scan_metadata_stats_columns_with_predicate`, the public-constructor arm of
`test_scan_metadata_with_specific_stats_columns`, and
`test_scan_metadata_with_multiple_stats_columns` with this matrix shape:

```rust
#[rstest]
#[case::id_without_predicate(
    vec![column_name!("id")],
    &["id"],
    None,
    "id",
    &[("1", "100"), ("101", "200"), ("201", "300"),
      ("301", "400"), ("401", "500"), ("501", "600")],
)]
#[case::id_predicate_requested(
    vec![column_name!("id")],
    &["id"],
    Some(Pred::gt(column_expr!("id"), Expr::literal(400i64))),
    "id",
    &[("401", "500"), ("501", "600")],
)]
#[case::id_predicate_not_requested(
    vec![column_name!("name")],
    &["name"],
    Some(Pred::gt(column_expr!("id"), Expr::literal(400i64))),
    "name",
    &[("name_401", "name_500"), ("name_501", "name_600")],
)]
#[case::salary_predicate_with_multiple_requested_columns(
    vec![column_name!("id"), column_name!("name")],
    &["id", "name"],
    Some(Pred::le(column_expr!("salary"), Expr::literal(70_000i64))),
    "id",
    &[("1", "100"), ("101", "200")],
)]
#[case::predicate_selects_no_files(
    vec![column_name!("salary")],
    &["salary"],
    Some(Pred::gt(column_expr!("id"), Expr::literal(600i64))),
    "salary",
    &[],
)]
fn scan_metadata_struct_columns_returns_expected_stats(
    #[case] stats_columns: Vec<ColumnName>,
    #[case] requested_names: &[&str],
    #[case] predicate: Option<Pred>,
    #[case] probe_column: &str,
    #[case] expected_min_max: &[(&str, &str)],
) {
    let path = fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
    let url = Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let predicate = predicate.map(|predicate| Arc::new(predicate) as PredicateRef);
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(StatsOptions::struct_columns(stats_columns))
        .build()
        .unwrap();

    let mut actual_min_max = Vec::new();
    for scan_metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        let (underlying_data, selection_vector) = scan_metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
            .unwrap()
            .into();
        let stats_parsed = get_column!(batch, STATS_PARSED, StructArray);
        let min_values = get_column!(stats_parsed, MIN_VALUES, StructArray);
        let max_values = get_column!(stats_parsed, MAX_VALUES, StructArray);
        let null_count = get_column!(stats_parsed, NULL_COUNT, StructArray);
        for requested in requested_names {
            assert!(min_values.column_by_name(requested).is_some(), "minValues.{requested}");
            assert!(max_values.column_by_name(requested).is_some(), "maxValues.{requested}");
            assert!(null_count.column_by_name(requested).is_some(), "nullCount.{requested}");
        }

        let filtered =
            filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();
        let stats_parsed = get_column!(filtered, STATS_PARSED, StructArray);
        let num_records = get_column!(stats_parsed, NUM_RECORDS, Int64Array);
        let min_values = get_column!(stats_parsed, MIN_VALUES, StructArray);
        let max_values = get_column!(stats_parsed, MAX_VALUES, StructArray);
        let probe_min = min_values.column_by_name(probe_column).unwrap();
        let probe_max = max_values.column_by_name(probe_column).unwrap();
        for row in 0..filtered.num_rows() {
            assert!(!stats_parsed.is_null(row));
            assert_eq!(num_records.value(row), 100);
            actual_min_max.push((
                array_value_to_string(probe_min.as_ref(), row).unwrap(),
                array_value_to_string(probe_max.as_ref(), row).unwrap(),
            ));
        }
    }

    actual_min_max.sort_unstable();
    let mut expected_min_max: Vec<_> = expected_min_max
        .iter()
        .map(|(min, max)| (min.to_string(), max.to_string()))
        .collect();
    expected_min_max.sort_unstable();
    assert_eq!(actual_min_max, expected_min_max);
}
```

Retain the private `synthesize_json: true` selected-column coverage by converting the existing
`with_json` rstest case to a single `scan_metadata_with_specific_stats_columns_and_json` test with
the same assertions. Do not compare the complete struct field list in the new matrix, because
predicate-only columns are allowed.

- [ ] **Step 2: Verify that the matrix detects incorrect pruning**

Temporarily change the `id_predicate_not_requested` predicate literal from `400i64` to `600i64` and
run:

```bash
cargo nextest run -p delta_kernel --lib --all-features \
  scan_metadata_struct_columns_returns_expected_stats::case_3_id_predicate_not_requested
```

Expected: FAIL because zero rows survive while two name min/max pairs are expected. Restore the
literal to `400i64` immediately after observing the failure.

- [ ] **Step 3: Run the completed matrix**

Run:

```bash
cargo nextest run -p delta_kernel --lib --all-features \
  scan_metadata_struct_columns_returns_expected_stats
```

Expected: all five named cases pass.

- [ ] **Step 4: Update the parsed-stats fixture catalog**

Replace the stale test list in `kernel/tests/README.md` with:

```text
scan/tests.rs::test_scan_metadata_with_stats_columns/
scan_metadata_struct_columns_returns_expected_stats/
test_build_actions_meta_predicate_with_predicate/
test_build_actions_meta_predicate_no_predicate/
test_build_actions_meta_predicate_static_skip_all/
test_skip_stats_disables_data_skipping/
test_with_stats_last_call_wins/
test_default_stats_options_no_struct_output/
scan_builder_validates_predicate_and_stats_columns/
test_scan_metadata_with_nonexistent_stats_columns
```

Format it as one inline slash-separated entry, matching the surrounding table style. Do not retain
removed test names.

- [ ] **Step 5: Format, re-run focused tests, and commit**

Run:

```bash
cargo +nightly fmt
cargo nextest run -p delta_kernel --lib --all-features \
  scan_metadata_struct_columns_returns_expected_stats
git diff --check
git add kernel/src/scan/tests.rs kernel/tests/README.md
git commit -m "test: strengthen scan metadata struct columns coverage"
```

Expected: formatting is stable, all matrix cases pass, and only the test/catalog changes are in the
commit.

### Task 2: Predicate-column Criterion comparison

**Files:**
- Modify: `kernel/benches/metadata_bench.rs`

**Interfaces:**
- Consumes: `scan_metadata_benchmark` setup, `StatsOptions::struct_columns`, `col!`, and `lit`.
- Produces: `scan_metadata_struct_columns_benchmark`, with benchmark IDs
  `predicate_column_excluded` and `predicate_column_included`.

- [ ] **Step 1: Add the focused benchmark group**

Import `column_name`, `col`, and `lit` from `delta_kernel::expressions` and `StatsOptions` from
`delta_kernel::scan`. Add:

```rust
fn scan_metadata_struct_columns_benchmark(c: &mut Criterion) {
    let (_tempdir, url, engine) = setup();
    let snapshot = Snapshot::builder_for(url)
        .build(engine.as_ref())
        .expect("Failed to create snapshot");
    let predicate = Arc::new(col!("col_0").gt(lit(0i64)));
    let mut group = c.benchmark_group("scan_metadata_struct_columns");
    group.sample_size(SCAN_METADATA_BENCH_SAMPLE_SIZE);

    for (id, stats) in [
        (
            "predicate_column_excluded",
            StatsOptions::struct_columns(vec![column_name!("col_1")]),
        ),
        (
            "predicate_column_included",
            StatsOptions::struct_columns(vec![column_name!("col_0"), column_name!("col_1")]),
        ),
    ] {
        group.bench_function(id, |b| {
            b.iter(|| {
                let scan = snapshot
                    .clone()
                    .scan_builder()
                    .with_predicate(predicate.clone())
                    .with_stats(stats.clone())
                    .without_row_transforms()
                    .build()
                    .expect("Failed to build scan");
                for result in scan
                    .scan_metadata(engine.as_ref())
                    .expect("Failed to get scan metadata")
                {
                    result.expect("Failed to process scan metadata");
                }
            })
        });
    }
    group.finish();
}
```

Add `scan_metadata_struct_columns_benchmark` to `criterion_group!`.

- [ ] **Step 2: Compile the benchmark**

Run:

```bash
cargo bench -p delta_kernel --bench metadata_bench --all-features --no-run
```

Expected: the optimized benchmark binary compiles without warnings.

- [ ] **Step 3: Measure both variants**

Run:

```bash
cargo bench -p delta_kernel --bench metadata_bench --all-features -- \
  scan_metadata_struct_columns
```

Expected: Criterion reports timing confidence intervals for both IDs. Record both intervals and
their ratio. Treat the included variant as faster only if its entire interval is below the excluded
variant's interval; otherwise treat the result as indistinguishable or slower.

- [ ] **Step 4: Apply the measured choice to cross-column cases**

If inclusion is measurably faster, add the predicate column to `stats_columns` and
`requested_names` for the salary predicate case. Keep `id_predicate_not_requested` unchanged so the
matrix continues to cover internal predicate stats. If inclusion is not measurably faster, make no
test-input change.

Re-run:

```bash
cargo nextest run -p delta_kernel --lib --all-features \
  scan_metadata_struct_columns_returns_expected_stats
```

Expected: all cases pass with the measured configuration.

- [ ] **Step 5: Format and commit the benchmark**

Run:

```bash
cargo +nightly fmt
git diff --check
git add kernel/benches/metadata_bench.rs kernel/src/scan/tests.rs
git commit -m "perf: benchmark predicate columns in scan metadata stats"
```

Expected: the benchmark and any measurement-driven test-input adjustment are committed together.

### Task 3: Final verification

**Files:**
- Verify: `kernel/src/scan/tests.rs`
- Verify: `kernel/benches/metadata_bench.rs`
- Verify: `kernel/tests/README.md`

**Interfaces:**
- Consumes: the matrix and Criterion comparison from Tasks 1 and 2.
- Produces: a clean, formatted, linted, documented, and tested change set plus benchmark results for
  the user.

- [ ] **Step 1: Run the complete delta_kernel library suite**

Run:

```bash
cargo nextest run -p delta_kernel --lib --all-features
```

Expected: all library tests pass.

- [ ] **Step 2: Run required formatting, lint, and documentation checks**

Run:

```bash
cargo +nightly fmt
cargo clippy --workspace --benches --tests --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
```

Expected: every command exits successfully without warnings.

- [ ] **Step 3: Review repository state and report**

Run:

```bash
git diff --check
git status --short
git log -3 --oneline
```

Expected: no tracked changes remain, the design/test/benchmark commits are present, and unrelated
untracked files remain untouched. Report the exact selected-file cases and both Criterion intervals
to the user.
