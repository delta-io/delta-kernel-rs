# DAT read testing improvements

Ryan Liao's DAT migration and Kernel acceptance-harness work modernizes workload generation,
expands runnable read coverage, and makes failures easier to diagnose. The work spans the DAT
repository, the published `v0.0.7-preview` corpus, and seven Kernel PRs.

## Workload generation and release

The legacy reader corpus had 19 scenarios maintained through a separate Python generator. Ryan
reviewed their coverage in the modern Scala suites and added the missing scenarios: checkpoint
reads after prior commits are removed, struct-only statistics, disabled statistics, and
Iceberg compatibility v1. Existing modern suites supplied much of the other coverage.

[DAT PR #62](https://github.com/delta-incubator/dat/pull/62) merged this migration and removed the
legacy Python generator, its PySpark tests, Python-only dependencies, and obsolete CI commands.

These are the four gaps filled in the modern generator. Each workload has both a read spec and a
snapshot spec in the v7 corpus:

| Workload | Concrete scenario | Coverage gained |
|---|---|---|
| `checkpoints_prior_commits_removed` | A checkpoint is available, but earlier JSON commits are removed. | Recover the table from the retained checkpoint and log history. The read expects one row. |
| `checkpoints_struct_stats_only` | `delta.checkpoint.writeStatsAsJson=false`, `writeStatsAsStruct=true`. | Read checkpointed tables that store statistics only as structs. The read expects three rows. |
| `checkpoints_stats_disabled` | Both checkpoint statistics formats are disabled, with `delta.dataSkippingNumIndexedCols=0`. | Read correctly without relying on optional file statistics. The read expects one row. |
| `protocol_versions_iceberg_compat_v1` | An Iceberg-compatible table uses name-based column mapping and the `icebergCompatV1` writer feature. | Check the protocol and metadata, and read two rows through logical column names. |

The release work generated the new acceptance corpus through the runtime workload generator and
published [v0.0.7-preview](https://github.com/delta-incubator/dat/releases/tag/v0.0.7-preview).
The release packages both acceptance and benchmark archives. The benchmark archive reuses the
existing hand-generated workloads; repackaging it does not introduce new benchmark scenarios.

## Kernel read-harness improvements

| Improvement | What it enables or catches | PR |
|---|---|---|
| Typed predicate literals | Numeric casts, decimal and scientific-notation literals, and empty string/binary literals can be translated into Kernel expressions. | [#3450](https://github.com/delta-io/delta-kernel-rs/pull/3450) |
| Expected-data alignment | Equivalent Spark/Parquet timestamp representations and omitted all-null `VOID` fields can be compared with Kernel output, including inside structs, lists, and maps. | [#3451](https://github.com/delta-io/delta-kernel-rs/pull/3451) |
| Meaningful error validation | Expected-error tests check supported error categories instead of passing on any failure. Snapshot tests also check requested versions and compare protocol feature lists without depending on list order. | [#3458](https://github.com/delta-io/delta-kernel-rs/pull/3458) |
| Filter-only columns | Reads can filter on a column omitted from the final output. The harness requests it temporarily, filters rows, and then drops it. | [#3459](https://github.com/delta-io/delta-kernel-rs/pull/3459) |
| Spec-aware loading | The harness reads each spec file once, classifies its operation, and distinguishes unsupported operations from malformed supported specs. | [#3452](https://github.com/delta-io/delta-kernel-rs/pull/3452) |
| Auditable failure inventory and v7 adoption | Known failures match exact corpus-relative spec IDs and required error text, with explanations and issue references. Corpus extraction uses the pinned checksum to identify the extracted release. | [#3454](https://github.com/delta-io/delta-kernel-rs/pull/3454) |
| Legacy runner retirement | Removes the old Kernel DAT runner and unused support code while retaining v0.0.3 table fixtures needed by FFI tests. | [#3455](https://github.com/delta-io/delta-kernel-rs/pull/3455) |

For example, `SELECT name FROM people WHERE age > 18` follows this path:

```text
Workload output columns:     [name]
Columns requested of Kernel: [name, age]
Kernel output columns:      [name, age]
Harness:                    filter on age, then drop age
Final output columns:       [name]
```

Kernel preserves the scan's requested schema. The extra projection removes temporary predicate
columns. Exact row filtering is necessary because Kernel's predicate filtering permits extra
candidate rows.

Expected-data alignment is deliberately limited. Timestamp conversion must preserve precision;
other type differences and unexpected field ordering still fail. Decimal conversion uses
`BigDecimal` and rejects values that require rounding or exceed the target precision. Error
validation also gains the public `KernelError::without_backtrace()` helper.

## Concrete read cases enabled by the stack

Some cases already existed but were marked as expected failures because the harness could not
parse or compare them correctly. Others gain stronger validation. The examples below cover each
kind of improvement, including the implemented PRs awaiting merge. Corpus examples use v7 names;
historical names are identified explicitly. They describe supported behavior, not a new claim that
every related workload passes.

### Numeric predicates and empty values: #3450, merged

| Case | Example | Before -> updated behavior |
|---|---|---|
| Byte boundary and positive filters | `partition_values_byte_filter_max`: `b = CAST(127 AS BYTE)`; minimum `-128`; positive `b > CAST(0 AS BYTE)`. | Unsupported casts -> typed byte comparisons. Max/min expect one row; positive expects two. |
| Short boundary and range filters | `partition_values_short_filter_range`: `s >= CAST(0 AS SHORT) AND s <= CAST(100 AS SHORT)`; boundaries `-32768` and `32767`. | Unsupported casts -> typed short comparisons. Range expects two rows; boundaries expect one each. |
| Float equality and positive filters | `partition_values_float_filter_eq`: `f = CAST(1.5 AS FLOAT)`; `f > CAST(0.0 AS FLOAT)`. | Unsupported casts -> typed float comparisons, with one and two expected rows respectively. |
| Decimal predicates | `reads_decimal_read_price_gt_100`: `price > 100` on `DECIMAL(10,2)`. | Literal scale mismatch -> exact decimal scalar construction. This spec expects one row. |
| Decimal precision and notation | Parser tests use `price > 1.5`, `price > 1e2`, `price = 1000e-3`, and `price = -100`. | Handle equivalent decimal spellings without rounding; reject excess precision and oversized exponents. |
| Integer/long literal casts | Parser tests use `CAST(2147483647 AS INT)` and `CAST(9223372036854775807 AS BIGINT)`. | Recognize these literal types and reject out-of-range values. |
| Empty strings and binary literals | `partition_values_empty_string_filter_empty_string`: `tag = ''`; parser coverage also checks empty binary literals. | Construct valid empty literals rather than failing conversion. Empty-string partition behavior is checked against the generated expected answer. |

Historical expected failures removed by this work include byte/short/float partition filters,
`cc_007_multiple_constraints_filter_amount`, `cc_020_decimal_constraint_filter_high_price`,
`dsReadDecimalType_readExpensive`, and `tw_decimal_precision_read_large_values`.

### Result schemas: #3451, merged

| Case | Example | Before -> updated behavior |
|---|---|---|
| Timestamp-bearing full scans | Historical `dsReadTimestampType_readAll` and `dpReadPartitionTimestamp_readAll`. | Equal timestamp values failed schema comparison -> compare timezone-free nanoseconds with UTC microseconds when conversion is exact. |
| Mixed-type filters | Historical `ds_typed_stats_hit_c1_eq_1`, `ds_stats_after_drop_hit_c1_eq_1`, and `ds_stats_after_rename_hit_cc1_eq_1`. | Even an integer predicate could fail because the returned batch also contained timestamps -> validate the complete result using compatible expected schemas. |
| Nested schemas | Historical `dcscStructWithSpecialTypes_read_all`, `_read_by_date`, and `_read_high_amount`. | Timestamp differences inside nested values blocked comparison -> apply the limited alignment recursively through structs, lists, and maps. |
| Missing all-null fields | `types_void_001_void_top_level_read_all` and `types_void_002_void_nested_struct_read_all`. | Expected Parquet can omit a `VOID` field -> restore it as nulls in the expected batch before comparing. |
| Generated-column reads | Historical `gc_append_data`, `gc_delete`, `gc_insert_by_name`, `gc_update_source`, and `gc_datetime` read/filter specs. | Timestamp-bearing expected results could block validation -> compare stored generated values using the same schema alignment. Kernel does not calculate the generated expressions during these reads. |
| Other timestamp-bearing table histories | Historical `cloneDeepMultiType_readAll`, `restoreCheckData_readAll`/`_filterDecimal`, mixed timestamp/NTZ scans, date-time statistics, and ordinary reads from CDC workload tables. | Representation differences no longer prevent comparison. Ordinary reads from a CDC table remain distinct from executing a CDF spec. |

Guard tests reject sub-microsecond timestamp values, arbitrary type conversions, reordered struct
fields, incompatible nullability, and missing non-`VOID` columns. These checks prevent alignment
from erasing real differences. Boundary cases that cannot be converted exactly remain failures.

### Projection and selective execution: #3459 and #3452, implemented, awaiting merge

| Case | Actual spec or example | Before -> updated behavior |
|---|---|---|
| Filter on an omitted output column | `deletion_vectors_projection_with_pred_read_value_gt_20_cols_id_name`: output `[id, name]`, predicate `value > 20`. | Harness cannot filter without `value` -> scan `[id, name, value]`, filter, then drop `value`. The spec expects two rows. |
| Reverse combination | `deletion_vectors_projection_with_pred_read_id_lt_4_cols_name_value`: output `[name, value]`, predicate `id < 4`. | Temporarily retain `id`, then return only the requested columns. The spec expects two rows. |
| Requested column order differs from table order | `data_skipping_schema_order_mismatch_read_cols_c_a`: table `[a, b, c]`, output `[c, a]`. | Continue validating the requested output order. Kernel is explicitly given that scan schema; the harness is not correcting a known Kernel ordering bug. |
| Companion specs in an unsupported-operation workload | One table directory contains `read`, `snapshot`, and `cdf` or `write` specs. | A broad directory skip could hide supported reads -> classify each spec and run the supported companions. |
| Malformed supported spec | A `read` spec is missing a required field. | Must remain a visible parse failure; it cannot silently become an unsupported operation. |
| Unknown operation | `{"type":"typo"}`. | The runner rejects the unknown type rather than silently skipping it. Known unsupported types are handled separately. |

The projection PR removes the historical failures
`ds_schema_order_mismatch_single_col_last`, `ds_with_dvs_edge_proj_and_skip_with_dv`, and
`dv_projection_with_pred_proj_and_pred`. Its focused regression test also checks that filtering
`[10, 20, 30]` by a separate column returns exactly `[20, 30]` in one output column.

### Stronger assertions: #3458 merged; #3454 implemented, awaiting merge

These changes improve what the test proves, even when the underlying scenario was already run:

| Scenario | Example | Assertion gained |
|---|---|---|
| Expected error must have the right cause | Expect `DELTA_STATE_RECOVER_ERROR`, get `MissingMetadata` versus `FileNotFound`. | Accept the state-recovery error; reject an unrelated missing-file error. |
| Protocol error categories | Unsupported reader version versus unsupported reader feature. | Check the corresponding category instead of accepting either failure indiscriminately. |
| Duplicate or unknown fields | Duplicate schema field, unknown predicate identifier, or missing nested field. | Match the supported schema/reference error forms; an unrelated harness limitation still fails. |
| Missing files and versions | Missing version 2; missing Parquet file versus storage permission denied. | Recognize supported missing-version/file categories and reject unrelated storage failures. |
| Snapshot time travel | Ask for version 3 but receive version 4 with otherwise matching metadata. | Fail with `Snapshot version mismatch: expected 3, got 4`. |
| Protocol feature order | `[columnMapping, deletionVectors]` versus `[deletionVectors, columnMapping]`. | Treat feature-list ordering as irrelevant while retaining the rest of the protocol comparison. |
| Exact known-failure identity | `table/specs/read_all` versus `table/specs/read_all_extra`. | The longer name does not inherit the shorter test's failure exemption. |
| Unexpected regression or fix | A known checkpoint-hint failure returns a different error, or starts succeeding. | Require the listed error text; fail on unexpected success so the inventory can be updated. |

## Investigation and follow-up

Ryan investigated failures against actual workloads and the Delta protocol, preserving useful
historical explanations and separating harness limitations, Kernel gaps, and Spark divergences.
The inventory links findings such as checksum masking (#2753), invalid column-mapping mode
(#1849), writer-invalid add/remove commits (#3441), struct/Variant null predicates (#3442), and
checkpoint fallback (#582).

Harness follow-up issues cover timestamp time travel (#3444), date and other predicate functions
(#3445), and `LIKE` expressions (#3446). Additional findings include column `IN` evaluation
(#3447) and empty lines in JSON commits (#3448). These are documented gaps, not claims that the
stack fixes each underlying behavior.

Unsupported operations are handled per spec, so a table's supported read and snapshot tests can
still run alongside its unsupported operations. This does not add CDF or write execution to the
read harness. Expected failures also fail when they unexpectedly succeed, prompting removal of
obsolete entries; required error substrings help catch failures for unrelated reasons, although
generic substrings remain less specific than typed checks.

## Delivery and validation

As of October 9, 2026, DAT #62 and Kernel #3450, #3451, and #3458 are merged. Kernel #3459, #3452,
#3454, and #3455 are implemented and pushed for review. The standalone corpus-pin PR #3453 was
closed; the release pin is included in #3454.

Validation includes focused parser, schema, error, snapshot, and projection tests; acceptance
corpus runs; and the cross-platform CI build/test matrix. Remaining red checks include PR-body
validation of GitStack's generated Unicode metadata and coverage thresholds. Those checks still
need resolution before calling the full stack merge-ready.
