# Changelog

## [v0.1.0_delta-kernel-unity-catalog](https://github.com/delta-io/delta-kernel-rs/tree/v0.1.0_delta-kernel-unity-catalog/) (2026-10-08)


### 🚀 Features / new APIs

1. Add catalog-managed table creation utilities ([#2203])
2. Support version 0 (table creation) commits in UCCommitter ([#2247])
3. Add UCCommitter validation for catalog-managed tables ([#2254])
4. Add max catalog version parameter and edit call sites ([#2066])
5. Robust partitioned-write APIs ([#2356])
6. Reject non-null columns in CREATE TABLE unconditionally ([#2404])
7. Support writing V2 checkpoint with sidecar through snapshot.checkpoint ([#2333])
8. Add `table_type` to snapshot-load and scan metric events ([#2721])
9. Schema! macro for easier schema creation ([#2795])
10. Migrate UC integration to Delta Tables LoadTable and UpdateTable APIs ([#2855])
11. Create Unity Catalog managed tables via the Delta Tables API ([#2826])
12. Tag UC client User Agent and rename unity-catalog-delta-rest-client ([#3033])
13. Add create_staging_table and create_table to UCClient ([#3034])

### 🐛 Bug Fixes

1. Make generic errors more specific in snapshot creation ([#2987])
2. Report exact JSON write sizes ([#3063])

### 📚 Documentation

1. Add crate metadata and READMEs for the UC crates ([#3061])

### 🚜 Refactor

1. Rename uc-catalog and uc-client crates ([#2136])
2. Enforce line width and import ordering with nightly rustfmt ([#2383])
3. Use DeltaResultIterator where possible ([#2622])
4. Rename ScopedDeltaResultIterator ([#2632])
5. Extract default engine into separate crate ([#2397])
6. Extract log_tail_from_commits helper in UC kernel client ([#2927])
7. Use schema macros for declarative schemas ([#3127])
8. Reclassify generic log segment errors ([#3195])
9. Rename `Error` to `KernelError` ([#3420])
10. Rename UCClient to UCDeltaTableClient ([#3463])
11. Rename delta result aliases to result ([#3477])
12. Use kernel results for private operations ([#3478])

### 🧪 Testing

1. Cover non-catalog-managed UC tables in read/write examples ([#2463])

### ⚙️ Chores/CI

1. Remove the catalog-managed feature flag ([#2310])
2. Upgrade to arrow-58 and object_store-13, drop arrow-56 support ([#2116])
3. Use type constructors to avoid Box::new and DataType::from boilerplate ([#2700])
4. Add arrow-59, drop arrow-57 ([#2847])
5. Version the UC crates independently at 0.1.0 ([#3062])
6. Release v0.27.0 ([#3107])
7. Use col and col_name macros everywhere they make sense ([#3121])
8. Release v0.27.1 ([#3125])
9. Release/0.28.0 ([#3200])
10. Release 0.29.0 ([#3427])
11. Update to arrow-60, drop arrow-58 ([#3403])


[#2136]: https://github.com/delta-io/delta-kernel-rs/pull/2136
[#2203]: https://github.com/delta-io/delta-kernel-rs/pull/2203
[#2247]: https://github.com/delta-io/delta-kernel-rs/pull/2247
[#2254]: https://github.com/delta-io/delta-kernel-rs/pull/2254
[#2310]: https://github.com/delta-io/delta-kernel-rs/pull/2310
[#2116]: https://github.com/delta-io/delta-kernel-rs/pull/2116
[#2066]: https://github.com/delta-io/delta-kernel-rs/pull/2066
[#2356]: https://github.com/delta-io/delta-kernel-rs/pull/2356
[#2383]: https://github.com/delta-io/delta-kernel-rs/pull/2383
[#2404]: https://github.com/delta-io/delta-kernel-rs/pull/2404
[#2333]: https://github.com/delta-io/delta-kernel-rs/pull/2333
[#2463]: https://github.com/delta-io/delta-kernel-rs/pull/2463
[#2622]: https://github.com/delta-io/delta-kernel-rs/pull/2622
[#2632]: https://github.com/delta-io/delta-kernel-rs/pull/2632
[#2700]: https://github.com/delta-io/delta-kernel-rs/pull/2700
[#2397]: https://github.com/delta-io/delta-kernel-rs/pull/2397
[#2721]: https://github.com/delta-io/delta-kernel-rs/pull/2721
[#2795]: https://github.com/delta-io/delta-kernel-rs/pull/2795
[#2847]: https://github.com/delta-io/delta-kernel-rs/pull/2847
[#2927]: https://github.com/delta-io/delta-kernel-rs/pull/2927
[#2987]: https://github.com/delta-io/delta-kernel-rs/pull/2987
[#2855]: https://github.com/delta-io/delta-kernel-rs/pull/2855
[#2826]: https://github.com/delta-io/delta-kernel-rs/pull/2826
[#3033]: https://github.com/delta-io/delta-kernel-rs/pull/3033
[#3034]: https://github.com/delta-io/delta-kernel-rs/pull/3034
[#3061]: https://github.com/delta-io/delta-kernel-rs/pull/3061
[#3062]: https://github.com/delta-io/delta-kernel-rs/pull/3062
[#3107]: https://github.com/delta-io/delta-kernel-rs/pull/3107
[#3121]: https://github.com/delta-io/delta-kernel-rs/pull/3121
[#3125]: https://github.com/delta-io/delta-kernel-rs/pull/3125
[#3063]: https://github.com/delta-io/delta-kernel-rs/pull/3063
[#3127]: https://github.com/delta-io/delta-kernel-rs/pull/3127
[#3200]: https://github.com/delta-io/delta-kernel-rs/pull/3200
[#3195]: https://github.com/delta-io/delta-kernel-rs/pull/3195
[#3420]: https://github.com/delta-io/delta-kernel-rs/pull/3420
[#3427]: https://github.com/delta-io/delta-kernel-rs/pull/3427
[#3463]: https://github.com/delta-io/delta-kernel-rs/pull/3463
[#3403]: https://github.com/delta-io/delta-kernel-rs/pull/3403
[#3477]: https://github.com/delta-io/delta-kernel-rs/pull/3477
[#3478]: https://github.com/delta-io/delta-kernel-rs/pull/3478
