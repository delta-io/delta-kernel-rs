# Changelog

## [v0.1.0_unity-catalog-delta-rest-client](https://github.com/delta-io/delta-kernel-rs/tree/v0.1.0_unity-catalog-delta-rest-client/) (2026-10-08)


### 🚀 Features / new APIs

1. Add UC Delta-Tables API wire models and UpdateTableClient trait ([#2906])
2. Send User-Agent header on UC Delta-Tables API requests ([#2909])
3. Add UC OSS-server live integration test CI for get_config and load_table ([#2925])
4. Add UC Delta-Tables credential-vending wire types and client method ([#2934])
5. Add UC Delta-Tables create-table wire types and staging E2E test ([#2941])
6. Migrate UC integration to Delta Tables LoadTable and UpdateTable APIs ([#2855])
7. Tag UC client User Agent and rename unity-catalog-delta-rest-client ([#3033])
8. Add create_staging_table and create_table to UCClient ([#3034])
9. Re-export UC wire models from unity-catalog-delta-rest-client ([#3143])

### 📚 Documentation

1. Add crate metadata and READMEs for the UC crates ([#3061])

### 🚜 Refactor

1. Rename uc-catalog and uc-client crates ([#2136])
2. Enforce line width and import ordering with nightly rustfmt ([#2383])
3. Tighten unity-catalog-delta-rest-client public surface ([#3264])
4. Rename UCClient to UCDeltaTableClient ([#3463])

### ⚙️ Chores/CI

1. Version the UC crates independently at 0.1.0 ([#3062])


[#2136]: https://github.com/delta-io/delta-kernel-rs/pull/2136
[#2383]: https://github.com/delta-io/delta-kernel-rs/pull/2383
[#2906]: https://github.com/delta-io/delta-kernel-rs/pull/2906
[#2909]: https://github.com/delta-io/delta-kernel-rs/pull/2909
[#2925]: https://github.com/delta-io/delta-kernel-rs/pull/2925
[#2934]: https://github.com/delta-io/delta-kernel-rs/pull/2934
[#2941]: https://github.com/delta-io/delta-kernel-rs/pull/2941
[#2855]: https://github.com/delta-io/delta-kernel-rs/pull/2855
[#3033]: https://github.com/delta-io/delta-kernel-rs/pull/3033
[#3034]: https://github.com/delta-io/delta-kernel-rs/pull/3034
[#3061]: https://github.com/delta-io/delta-kernel-rs/pull/3061
[#3062]: https://github.com/delta-io/delta-kernel-rs/pull/3062
[#3143]: https://github.com/delta-io/delta-kernel-rs/pull/3143
[#3264]: https://github.com/delta-io/delta-kernel-rs/pull/3264
[#3463]: https://github.com/delta-io/delta-kernel-rs/pull/3463


