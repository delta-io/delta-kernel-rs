# delta-kernel-unity-catalog

An experimental/under-construction crate connecting [`delta_kernel`] to Unity Catalog for
catalog-managed Delta tables. This crate is not intended for production use.

A table with the `catalogManaged` table feature cannot be read or written by accessing the
transaction log on disk alone. This crate bridges UC's responses into kernel's APIs and implements
kernel's `Committer` so commits go through the catalog.

It depends on [`unity-catalog-delta-client-api`] for the client contract, not on a concrete HTTP
client, so you can pair it with [`unity-catalog-delta-rest-client`] or your own implementation.

It provides:

- `UCCommitter`: kernel's `Committer` trait implemented against UC, so commits are staged and
  ratified through the catalog rather than written straight to the log, plus `publish()` to promote
  ratified commits. Requires a multi-threaded tokio runtime.
- `snapshot_builder_from_load_table()` and `log_tail_from_commits()`: build a kernel snapshot from a
  `load_table` response, including commits UC knows about but has not yet published.
- `get_required_properties_for_disk()` and `build_uc_create_table_request()`: the table-creation
  flow.
- `aws_object_store_options()`: maps UC-vended storage credentials onto object-store options.

See the [Unity Catalog integration guide] for the end-to-end read, write, and create flows.

[`delta_kernel`]: https://crates.io/crates/delta_kernel
[`unity-catalog-delta-client-api`]: https://github.com/delta-io/delta-kernel-rs/tree/main/unity-catalog-delta-client-api
[`unity-catalog-delta-rest-client`]: https://github.com/delta-io/delta-kernel-rs/tree/main/unity-catalog-delta-rest-client
[Unity Catalog integration guide]: https://github.com/delta-io/delta-kernel-rs/blob/main/docs/user-guide/src/unity_catalog/overview.md
