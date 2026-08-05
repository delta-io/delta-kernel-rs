# unity-catalog-delta-client-api

An experimental/under-construction crate defining the client API contract for the Unity Catalog
Delta Tables API. This crate is not intended for production use.

It has no network dependencies, so any transport can implement it. The REST implementation lives in
[`unity-catalog-delta-rest-client`], and [`delta-kernel-unity-catalog`] depends on this crate rather
than on a concrete client.

It provides:

- `UpdateTableClient`: the trait for committing a new version to a UC-managed table, which
  `delta-kernel-unity-catalog`'s `UCCommitter` dispatches through.
- Serde wire models for the UC Delta Tables endpoints: `load_table`, credential vending, `/config`,
  table and staging-table creation, `update_table`, and metrics reporting.

Only `update_table` (the commit RPC) sits behind a trait. Read and credential-vending flows are
connector-driven: connectors call concrete client methods, or bring their own HTTP plumbing, then
hand the responses to `delta-kernel-unity-catalog` helpers.

[`unity-catalog-delta-rest-client`]: https://github.com/delta-io/delta-kernel-rs/tree/main/unity-catalog-delta-rest-client
[`delta-kernel-unity-catalog`]: https://github.com/delta-io/delta-kernel-rs/tree/main/delta-kernel-unity-catalog
