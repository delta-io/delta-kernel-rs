# unity-catalog-delta-rest-client

An experimental/under-construction Rust client for the Unity Catalog Delta APIs. This crate is not
intended for production use.

It provides two REST structs:

- `UCClient`: concrete HTTP methods for the connector-driven read endpoints (`load_table`,
  credential vending, `/config`).
- `UCUpdateTableRestClient`: an implementation of the `UpdateTableClient` trait (from
  `unity-catalog-delta-client-api`) against the `update_table` commit endpoint.

See the crate-level documentation for usage examples.
