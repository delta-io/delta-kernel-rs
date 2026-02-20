# unity-catalog-client

A Rust HTTP client for Unity Catalog REST APIs. This crate handles table metadata
resolution and temporary credential management -- the general-purpose UC operations that
are not specific to Delta Kernel.

This crate depends on `delta-kernel-unity-catalog` for shared infrastructure (`config`,
`error`, `http`) and re-exports those modules for convenience.

This crate is experimental and under active development with the UC team. It is not
published to crates.io.

## Components

### `UCClient`

The main HTTP client. Provides:

- `get_table` -- resolve a three-part table name (`catalog.schema.table`) to its table ID
  and storage location
- `get_credentials` -- obtain temporary cloud storage credentials (e.g. AWS STS) for
  reading or writing a table's underlying files

### `models`

API request/response types:

- `models::tables` -- `TablesResponse` (table metadata from `GET /tables/{name}`)
- `models::credentials` -- `TemporaryTableCredentials`, `AwsTempCredentials`, `Operation`