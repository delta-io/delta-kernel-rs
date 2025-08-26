# uc-client

An experimental/under-construction rust client for Unity Catalog. This crate is not intended for
production use.

## Example CLI
This crate provides a command-line interface (CLI) to interact with Unity Catalog APIs, see
`uc-client/examples/uc-cli.rs`.

```bash
# set environment variables for UC url/token
UC_WORKSPACE_URL=https://some.uc.org
UC_TOKEN=your-token

# or set them in each command like
cargo run --example uc-cli -- --workspace-url <url> --token <token> table catalog.schema.table

# there are 3 operations:
# 1. get table metadata
cargo run --example uc-cli -- table catalog.schema.table

# 2. get commits for a table (automatically resolves table id and storage location)
cargo run --example uc-cli -- commits catalog.schema.table

# or get commits with version range
cargo run --example uc-cli -- commits catalog.schema.table \
  --start-version 0 \
  --end-version 10

# 3. get temporary credentials
cargo run --example uc-cli -- credentials \
  --table-id "table-uuid" \
  --operation READ

# also can enable verbose logging
cargo run --example uc-cli -- --verbose table catalog.schema.table
```