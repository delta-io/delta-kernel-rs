# unity-catalog-delta-rest-client

An experimental/under-construction Rust client for the Unity Catalog Delta APIs. This crate is not
intended for production use.

It provides two REST structs:

- `UCClient`: concrete HTTP methods for the connector-driven read endpoints (`load_table`,
  credential vending, `/config`).
- `UCUpdateTableRestClient`: an implementation of the `UpdateTableClient` trait (from
  `unity-catalog-delta-client-api`) against the `update_table` commit endpoint.

## Example

```rust,no_run
use unity_catalog_delta_client_api::Operation;
use unity_catalog_delta_rest_client::{ClientConfig, UCClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure against a UC workspace (URL + token).
    let config = ClientConfig::build("https://some.uc.org", "your-token").build()?;
    let client = UCClient::new(config)?;

    // Load a table by its three-part name. The response carries the table
    // metadata and any inline (unpublished) commits.
    let table = client.load_table("catalog", "schema", "table").await?;
    println!("table id: {}", table.metadata.table_uuid);
    println!("location: {}", table.metadata.location);

    // Vend temporary storage credentials for reads.
    let creds = client
        .get_table_credentials("catalog", "schema", "table", Operation::Read)
        .await?;
    println!("vended {} credential(s)", creds.storage_credentials.len());

    Ok(())
}
```

To coordinate commits (version >= 1), construct a `UCUpdateTableRestClient` and use it as the
`UpdateTableClient` trait object; see the crate-level documentation for details.
