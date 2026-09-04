# Configuring storage

To configure how `DefaultEngine` accesses your Delta tables, create an `EngineStore`
from a URL and pass it to the engine builder. The `DefaultEngine` uses the
[`object_store`](https://docs.rs/object_store) crate for all storage I/O, supporting
local files, S3, GCS, and Azure out of the box.

Before reading this page, make sure you understand
[The Engine Trait](../concepts/engine_trait.md).

> [!NOTE]
> The storage APIs on this page come from the `delta_kernel_default_engine` crate. Add it
> to your `Cargo.toml` with either the `rustls` or `native-tls` feature.
> See [Feature Flags](../concepts/feature_flags.md) for details.

`EngineStore` retains both the object store and its listing capabilities. For built-in
S3, GCS, and Azure stores, URL-based construction enables single-directory pagination.
Ordered stores push the starting offset into the request and fetch pages on demand,
avoiding older log files and the contents of nested directories. S3 Express cannot apply
that offset: the engine collects its shallow listing, then sorts and filters it locally.

- For a standard URL, use `EngineStore::from_url`.
- For a URL with credentials or other options, use `EngineStore::from_url_opts`.
- For a custom URL scheme, register a handler with `insert_url_handler`.
- For a preconfigured store, select `EngineStore::with_paginated` or `EngineStore::plain`.

## Standard URL

`EngineStore::from_url` creates a store from a URL. The `object_store` crate detects
the storage backend from the URL scheme:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# extern crate url;
# use std::sync::Arc;
# use url::Url;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::EngineStore;
# use delta_kernel::DeltaResult;
# fn main() -> DeltaResult<()> {
let url = Url::parse("file:///path/to/table")?;
let store = EngineStore::from_url(&url)?;
let engine = DefaultEngine::builder(store).build();
# Ok(())
# }
```

## URL with options

To pass provider-specific options (credentials, region, endpoint, etc.), use
`EngineStore::from_url_opts`. These options configure the underlying `object_store` provider:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# extern crate url;
# use std::collections::HashMap;
# use url::Url;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::EngineStore;
# use delta_kernel::DeltaResult;
# fn main() -> DeltaResult<()> {
let url = Url::parse("s3://my-bucket/path/to/table")?;
let options = HashMap::from([
    ("region", "us-west-2"),
    ("access_key_id", "AKIA..."),
    ("secret_access_key", "..."),
]);
let store = EngineStore::from_url_opts(&url, options)?;
let engine = DefaultEngine::builder(store).build();
# Ok(())
# }
```

See the [`object_store` documentation](https://docs.rs/object_store) for the full list
of supported options per storage provider.

## Custom URL schemes

If you need to support a URL scheme that `object_store` doesn't handle natively (e.g.
`hdfs://`), register a handler with `insert_url_handler`:

```rust,ignore
use std::sync::Arc;
use delta_kernel_default_engine::storage::{insert_url_handler, EngineStore};

insert_url_handler("hdfs", Arc::new(|url, options| {
    // Build your custom ObjectStore from the URL and options
    let store = build_hdfs_store(url, &options)?;
    let path = object_store::path::Path::parse(url.path())?;
    Ok((EngineStore::plain(Arc::new(store)), path))
}))?;

let store = EngineStore::from_url(&url)?;
```

The handler closure receives a `&Url` and a `HashMap<String, String>` of options, and
returns a `Result<(EngineStore, Path), Error>`. If your store implements
`PaginatedListStore`, return `EngineStore::with_paginated` to preserve its pagination
support. The engine uses the capabilities selected by your handler.

## Bringing your own object store

To bypass URL-based construction, build a concrete cloud store and retain its paginated
listing capability:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::DeltaResult;
use std::sync::Arc;
use delta_kernel::object_store::aws::AmazonS3Builder;
use delta_kernel_default_engine::{storage::EngineStore, DefaultEngine};

# fn main() -> DeltaResult<()> {
let store = Arc::new(
    AmazonS3Builder::new()
        .with_bucket_name("my-bucket")
        .with_region("us-west-2")
        .build()?,
);
let engine = DefaultEngine::builder(EngineStore::with_paginated(store)).build();
# Ok(())
# }
```

For stores without pagination support, opt into the shallow fallback explicitly:

```rust
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
use std::sync::Arc;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel_default_engine::{storage::EngineStore, DefaultEngine};

let store = Arc::new(LocalFileSystem::new());
let engine = DefaultEngine::builder(EngineStore::plain(store)).build();
```

The fallback retrieves every direct child in the directory before applying the offset.
On a cloud store with a long retained history, this can require many listing requests even
when you only need the newest files. Keep pagination support through custom wrappers, or
choose the fallback with that cost in mind. Bare `Arc` stores are not accepted by the builders.

The lower-level `store_from_url` and `store_from_url_opts` functions return an
`Arc<dyn ObjectStore>` without pagination capabilities. Use them for direct object-store
operations, not as an intermediate step when constructing an engine.

## What's next

- [Building a scan](../reading/building_a_scan.md)
- [Implementing an engine](../connector/implementing_engine.md)
