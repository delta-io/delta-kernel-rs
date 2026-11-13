# Implementing a catalog committer

To commit transactions on catalog-managed tables, you implement the `Committer` trait with
your catalog's staging and ratification logic. For filesystem-managed tables, the built-in
`FileSystemCommitter` writes delta files directly via PUT-if-absent. For catalog-managed
tables, you provide your own `Committer` that routes commits through your catalog.

> [!WARNING]
> Kernel rejects `FileSystemCommitter` on a catalog-managed table at `txn.commit()`
> time. You must provide a catalog committer before commit runs.

Before reading this page, make sure you understand [Catalog-managed tables](./overview.md).

## The Committer trait

```rust,ignore
pub trait Committer: Send {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        commit_metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse>;

    fn is_catalog_committer(&self) -> bool;

    fn publish(
        &self,
        engine: &dyn Engine,
        publish_metadata: PublishMetadata,
    ) -> KernelResult<()>;
}
```

This excerpt shows the required methods. `commit()` and `publish()` perform I/O;
`is_catalog_committer()` identifies the committer type:

1. **`commit()`** atomically commits the given actions at the version specified in
   `CommitMetadata`. Returns `CommitResponse::Committed` on success or
   `CommitResponse::Conflict { version, commits }` if another writer already committed this
   version. Bundle complete winning history in `commits`; if discovery fails, put the load error
   in that field so the response still reports a confirmed conflict.
   Return `CommitResponse::Retryable { error }` for a temporary failure known to have
   prevented acceptance of the commit.

2. **`is_catalog_committer()`** returns `true` for catalog committers. Kernel checks
   this flag on both commit and publish paths and enforces the pairing in both
   directions: it rejects a `FileSystemCommitter` on a catalog-managed table, and
   it rejects a catalog committer on a filesystem-managed table.

3. **`publish()`** copies ratified catalog commits from `_staged_commits/` to the main
   `_delta_log/` directory as published delta files. Some catalogs publish
   server-side, in which case `publish()` only notifies the catalog; others use
   PUT-if-absent copies from the client.

After a confirmed version conflict, the committer loads and bundles the history needed for conflict
checking. Catalog committers combine published files with catalog-ratified commits and report a
history-load error if any required version is missing. Kernel saves the bundled history or its
error on the conflict result. `get_winning_commits()` returns only that saved range or error and
performs no I/O. If discovery failed, load a fresh Snapshot and rebuild your transaction. Success,
temporary rejection, and unknown outcomes skip discovery. The trait also provides an optional
declarative commit method; see its rustdoc for the complete API.

## CommitMetadata

Kernel constructs `CommitMetadata` and passes it to your `commit()` method. Key methods:

```rust,ignore
impl CommitMetadata {
    pub fn published_commit_path(&self) -> KernelResult<Url>;
    pub fn staged_commit_path(&self) -> KernelResult<Url>;  // unique UUID each call
    pub fn version(&self) -> Version;
    pub fn commit_type(&self) -> CommitType;
    pub fn in_commit_timestamp(&self) -> i64;
    pub fn max_published_version(&self) -> Option<Version>;
    pub fn table_root(&self) -> &Url;
}
```

This block is a subset of `CommitMetadata`'s public surface. See the rustdoc for
the full list.

- `published_commit_path()` returns the final delta log path (e.g.,
  `s3://bucket/table/_delta_log/00000000000000000001.json`).
- `staged_commit_path()` returns a unique staged commit path (e.g.,
  `s3://bucket/table/_delta_log/_staged_commits/00000000000000000001.<uuid>.json`).
  Kernel generates a fresh UUID on every call; do not substitute a catalog-supplied
  UUID.
- `commit_type()` returns a `CommitType` variant indicating whether this is a table
  creation or a write, and whether the table is catalog-managed.
- `in_commit_timestamp()` returns the timestamp (milliseconds since the Unix epoch,
  UTC) Kernel will record in the `CommitInfo` action as `inCommitTimestamp`. Kernel
  also writes a fresh `txnId` (UUID) on every `CommitInfo` it generates; the Delta
  spec requires `txnId` on catalog-managed commits, and Kernel emits it
  unconditionally.
- `max_published_version()` returns the highest version already published to the delta
  log, if any. Your catalog may need this to determine which commits still require
  publishing.

> [!WARNING]
> Call `staged_commit_path()` exactly once per `commit()` invocation. Because each
> call produces a different UUID, a second call yields a different path: only one
> path gets written, and if you use the other to tell the catalog, the catalog
> points at a missing file.

## CommitResponse

```rust,ignore
pub enum CommitResponse {
    Committed { file_meta: FileMeta },
    Conflict { version: Version },
    Retryable { error: KernelError },
}
```

Return `Committed` with the `FileMeta` of the staged commit file on success. Return
`Conflict` with the attempted version if another writer already committed that version.
Return `Retryable` only when the attempt is known not to have committed and the failure can be
addressed before retrying the same version. Uncertain outcomes must remain errors.

## Implementing a catalog committer

The typical implementation follows four steps. Steps 1 and 2 form the body of
`commit()`. Step 3 is the `is_catalog_committer()` flag. Step 4 is `publish()`.

### Step 1: Stage the commit

Write the actions to a staged commit file in `_staged_commits/`:

```rust,ignore
fn commit(
    &self,
    engine: &dyn Engine,
    actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
    commit_metadata: CommitMetadata,
) -> KernelResult<CommitResponse> {
    // Write actions to _staged_commits/<version>.<uuid>.json. `actions` is
    // already a Box<dyn KernelResultIterator<...>>, so pass it directly (do not re-box).
    let staged_path = commit_metadata.staged_commit_path()?;
    let written_size = engine
        .json_handler()
        .write_json_file(&staged_path, actions, false)?;
    // ...
```

### Step 2: Ratify through the catalog

Call your catalog's commit API to ratify the staged commit. The exact arguments
vary by catalog; Unity Catalog's `CommitRequest`, for example, carries the table
id, commit version, staged filename, in-commit timestamp, and the maximum
published version. Your catalog's API may look different. Here is the general
shape:

```rust,ignore
    // Tell the catalog about the staged commit.
    // Replace this with your catalog's ratification API. Forward the
    // commit_metadata.in_commit_timestamp() value so the catalog records the
    // same timestamp Kernel writes into the CommitInfo action.
    self.catalog_client.ratify_commit(
        &self.table_id,
        commit_metadata.version(),
        &staged_path,
        commit_metadata.in_commit_timestamp(),
        commit_metadata.max_published_version(),
    )?;

    // Return the staged file metadata on success and use
    // the in-commit timestamp as the logical commit time (not the filesystem
    // mtime, which reflects when the file was written rather than when the
    // commit took effect).
    Ok(CommitResponse::Committed {
        file_meta: FileMeta::new(
            staged_path,
            commit_metadata.in_commit_timestamp(),
            written_size,
        ),
    })
}
```

Map your catalog's "another writer won this version" error to `CommitResponse::Conflict` and
include the rejected version and a winning-history discovery result. Return the confirmed conflict
even if loading history fails. Kernel saves the bundled history or discovery error; result getters
perform no I/O. A failed discovery requires a fresh Snapshot and rebuilt transaction. Return
`CommitResponse::Retryable { error }` for confirmed temporary failures, such as a connection failure
before sending the request or a catalog rejection requiring outstanding commits to be published.
The pending transaction can then retry the same version without rebasing. Retryable outcomes do
not trigger winning-history discovery or an automatic retry loop.

Return other errors as `Err(...)`. Prepared declarative transactions preserve these as `Unknown`
outcomes and block resubmission until the submitted outcome is resolved. A timeout after sending
the request is insufficient evidence that the commit failed. Imperative transactions also retain
their existing `KernelError::IOError` to `CommitResult::Retryable` mapping; do not disguise other
errors as `IOError` to opt into retry semantics.

### Step 3: Mark as catalog committer

```rust,ignore
fn is_catalog_committer(&self) -> bool {
    true
}
```

### Step 4: Implement publish

Publishing copies staged commits from `_staged_commits/` to the main `_delta_log/`.
Kernel passes the commits to publish as a contiguous ascending batch via
`PublishMetadata::commits_to_publish()`. Your implementation must:

- Copy the commits **in the order Kernel provides** (version `v-1` before version `v`).
  Do not reorder.
- Be **idempotent**. Treat `FileAlreadyExists` as success; a previous publish may have
  already copied some entries.

```rust,ignore
use delta_kernel::KernelError;

fn publish(
    &self,
    engine: &dyn Engine,
    publish_metadata: PublishMetadata,
) -> KernelResult<()> {
    for catalog_commit in publish_metadata.commits_to_publish() {
        let src = catalog_commit.location();            // _staged_commits/<v>.<uuid>.json
        let dest = catalog_commit.published_location(); // _delta_log/<v>.json
        match engine.storage_handler().copy_atomic(src, dest) {
            Ok(()) | Err(KernelError::FileAlreadyExists(_)) => (), // already published
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
```

## Putting it all together

Here is the full skeleton for a catalog committer. Replace `MyCatalogClient` with your
catalog's client type and fill in the ratification logic:

```rust,ignore
// Imports elided for brevity. In addition to the ones below, you will need
// Committer, CommitMetadata, CommitResponse, PublishMetadata, KernelResult,
// FilteredEngineData, and Engine from delta_kernel.
use delta_kernel::{KernelError, FileMeta};

pub struct MyCatalogCommitter {
    catalog_client: Arc<MyCatalogClient>,
    table_id: String,
}

impl Committer for MyCatalogCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        commit_metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        // 1. Stage: write actions to _staged_commits/
        let staged_path = commit_metadata.staged_commit_path()?;
        let written_size = engine
            .json_handler()
            .write_json_file(&staged_path, actions, false)?;

        // 2. Ratify: register the staged commit with the catalog. ratify_commit
        //    is an imagined example API; your catalog's signature will differ.
        self.catalog_client.ratify_commit(
            &self.table_id,
            commit_metadata.version(),
            &staged_path,
            commit_metadata.in_commit_timestamp(),
            commit_metadata.max_published_version(),
        )?;

        // 3. Return success and use the in-commit timestamp as the logical commit time (not
        //    the filesystem mtime).
        Ok(CommitResponse::Committed {
            file_meta: FileMeta::new(
                staged_path,
                commit_metadata.in_commit_timestamp(),
                written_size,
            ),
        })
    }

    fn is_catalog_committer(&self) -> bool {
        true
    }

    fn publish(
        &self,
        engine: &dyn Engine,
        publish_metadata: PublishMetadata,
    ) -> KernelResult<()> {
        for catalog_commit in publish_metadata.commits_to_publish() {
            let src = catalog_commit.location();
            let dest = catalog_commit.published_location();
            match engine.storage_handler().copy_atomic(src, dest) {
                Ok(()) | Err(KernelError::FileAlreadyExists(_)) => (),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
```

For a complete Unity Catalog implementation, see
[Unity Catalog integration](../unity_catalog/overview.md).

## What's next

- [Reading catalog-managed tables](./reading.md) covers how the catalog client
  provides commits for snapshot construction.
- [Writing: the commit and publish flow](./writing.md) walks through the complete write
  lifecycle including publishing.
