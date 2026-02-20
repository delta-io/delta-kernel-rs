# delta-kernel-unity-catalog

The translation layer between Unity Catalog and Delta Kernel. This crate bridges UC's
commit coordination APIs with kernel's read/write paths, allowing kernel-based connectors
to read and write catalog-managed Delta tables.

## Components

### `UCCommitter`

The UC-specific kernel [`Committer`] implementation. It stages commit files to cloud storage,
then calls the UC commits API to ratify (finalize) each staged commit. After ratification,
clients are expected to periodically *publish* staged commits to the Delta log via the
`publish` method.

`UCCommitter` is generic over `UCCommitClient`, so different transport implementations
can be injected -- for example, the default REST client, a C++ FFI client, or a gRPC client.

### `UCCommitClient` / `UCGetCommitsClient` (traits)

Defines the interface for UC commit operations:

- `UCGetCommitsClient::get_commits` -- fetch committed versions for a table (used during
  snapshot loading)
- `UCCommitClient::commit` -- register a new staged commit with UC

Implementations:

| Implementation | Description |
|----------------|-------------|
| `UCCommitsRestClient` | Default REST client that talks to the UC commits API over HTTP |
| `InMemoryCommitsClient` | In-memory mock for testing (behind `test-utils` feature) |

### `UCKernelClient`

A lightweight client that loads kernel `Snapshot`s for UC-managed tables. It fetches commits
from UC via a `UCGetCommitsClient`, translates them into kernel `LogPath`s, and delegates to
kernel's `Snapshot::builder_for` to construct the snapshot. Supports both latest-version
reads and time-travel queries.

### `models`

Request/response types for the UC commits API: `CommitsRequest`, `CommitsResponse`,
`CommitRequest`, and `Commit`.
