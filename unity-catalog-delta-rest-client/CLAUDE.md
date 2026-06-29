# Unity Catalog REST client guidelines

Scope: `unity-catalog-delta-rest-client/**`. Cross-cutting conventions live in the root
`CLAUDE.md`. The UC crate stack is described in `delta-kernel-unity-catalog/CLAUDE.md`.

## What this crate is

The concrete HTTP implementation of the traits defined in `unity-catalog-delta-client-api`,
built on `reqwest`. It provides the commits client (`GetCommitsClient` + `CommitClient`) and a
higher-level client for table/credential endpoints, configured through a `ClientConfig` builder.

## Invariants to uphold

- **Implement the upstream traits; don't reinvent their models.** This crate's job is wire
  transport. The trait shapes and exchanged types are owned by `unity-catalog-delta-client-api`.
- **Own retry and HTTP error mapping here.** The trait contract says clients absorb transient
  failures, so retry/backoff and translation of HTTP status into the shared error type live in
  this layer, not in callers.
- **The API is async** (Tokio/reqwest); every public method is async. Don't add blocking calls.
- **Configuration is explicit.** Endpoint and auth must come through `ClientConfig`; never read
  ambient credentials or hard-code endpoints.
