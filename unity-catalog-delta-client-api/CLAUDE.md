# Unity Catalog client API guidelines

Scope: `unity-catalog-delta-client-api/**`. Cross-cutting conventions live in the root
`CLAUDE.md`. The UC crate stack and how this layer fits is described in
`delta-kernel-unity-catalog/CLAUDE.md`.

## What this crate is

The transport-agnostic contract for talking to Unity Catalog's Delta APIs: the `GetCommitsClient`
and `CommitClient` traits plus the request/response and credential-vending models they exchange.
It contains **no HTTP and no concrete client** -- the REST implementation lives in
`unity-catalog-delta-rest-client`, and an in-memory mock (behind `test-utils`) backs unit tests.

## Invariants to uphold

- **This is the seam; keep it implementation-free.** No reqwest, no wire details, no kernel
  dependency. Anything HTTP-specific belongs in the REST client.
- **Retries are the implementor's responsibility, not the caller's.** The trait contract assumes
  a client handles transient failures internally; document and honor that when adding methods.
- **Clients are `Send + Sync`** so they can be shared across threads -- preserve those bounds on
  any new trait or method.
- **Models are shared across all UC crates.** Add or change a type here and let the REST client
  and `delta-kernel-unity-catalog` consume it; do not redefine UC models downstream.
