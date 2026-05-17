//! Consumer KDF implementations — observers that fold batches into typed
//! output without altering the row stream.
//!
//! Each submodule declares one state type + its `ConsumerKdf` / `KdfOutput`
//! / `RowVisitor` impls.

pub mod checkpoint_hint;
pub mod metadata_protocol;
pub mod sidecar_collector;

pub use checkpoint_hint::CheckpointHintReader;
pub use metadata_protocol::MetadataProtocolReader;
pub use sidecar_collector::SidecarCollector;
