//! Consumer KDF implementations — observers that fold batches into typed
//! output without altering the row stream.
//!
//! Each submodule declares one state type + its `KernelConsumer` / `KernelConsumerOutput`
//! / `RowVisitor` impls.

pub mod checkpoint_hint;
pub mod metadata_protocol;
pub mod sidecar_collector;

pub use checkpoint_hint::{CheckpointHintReader, CheckpointHintRecord};
pub use metadata_protocol::MetadataProtocolReader;
pub use sidecar_collector::SidecarCollector;
