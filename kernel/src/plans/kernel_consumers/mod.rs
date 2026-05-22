//! Kernel-Defined Functions (KDFs) — stateful per-row logic the kernel owns.
//!
//! KDFs encapsulate Delta-specific per-row work (checkpoint hint extraction,
//! protocol/metadata harvesting, sidecar collection) that engines can't interpret.
//!
//! The IR exposes one KDF shape: [`traits::KernelConsumer`], an observer over batches
//! returning `Continue` / `Break`. It's wired into a plan via
//! [`Step::Consume`](crate::plans::operations::framework::step::Step::Consume); the consumer drains
//! the terminal row stream and accumulates finalized state for the engine to harvest.
//!
//! KDFs dispatch in-process and never cross a serialization boundary.
//!
//! Each [`handle::Handle`] carries two identity tuples: [`token::KernelConsumerToken`]
//! (`{ kind, id }`, stamped at plan-build time, keys the executor's state table) and
//! `(sm_id, sm_kind, step_name)` (the owning SM, stamped at phase-execute time).

pub mod handle;
pub mod state;
pub mod token;
pub mod traits;
pub mod typed;

pub use handle::{FinishedHandle, Handle};
pub use state::consumer::{CheckpointHintReader, MetadataProtocolReader, SidecarCollector};
pub use token::{KernelConsumerKind, KernelConsumerToken};
pub use traits::{KdfControl, KernelConsumer};
pub use typed::{Extractor, KernelConsumerOutput};
