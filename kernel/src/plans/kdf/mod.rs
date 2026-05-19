//! Kernel-Defined Functions (KDFs) — stateful per-row logic the kernel owns.
//!
//! KDFs encapsulate Delta-specific per-row work (checkpoint hint extraction,
//! protocol/metadata harvesting, sidecar collection) that engines can't
//! interpret. Today the IR exposes one KDF shape:
//!
//! - [`traits::ConsumerKdf`] — observer over batches; returns `Continue` / `Break` for early
//!   termination. Wired into the plan via the
//!   [`SinkType::Consume`](crate::plans::ir::nodes::SinkType::Consume) sink — the consumer drains
//!   the terminal row stream, accumulating its own finalized state for the engine to harvest after
//!   the plan completes.
//!
//! KDFs are dispatched in-process and never cross a serialization boundary
//! (the sink is an opaque pointer to the engine).
//!
//! # Identity
//!
//! - [`token::KdfStateToken`] — `{ kdf_id, id }` stamped at plan-build time. Keys the executor's
//!   state table.
//! - `(sm_id, sm_kind, phase_name)` — owning SM's identity tuple stamped at phase-execute time.
//!   Lives directly as fields on [`handle::Handle`] and [`handle::FinishedHandle`]; used by tracing
//!   and cross-check validations.
//!
//! # Handles
//!
//! - [`handle::Handle<K>`] — generic runtime state. Executor code holds `Handle<dyn ConsumerKdf>`
//!   directly.
//!
//! # Adding a KDF
//!
//! New file with three impl blocks (`ConsumerKdf`, `KdfOutput`, `RowVisitor`), one
//! line in the submodule mod.rs, one line re-exporting here.
//!
//! [`ConsumeSink`]: crate::plans::ir::nodes::ConsumeSink

pub mod handle;
pub mod state;
pub mod token;
pub mod traits;
pub mod typed;

pub use handle::{FinishedHandle, Handle};
pub use state::consumer::{CheckpointHintReader, MetadataProtocolReader, SidecarCollector};
pub use token::{ConsumerKdfId, KdfStateToken};
pub use traits::{ConsumerKdf, KdfControl};
pub use typed::{Extractor, KdfOutput};
