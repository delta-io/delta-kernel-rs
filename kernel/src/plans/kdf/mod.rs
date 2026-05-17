//! Kernel-Defined Functions (KDFs) — stateful per-row logic the kernel owns.
//!
//! KDFs encapsulate Delta-specific per-row work (checkpoint hint extraction,
//! protocol/metadata harvesting, sidecar collection) that engines can't
//! interpret. Today the IR exposes one KDF shape:
//!
//! - [`traits::ConsumerKdf`] — observer over batches; returns `Continue` / `Break` for early
//!   termination. Wired into the plan via the
//!   [`SinkType::Consume`](crate::plans::ir::nodes::SinkType::Consume) sink — the
//!   consumer drains the terminal row stream, accumulating its own finalized state for the
//!   engine to harvest after the plan completes.
//!
//! `ConsumerKdf` extends a small supertrait [`traits::Kdf`] carrying
//! `kdf_id()` and `finish()`. KDFs are dispatched in-process and never cross
//! a serialization boundary (the sink is an opaque pointer to the engine).
//!
//! # Identity
//!
//! - [`token::KdfStateToken`] — `{ kdf_id, id }` stamped at plan-build time. Keys the
//!   executor's state table.
//! - [`trace::TraceContext`] — `{ sm, phase }` stamped at phase-execute time. Lives on handles;
//!   used by tracing and cross-check validations.
//!
//! # Handles
//!
//! - [`handle::Handle<K>`] — generic runtime state. Executor code holds `Handle<dyn
//!   ConsumerKdf>` directly.
//!
//! # Adding a KDF
//!
//! New file with four impl blocks (`Kdf`, `ConsumerKdf`, `KdfOutput`, `RowVisitor`), one
//! line in the submodule mod.rs, one line re-exporting here. Row-ordering requirements
//! live on the KDF trait itself — no separate factory.
//!
//! [`ConsumeSink`]: crate::plans::ir::nodes::ConsumeSink

#[macro_use]
mod macros;

pub mod handle;
pub mod state;
pub mod token;
pub mod trace;
pub mod traits;
pub mod typed;

pub use handle::{FinishedHandle, Handle};
pub use state::consumer::{CheckpointHintReader, MetadataProtocolReader, SidecarCollector};
pub use token::{ConsumerKdfId, KdfStateToken};
pub use trace::TraceContext;
pub use traits::{ConsumerKdf, Kdf, KdfControl};
pub use typed::{downcast_all, take_single, Extractor, KdfOutput};
