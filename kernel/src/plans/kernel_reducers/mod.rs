//! Kernel-Defined Functions (KDFs) -- stateful per-row logic the kernel owns.
//!
//! KDFs encapsulate Delta-specific per-row work (checkpoint hint extraction,
//! protocol/metadata harvesting, sidecar collection) that engines can't interpret.
//!
//! The IR exposes one KDF shape: [`KernelReducer`], an observer over batches
//! returning `Continue` / `Break`. The reducer is wired into a plan via the
//! `Reduce` request handled by the state-machine framework (declared in a
//! consumer module); the reducer drains the terminal row stream and accumulates
//! finalized state for the engine to harvest.
//!
//! KDFs dispatch in-process and never cross a serialization boundary.
//!
//! Each [`ReducerHandle`] carries a [`KernelReducerToken`] (`{ kind, id }`, stamped at
//! plan-build time, keys the executor's state table and the paired [`Extractor`]).
// Intra-doc links to the state-machine framework / IR sink types are intentionally not
// resolvable in this module slice (consumers live in sibling stacks). Plain-text
// references above keep `cargo doc -D warnings` happy until those modules land.
#![allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]

mod checkpoint_hint;
mod handle;
mod metadata_protocol;
mod reducer;
mod sidecar_collector;

pub use checkpoint_hint::{CheckpointHintReader, CheckpointHintRecord};
pub use handle::{Extractor, FinishedHandle, ReducerHandle};
pub use metadata_protocol::MetadataProtocolReader;
pub use reducer::{
    KdfControl, KernelReducer, KernelReducerKind, KernelReducerOutput, KernelReducerToken,
};
pub use sidecar_collector::SidecarCollector;
