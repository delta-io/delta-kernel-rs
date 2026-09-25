//! Protobuf transport for kernel-owned control-plane state.
//!
//! This module is independent of declarative plan execution so imperative connectors can
//! serialize snapshot and scan state through the same protobuf representation.

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.schema.rs"));
}

pub mod state {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.state.rs"));
}

mod convert;
