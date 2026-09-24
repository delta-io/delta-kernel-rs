//! Protobuf transport for kernel-owned snapshot state.
//!
//! This module is independent of declarative plan execution so imperative connectors can
//! serialize schema, metadata, and protocol through the same protobuf representation.

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.schema.rs"));
}

pub mod state {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.state.rs"));
}

mod convert;
