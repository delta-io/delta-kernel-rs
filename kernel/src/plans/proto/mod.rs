//! Protobuf wire format mirroring the kernel's plan / schema / expression IR.
//!
//! Each submodule wraps the prost-generated code for one `.proto` file in
//! `kernel/proto/`. Consumers can serialise a kernel-built plan and ship it
//! across a process / language boundary (Rust kernel -> JVM engine, etc.).
//!
//! - [`schema`] -- mirror of `kernel/src/schema/mod.rs` (`DataType`, `PrimitiveType`, `StructType`,
//!   ...).
//! - [`expressions`] -- mirror of `kernel/src/expressions/mod.rs` and `scalars.rs` (`Expression`,
//!   `Predicate`, `Scalar`, `ColumnName`, ...).
//! - [`plan`] -- mirror of `kernel/src/plans/ir/{plan,nodes}.rs` (`Plan`, `PlanNode`, `Operator`,
//!   per-variant payload messages).
//! - [`operation`] -- mirror of `kernel/src/plans/ir/operation.rs` (`Operation`, `IoOperation`, and
//!   the I/O payload messages).

use std::sync::Arc;

use prost::Message as _;

use crate::schema::{SchemaRef, StructType};
use crate::{KernelError, KernelResult};

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.schema.rs"));
}

pub mod expressions {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.expressions.rs"));
}

pub mod plan {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.plan.rs"));
}

// The top-level `Operation` message's oneof generates a nested `mod operation`, which trips
// `clippy::module_inception` against this same-named module. The name mirrors the `.proto` file
// (matching the other proto modules), so suppress the lint rather than rename.
#[allow(clippy::module_inception)]
pub mod operation {
    include!(concat!(env!("OUT_DIR"), "/delta.kernel.operation.rs"));
}

mod convert;

/// Encodes a logical table schema using the declarative-plan protobuf, including field metadata.
///
/// Variant types are opaque in this format. Their inner field structure is not encoded, and
/// decoding produces the canonical unshredded Variant type.
pub fn encode_schema(value: &StructType) -> Vec<u8> {
    schema::StructType::from(value).encode_to_vec()
}

/// Decodes a logical table schema and validates its structural invariants.
///
/// Variant types decode to the canonical unshredded form, as described in [`encode_schema`].
/// Returns an error for malformed protobuf or an invalid schema.
pub fn decode_schema(bytes: &[u8]) -> KernelResult<SchemaRef> {
    let value = schema::StructType::decode(bytes).map_err(KernelError::generic_err)?;
    Ok(Arc::new(StructType::try_from(value)?))
}

/// Decodes unchanged logical schema `bytes` exported from a validated snapshot by this Kernel
/// build.
///
/// Variant types decode to the canonical unshredded form, as described in [`encode_schema`].
///
/// The caller must preserve the source schema, including field metadata. This entry point is
/// not for connector-authored schemas or state from a different Kernel build. It relies on the
/// source having checked case-insensitive duplicate field names, duplicate metadata-column
/// specifications, and nested metadata columns.
/// Protobuf decoding, required values, scalar parameters, metadata, and lookup indexes still run.
/// It does not skip table configuration or operation-support checks.
///
/// Returns an error for malformed protobuf or invalid scalar parameters.
pub fn decode_trusted_schema(bytes: &[u8]) -> KernelResult<SchemaRef> {
    let value = schema::StructType::decode(bytes).map_err(KernelError::generic_err)?;
    Ok(Arc::new(convert::trusted_struct_type_from_proto(value)?))
}

#[cfg(test)]
mod schema_transport_tests {
    use super::*;

    #[test]
    fn complete_schema_round_trips_including_nested_metadata() {
        let value: StructType = serde_json::from_str(
            r#"{"type":"struct","fields":[
              {"name":"value","type":"string","nullable":true,
               "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"p1"}},
              {"name":"nested","type":{"type":"struct","fields":[
                {"name":"items","type":{"type":"array","elementType":"integer",
                 "containsNull":false},"nullable":true,"metadata":{"custom":{"a":7}}}
              ]},"nullable":false,"metadata":{}},
              {"name":"mapping","type":{"type":"map","keyType":"string",
               "valueType":"long","valueContainsNull":true},"nullable":true,"metadata":{}}
            ]}"#,
        )
        .unwrap();
        let bytes = encode_schema(&value);
        assert_eq!(decode_schema(&bytes).unwrap().as_ref(), &value);
        assert_eq!(decode_trusted_schema(&bytes).unwrap().as_ref(), &value);
    }

    #[test]
    fn trusted_schema_still_rejects_bad_wire_data_and_missing_types() {
        assert!(decode_trusted_schema(&[0xff; 8]).is_err());
        let value = schema::StructType {
            fields: vec![schema::StructField {
                name: "value".into(),
                data_type: None,
                nullable: true,
                metadata: Default::default(),
            }],
        };
        assert!(decode_trusted_schema(&value.encode_to_vec()).is_err());
    }
}
