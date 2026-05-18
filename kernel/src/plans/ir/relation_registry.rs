use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;

use super::declarative::PlanBuilder;
use super::nodes::{FileType, RelationHandle};
use crate::expressions::ColumnName;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::schema::{arc_schema, SchemaRef};

/// Coroutine-scoped registry for relation-name lookups and sink registration.
///
/// Owned by an SM `Context` for the lifetime of one SM body. Each registry is keyed by an
/// `sm_id` so that handle identities are deterministically derived from `(sm_id, logical_name)`
/// and never collide across SMs.
#[derive(Debug)]
pub struct RelationRegistry {
    by_name: HashMap<String, RelationHandle>,
    sm_id: Uuid,
}

impl RelationRegistry {
    /// Build a registry owned by the SM identified by `sm_id`. Handles minted by this registry
    /// carry an `id` derived from `(sm_id, logical_name)`, embedding the originating SM into
    /// every relation's identity. Synchronous / test callers that have no real SM mint a
    /// throwaway `Uuid::new_v4()`.
    pub fn new(sm_id: Uuid) -> Self {
        Self {
            by_name: HashMap::new(),
            sm_id,
        }
    }

    /// Build a relation-reference source for `name`.
    ///
    /// Errors immediately when `name` is unknown.
    pub fn relation_ref(&self, name: &str) -> Result<PlanBuilder, DeltaError> {
        let handle = self.by_name.get(name).cloned().ok_or_else(|| {
            crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "relation_ref: unknown relation `{name}`",
            )
        })?;
        Ok(PlanBuilder::from_relation_handle(handle))
    }

    /// Remove a relation name from lookup.
    ///
    /// Existing plans built against the old handle are unaffected.
    pub fn drop_relation(&mut self, name: &str) -> Result<(), DeltaError> {
        if self.by_name.remove(name).is_some() {
            Ok(())
        } else {
            Err(crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "drop_relation: unknown relation `{name}`",
            ))
        }
    }

    pub(crate) fn register_relation_sink(
        &mut self,
        name: &str,
        schema: SchemaRef,
    ) -> Result<RelationHandle, DeltaError> {
        self.register_sink(name, schema)
    }

    pub(crate) fn register_load_sink(
        &mut self,
        name: &str,
        scan_schema: &SchemaRef,
        file_schema: &SchemaRef,
        passthrough_columns: &[ColumnName],
        _file_type: FileType,
    ) -> Result<RelationHandle, DeltaError> {
        let mut output_fields: Vec<_> = file_schema.fields().cloned().collect();
        for col in passthrough_columns {
            let Some(leaf_name) = col.path().first() else {
                return Err(crate::delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    "load sink passthrough column cannot have an empty path",
                ));
            };
            let field = scan_schema
                .field(leaf_name.as_str())
                .cloned()
                .ok_or_else(|| {
                    crate::delta_error!(
                        DeltaErrorCode::DeltaCommandInvariantViolation,
                        "load sink passthrough column `{}` not found in scan schema",
                        leaf_name
                    )
                })?;
            output_fields.push(field);
        }
        self.register_sink(name, arc_schema(output_fields))
    }

    fn register_sink(
        &mut self,
        name: &str,
        schema: SchemaRef,
    ) -> Result<RelationHandle, DeltaError> {
        if self.by_name.contains_key(name) {
            return Err(crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "relation `{name}` is already registered",
            ));
        }
        let handle = self.mint_handle(name, Arc::clone(&schema));
        self.by_name.insert(name.to_string(), handle.clone());
        Ok(handle)
    }

    /// Mint a `RelationHandle` for a registry-owned name. The `id` field encodes both the
    /// owning SM (`sm_id`) and the logical name, so two relations with the same logical name
    /// produced by different SMs are still distinct, while lookups inside one SM stay stable.
    fn mint_handle(&self, logical_name: &str, schema: SchemaRef) -> RelationHandle {
        RelationHandle {
            name: logical_name.to_string(),
            id: format!("{}_{}", self.sm_id, logical_name),
            schema,
        }
    }

    /// Sorted snapshot of currently-registered logical relation names. Sorted to make external
    /// consumers (tracing, parity tests, executor diagnostics) deterministic.
    pub fn live_relations(&self) -> Vec<String> {
        let mut names: Vec<String> = self.by_name.keys().cloned().collect();
        names.sort();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField};

    fn one_col_schema(name: &str) -> SchemaRef {
        arc_schema([StructField::not_null(name, DataType::LONG)])
    }

    fn registry() -> RelationRegistry {
        RelationRegistry::new(Uuid::new_v4())
    }

    #[test]
    fn relation_ref_unknown_name_errors() {
        let registry = registry();
        assert!(registry.relation_ref("missing").is_err());
    }

    #[test]
    fn duplicate_registration_errors() {
        let mut registry = registry();
        let schema = one_col_schema("a");
        registry
            .register_relation_sink("dup", Arc::clone(&schema))
            .unwrap();
        assert!(registry.register_relation_sink("dup", schema).is_err());
    }

    #[test]
    fn drop_then_ref_fails() {
        let mut registry = registry();
        registry
            .register_relation_sink("tmp", one_col_schema("x"))
            .unwrap();
        registry.drop_relation("tmp").unwrap();
        assert!(registry.relation_ref("tmp").is_err());
    }

    #[test]
    fn live_relations_is_sorted_snapshot() {
        let mut registry = registry();
        registry
            .register_relation_sink("zeta", one_col_schema("a"))
            .unwrap();
        registry
            .register_relation_sink("alpha", one_col_schema("a"))
            .unwrap();
        registry
            .register_relation_sink("mu", one_col_schema("a"))
            .unwrap();
        assert_eq!(registry.live_relations(), vec!["alpha", "mu", "zeta"]);
    }

    #[test]
    fn live_relations_reflects_drops() {
        let mut registry = registry();
        registry
            .register_relation_sink("a", one_col_schema("x"))
            .unwrap();
        registry
            .register_relation_sink("b", one_col_schema("x"))
            .unwrap();
        registry.drop_relation("a").unwrap();
        assert_eq!(registry.live_relations(), vec!["b"]);
    }

    #[test]
    fn live_relations_empty_when_no_sinks() {
        assert!(registry().live_relations().is_empty());
    }

    #[test]
    fn handle_id_embeds_sm_id_and_logical_name() {
        let sm_id = Uuid::new_v4();
        let mut registry = RelationRegistry::new(sm_id);
        let handle = registry
            .register_relation_sink("commits", one_col_schema("v"))
            .unwrap();
        assert_eq!(handle.name, "commits");
        assert_eq!(handle.id, format!("{}_commits", sm_id));
    }

    #[test]
    fn handles_from_different_registries_have_distinct_ids() {
        let mut r1 = registry();
        let mut r2 = registry();
        let h1 = r1
            .register_relation_sink("shared", one_col_schema("v"))
            .unwrap();
        let h2 = r2
            .register_relation_sink("shared", one_col_schema("v"))
            .unwrap();
        assert_ne!(h1.id, h2.id);
        assert_eq!(h1.name, h2.name);
    }
}
