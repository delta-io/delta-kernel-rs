use std::collections::HashMap;
use std::sync::Arc;

use super::declarative::PlanBuilder;
use super::nodes::{FileType, RelationHandle};
use crate::expressions::ColumnName;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::schema::{arc_schema, SchemaRef};

/// Coroutine-scoped registry for relation-name lookups and sink registration.
#[derive(Default, Debug)]
pub struct RelationRegistry {
    by_name: HashMap<String, RelationHandle>,
}

impl RelationRegistry {
    pub fn new() -> Self {
        Self::default()
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
        Ok(PlanBuilder::from_registered_relation(handle))
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

    /// Register an already-minted handle under its diagnostic name.
    ///
    /// Useful when relation handles are produced by a previous build stage and
    /// reused in the current coroutine step.
    pub fn register_existing(&mut self, handle: RelationHandle) -> Result<(), DeltaError> {
        let name = handle.name.clone();
        if self.by_name.contains_key(&name) {
            return Err(crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "relation `{name}` is already registered",
            ));
        }
        self.by_name.insert(name, handle);
        Ok(())
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
            let field = scan_schema.field(leaf_name.as_str()).cloned().ok_or_else(|| {
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

    fn register_sink(&mut self, name: &str, schema: SchemaRef) -> Result<RelationHandle, DeltaError> {
        if self.by_name.contains_key(name) {
            return Err(crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "relation `{name}` is already registered",
            ));
        }
        let handle = RelationHandle::fresh(name, Arc::clone(&schema));
        self.by_name.insert(name.to_string(), handle.clone());
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField};

    fn one_col_schema(name: &str) -> SchemaRef {
        arc_schema([StructField::not_null(name, DataType::LONG)])
    }

    #[test]
    fn relation_ref_unknown_name_errors() {
        let registry = RelationRegistry::new();
        assert!(registry.relation_ref("missing").is_err());
    }

    #[test]
    fn duplicate_registration_errors() {
        let mut registry = RelationRegistry::new();
        let schema = one_col_schema("a");
        registry
            .register_relation_sink("dup", Arc::clone(&schema))
            .unwrap();
        assert!(registry.register_relation_sink("dup", schema).is_err());
    }

    #[test]
    fn drop_then_ref_fails() {
        let mut registry = RelationRegistry::new();
        registry
            .register_relation_sink("tmp", one_col_schema("x"))
            .unwrap();
        registry.drop_relation("tmp").unwrap();
        assert!(registry.relation_ref("tmp").is_err());
    }
}
