use std::collections::HashMap;
use std::sync::Arc;

use uuid::Uuid;

use super::declarative::PlanBuilder;
use super::nodes::{FileType, RelationHandle};
use super::plan::{Plan, ResultPlan};
use crate::expressions::ColumnName;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::schema::{arc_schema, SchemaRef};

/// Coroutine-scoped registry for relation-name lookups, sink registration, and plan
/// accumulation.
///
/// Owned by an SM `Context` for the lifetime of one SM body. Each registry is keyed by:
///
/// - `sm_id` (Uuid): mints unique `RelationHandle::id` values so handles never collide across
///   SMs even when they share a logical relation name.
/// - `sm_name` (`&'static str`): namespace prefix prepended to every relation name in
///   `by_name`. SM bodies pass bare names ("commit_raw") and the registry stores them under
///   the prefixed full name ("scan.commit_raw"); trace output and `live_relations()` show the
///   prefixed names. Pass `""` for no prefix.
///
/// The registry also owns a private `plans: Vec<Plan>` accumulator. Every plan built via
/// `PlanBuilder::into_relation` / `::load` lands here as a side effect. SM bodies never see
/// the accumulator directly; they drain it via [`Self::into_result_plan`] (terminal) or via
/// `PlanBuilder::consume_phase` (mid-coroutine phase yield).
#[derive(Debug)]
pub struct RelationRegistry {
    by_name: HashMap<String, RelationHandle>,
    plans: Vec<Plan>,
    sm_id: Uuid,
    sm_name: &'static str,
}

impl RelationRegistry {
    /// Build a registry owned by the SM identified by `(sm_id, sm_name)`.
    ///
    /// Handles minted by this registry carry an `id` derived from `(sm_id, full_name)` (where
    /// `full_name` is `"{sm_name}.{name}"` when `sm_name` is non-empty), embedding the
    /// originating SM into every relation's identity. Synchronous / test callers that have no
    /// real SM mint a throwaway `Uuid::new_v4()` and pass `""` for `sm_name`.
    pub fn new(sm_id: Uuid, sm_name: &'static str) -> Self {
        Self {
            by_name: HashMap::new(),
            plans: Vec::new(),
            sm_id,
            sm_name,
        }
    }

    /// Internal: prepend the registry's `sm_name` to `name` so two pipelines (e.g. "scan" and
    /// "fsr") that use the same logical name within their SMs never collide in `by_name`.
    fn full(&self, name: &str) -> String {
        if self.sm_name.is_empty() {
            name.to_string()
        } else {
            format!("{}.{}", self.sm_name, name)
        }
    }

    /// Build a relation-reference source for `name`.
    ///
    /// Errors immediately when `name` is unknown.
    pub fn relation_ref(&self, name: &str) -> Result<PlanBuilder, DeltaError> {
        let full = self.full(name);
        let handle = self.by_name.get(&full).cloned().ok_or_else(|| {
            crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "relation_ref: unknown relation `{full}`",
            )
        })?;
        Ok(PlanBuilder::from_relation_handle(handle))
    }

    /// Remove a relation name from lookup.
    ///
    /// Existing plans built against the old handle are unaffected.
    pub fn drop_relation(&mut self, name: &str) -> Result<(), DeltaError> {
        let full = self.full(name);
        if self.by_name.remove(&full).is_some() {
            Ok(())
        } else {
            Err(crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "drop_relation: unknown relation `{full}`",
            ))
        }
    }

    /// Adopt an externally-minted [`RelationHandle`] under `name` so subsequent
    /// `relation_ref(name)` calls find it.
    ///
    /// Used when bridging across SM executions: e.g. `scan_data_from_metadata_state_machine`
    /// receives a `live_actions` handle that was minted by a previous scan-metadata SM run,
    /// adopts it under the locally-known name, then builds the data-phase chain on top.
    pub fn adopt(
        &mut self,
        name: &str,
        handle: RelationHandle,
    ) -> Result<(), DeltaError> {
        let full = self.full(name);
        if self.by_name.contains_key(&full) {
            return Err(crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "adopt: relation `{full}` is already registered",
            ));
        }
        self.by_name.insert(full, handle);
        Ok(())
    }

    /// Drain the registry's accumulated plans into a [`ResultPlan`] whose terminal relation
    /// is the one previously registered under `terminal_name`.
    ///
    /// Side effect: empties the internal plan accumulator. Registered names persist (in case
    /// the same registry is reused for a follow-up phase, though that's atypical).
    ///
    /// Errors if `terminal_name` is not registered or if the accumulator is empty.
    pub fn into_result_plan(&mut self, terminal_name: &str) -> Result<ResultPlan, DeltaError> {
        let full = self.full(terminal_name);
        let handle = self.by_name.get(&full).cloned().ok_or_else(|| {
            crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "into_result_plan: unknown terminal relation `{full}`",
            )
        })?;
        let plans = std::mem::take(&mut self.plans);
        if plans.is_empty() {
            return Err(crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "into_result_plan: no plans were accumulated for terminal `{full}`",
            ));
        }
        Ok(ResultPlan::new(plans, handle))
    }

    /// Drain the registry's accumulated plans, returning the vector. Names persist; the
    /// accumulator is emptied. Used by [`PlanBuilder::consume_phase`] to build the
    /// `PhaseOperation::Plans(...)` payload for a mid-coroutine phase yield, and by tests
    /// / advanced external callers that need to feed plans directly into an executor.
    pub fn take_plans(&mut self) -> Vec<Plan> {
        std::mem::take(&mut self.plans)
    }

    /// Crate-internal: append a built [`Plan`] to the accumulator. Called by
    /// [`PlanBuilder::into_relation`] and [`PlanBuilder::load`] as a side effect after they
    /// register the plan's terminal sink.
    pub(crate) fn push_plan(&mut self, plan: Plan) {
        self.plans.push(plan);
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
        let full = self.full(name);
        if self.by_name.contains_key(&full) {
            return Err(crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "relation `{full}` is already registered",
            ));
        }
        let handle = self.mint_handle(&full, Arc::clone(&schema));
        self.by_name.insert(full, handle.clone());
        Ok(handle)
    }

    /// Mint a `RelationHandle` for a registry-owned (already-prefixed) name. The `id` field
    /// encodes both the owning SM (`sm_id`) and the full name, so two relations with the same
    /// full name produced by different SMs are still distinct, while lookups inside one SM
    /// stay stable.
    fn mint_handle(&self, full_name: &str, schema: SchemaRef) -> RelationHandle {
        RelationHandle {
            name: full_name.to_string(),
            id: format!("{}_{}", self.sm_id, full_name),
            schema,
        }
    }

    /// Sorted snapshot of currently-registered logical relation names (already prefixed by
    /// `sm_name`). Sorted to make external consumers (tracing, parity tests, executor
    /// diagnostics) deterministic.
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
        RelationRegistry::new(Uuid::new_v4(), "")
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
        let mut registry = RelationRegistry::new(sm_id, "");
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

    #[test]
    fn sm_name_prefix_is_prepended_to_full_name() {
        let sm_id = Uuid::new_v4();
        let mut registry = RelationRegistry::new(sm_id, "scan");
        let handle = registry
            .register_relation_sink("commit_raw", one_col_schema("v"))
            .unwrap();
        assert_eq!(handle.name, "scan.commit_raw");
        assert_eq!(handle.id, format!("{}_scan.commit_raw", sm_id));
        assert_eq!(registry.live_relations(), vec!["scan.commit_raw"]);
    }

    #[test]
    fn sm_name_prefix_lets_callers_use_bare_names() {
        let sm_id = Uuid::new_v4();
        let mut registry = RelationRegistry::new(sm_id, "scan");
        registry
            .register_relation_sink("commit_raw", one_col_schema("v"))
            .unwrap();
        // Caller uses the bare name; registry resolves to the prefixed one.
        assert!(registry.relation_ref("commit_raw").is_ok());
        // Empty prefix: same bare lookup works.
        let mut r2 = RelationRegistry::new(Uuid::new_v4(), "");
        r2.register_relation_sink("commit_raw", one_col_schema("v"))
            .unwrap();
        assert!(r2.relation_ref("commit_raw").is_ok());
    }

    #[test]
    fn adopt_externally_minted_handle_is_referenceable_by_name() {
        let outer_sm = Uuid::new_v4();
        let mut outer = RelationRegistry::new(outer_sm, "outer");
        let live_handle = outer
            .register_relation_sink("live_actions", one_col_schema("v"))
            .unwrap();
        let _ = outer.take_plans(); // drain so we don't confuse follow-up

        let mut inner = RelationRegistry::new(Uuid::new_v4(), "scan");
        inner.adopt("live_actions", live_handle.clone()).unwrap();
        let pb = inner.relation_ref("live_actions").unwrap();
        assert_eq!(pb.schema_ref(), live_handle.schema);
    }

    #[test]
    fn adopt_rejects_collision() {
        let mut r = RelationRegistry::new(Uuid::new_v4(), "scan");
        let h = RelationHandle::fresh("scan.live", one_col_schema("v"));
        r.adopt("live", h.clone()).unwrap();
        let err = r.adopt("live", h).unwrap_err();
        assert!(format!("{err}").contains("already registered"));
    }

    #[test]
    fn into_result_plan_drains_accumulator_and_returns_terminal_handle() {
        use crate::plans::ir::nodes::SinkType;
        let mut r = RelationRegistry::new(Uuid::new_v4(), "");
        // Manually push a fake plan so we don't have to wire a full PlanBuilder chain here.
        let handle = r
            .register_relation_sink("t", one_col_schema("v"))
            .unwrap();
        let dummy = Plan::new(
            crate::plans::ir::DeclarativePlanNode::RelationRef(handle.clone()),
            SinkType::Relation(handle.clone()),
        );
        r.push_plan(dummy);
        let rp = r.into_result_plan("t").unwrap();
        assert_eq!(rp.plans.len(), 1);
        assert_eq!(rp.result_relation.id, handle.id);
        // Plans were drained.
        assert!(r.take_plans().is_empty());
    }

    #[test]
    fn into_result_plan_errors_if_terminal_unknown() {
        let mut r = RelationRegistry::new(Uuid::new_v4(), "");
        let h = RelationHandle::fresh("t", one_col_schema("v"));
        let dummy = Plan::new(
            crate::plans::ir::DeclarativePlanNode::RelationRef(h.clone()),
            crate::plans::ir::nodes::SinkType::Relation(h),
        );
        r.push_plan(dummy);
        let err = r.into_result_plan("missing").unwrap_err();
        assert!(format!("{err}").contains("unknown terminal relation"));
    }

    #[test]
    fn into_result_plan_errors_if_empty_accumulator() {
        let mut r = RelationRegistry::new(Uuid::new_v4(), "");
        r.register_relation_sink("t", one_col_schema("v")).unwrap();
        let err = r.into_result_plan("t").unwrap_err();
        assert!(format!("{err}").contains("no plans were accumulated"));
    }
}
