//! Declarative newest-action-wins replay shared by scans and checkpoint reconstruction.
//!
//! A replay group defines both the actions that compete and their identity. Retention, metadata
//! pruning, and output representation belong to the caller; replay preserves winning tombstones.

use std::sync::Arc;

use crate::actions::{
    ADD_FIELD, ADD_NAME, DOMAIN_METADATA_FIELD, METADATA_FIELD, PROTOCOL_FIELD, REMOVE_FIELD,
    REMOVE_NAME, SET_TRANSACTION_FIELD, action_presence_leaf,
};
use crate::expressions::{Expression as Expr, Predicate, col, column_name, lit, null_lit};
use crate::schema::{DataType, SchemaRef, StructField, StructType, schema};
use crate::{KernelError, KernelResult, PlanBuilder};

mod sources;
pub(crate) use sources::checkpoint_actions;

const ACTION_KEY: &str = "action_key";
const PAYLOAD: &str = "payload";
pub(crate) const VERSION: &str = "version";

/// Actions that share a latest-value identity. Adds and removes must replay together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplayGroup {
    Files,
    Protocol,
    Metadata,
    Transactions,
    Domains,
}

/// Equivalent reconciliation strategies; retention is applied by the caller afterward.
#[derive(Clone, Copy)]
pub(crate) enum ReplayStrategy {
    #[cfg_attr(not(test), allow(dead_code))]
    AggregateAll,
    AggregateTailAndAntiJoin,
}

/// Compiles selected action groups into a schema, presence predicate, and identity lanes.
pub(crate) struct ActionReplay {
    groups: Vec<ReplayGroup>,
}

impl ActionReplay {
    /// Selects groups in canonical order, ignoring duplicate selections.
    pub(crate) fn new(groups: &[ReplayGroup]) -> Self {
        use ReplayGroup::*;
        Self {
            groups: [Files, Protocol, Metadata, Transactions, Domains]
                .into_iter()
                .filter(|group| groups.contains(group))
                .collect(),
        }
    }

    /// Canonical log fields needed by the selected groups, including the version scan tag.
    pub(crate) fn read_schema(&self) -> KernelResult<SchemaRef> {
        Ok(Arc::new(StructType::try_new(
            self.fields()
                .chain([StructField::nullable(VERSION, DataType::LONG)]),
        )?))
    }

    /// Reconciles an already-reconciled base with strictly newer version-tagged tail actions.
    ///
    /// Source arms may add fields under an action (e.g. parsed scan statistics), but must agree
    /// on their types. A missing base action column is padded with typed NULL, allowing CRC adds
    /// to serve as a scan base. The caller must choose a base complete for its requested state:
    /// CRC active adds cannot supply checkpoint tombstones. Output contains selected actions only.
    pub(crate) fn replay(&self, base: PlanBuilder, tail: PlanBuilder) -> KernelResult<PlanBuilder> {
        self.replay_with_strategy(base, tail, ReplayStrategy::AggregateTailAndAntiJoin)
    }

    pub(crate) fn replay_with_strategy(
        &self,
        base: PlanBuilder,
        tail: PlanBuilder,
        strategy: ReplayStrategy,
    ) -> KernelResult<PlanBuilder> {
        let payload_schema =
            Arc::new(StructType::try_new(self.fields().map(|field| {
                tail.schema().field(field.name()).cloned().unwrap_or(field)
            }))?);
        let base = self.pack(base, &payload_schema)?;
        let tail = self.pack(tail, &payload_schema)?;

        if matches!(strategy, ReplayStrategy::AggregateAll) {
            return latest_by_key(PlanBuilder::union_all([base, tail])?)?
                .project(col!(PAYLOAD), payload_schema);
        }
        // Aggregate the whole action row atomically. A remove or domain deletion remains a
        // winning payload and its key cancels the corresponding base value before filtering.
        let winners = latest_by_key(tail)?;
        let surviving_base = base
            .anti_join(
                winners.clone(),
                [column_name!(ACTION_KEY)],
                [column_name!(ACTION_KEY)],
            )?
            .project(col!(PAYLOAD), payload_schema.clone())?;
        let tail = winners.project(col!(PAYLOAD), payload_schema)?;
        PlanBuilder::union_all([tail, surviving_base])
    }

    /// Replays only a bounded commit interval, preserving winning tombstones without retention.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn replay_commits(&self, commits: PlanBuilder) -> KernelResult<PlanBuilder> {
        let schema = Arc::new(StructType::try_new(self.fields())?);
        latest_by_key(self.pack(commits, &schema)?)?.project(col!(PAYLOAD), schema)
    }

    fn fields(&self) -> impl Iterator<Item = StructField> + '_ {
        self.groups.iter().flat_map(|group| match group {
            ReplayGroup::Files => vec![ADD_FIELD.clone(), REMOVE_FIELD.clone()],
            ReplayGroup::Protocol => vec![PROTOCOL_FIELD.clone()],
            ReplayGroup::Metadata => vec![METADATA_FIELD.clone()],
            ReplayGroup::Transactions => vec![SET_TRANSACTION_FIELD.clone()],
            ReplayGroup::Domains => vec![DOMAIN_METADATA_FIELD.clone()],
        })
    }

    fn pack(&self, input: PlanBuilder, payload_schema: &SchemaRef) -> KernelResult<PlanBuilder> {
        // Normalize missing columns before building keys; keep metadata shaping outside replay.
        let mut fields: Vec<_> = payload_schema.fields().cloned().collect();
        fields.push(StructField::nullable(VERSION, DataType::LONG));
        let mut values = Vec::with_capacity(fields.len());
        for field in &fields {
            values.push(match input.schema().field(field.name()) {
                Some(actual) if actual.data_type() == field.data_type() => {
                    Expr::column([field.name()])
                }
                Some(_) => {
                    return Err(KernelError::schema(format!(
                        "Replay sources disagree on the type of '{}'",
                        field.name()
                    )));
                }
                None => null_lit(field.data_type().clone()),
            });
        }
        let input = input.project(
            Expr::struct_from(values),
            Arc::new(StructType::try_new(fields)?),
        )?;
        let presence = Predicate::or_from(self.fields().filter_map(|field| {
            action_presence_leaf(field.name())
                .map(|leaf| Expr::column([field.name(), leaf]).is_not_null())
        }));
        let lanes: Vec<_> = self.groups.iter().map(|group| lane(*group)).collect();
        let key_schema = StructType::try_new(lanes.iter().map(|(field, _)| field.clone()))?;
        let schema = Arc::new(StructType::try_new([
            StructField::not_null(ACTION_KEY, key_schema),
            StructField::not_null(PAYLOAD, payload_schema.clone()),
            StructField::nullable(VERSION, DataType::LONG),
        ])?);
        input.filter(presence)?.project(
            Expr::struct_from([
                Expr::struct_from(lanes.into_iter().map(|(_, expr)| expr)),
                Expr::struct_from(
                    payload_schema
                        .fields()
                        .map(|field| Expr::column([field.name()])),
                ),
                col!(VERSION),
            ]),
            schema,
        )
    }
}

fn latest_by_key(tail: PlanBuilder) -> KernelResult<PlanBuilder> {
    tail.aggregate_by([column_name!(ACTION_KEY)], |a| {
        a.max_non_null_by(
            column_name!(PAYLOAD),
            column_name!(ACTION_KEY),
            column_name!(VERSION),
        )
    })
}

/// Each lane is NULL on other action kinds. Singleton lanes use markers, not changing values.
fn lane(group: ReplayGroup) -> (StructField, Expr) {
    match group {
        ReplayGroup::Files => {
            let key_col = |name: &str| {
                Expr::coalesce([
                    Expr::column(std::iter::once(ADD_NAME).chain(name.split('.'))),
                    Expr::column(std::iter::once(REMOVE_NAME).chain(name.split('.'))),
                ])
            };
            let path = key_col("path");
            let storage_type = key_col("deletionVector.storageType");
            let schema = schema! {
                nullable "path": STRING,
                nullable "deletionVector": {
                    not_null "storageType": STRING,
                    not_null "pathOrInlineDv": STRING,
                    nullable "offset": INTEGER,
                },
            };
            let expr = Expr::struct_with_nullability_from(
                [
                    path.clone(),
                    Expr::struct_with_nullability_from(
                        [
                            storage_type.clone(),
                            key_col("deletionVector.pathOrInlineDv"),
                            key_col("deletionVector.offset"),
                        ],
                        Expr::from_pred(storage_type.is_not_null()),
                    ),
                ],
                Expr::from_pred(path.is_not_null()),
            );
            (StructField::nullable("file", schema), expr)
        }
        ReplayGroup::Protocol => (
            StructField::nullable("protocol", DataType::BOOLEAN),
            Expr::case_when(
                [(col!("protocol.minReaderVersion").is_not_null(), lit(true))],
                null_lit(DataType::BOOLEAN),
            ),
        ),
        ReplayGroup::Metadata => (
            StructField::nullable("metadata", DataType::BOOLEAN),
            Expr::case_when(
                [(col!("metaData.id").is_not_null(), lit(true))],
                null_lit(DataType::BOOLEAN),
            ),
        ),
        ReplayGroup::Transactions => (
            StructField::nullable("transaction", DataType::STRING),
            col!("txn.appId"),
        ),
        ReplayGroup::Domains => (
            StructField::nullable("domain", DataType::STRING),
            col!("domainMetadata.domain"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine as _;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::plans::Operation;
    use crate::schema::schema_ref;

    #[rstest::rstest]
    fn replay_strategies_keep_winning_domain_deletions(
        #[values(false, true)] aggregate_all: bool,
        #[values(false, true)] commits_only: bool,
    ) -> KernelResult<()> {
        let replay = ActionReplay::new(&[ReplayGroup::Domains]);
        let input = |version: i64, removed: bool| {
            PlanBuilder::values(
                schema_ref! {
                    nullable "domainMetadata": { not_null "domain": STRING, not_null "configuration": STRING, not_null "removed": BOOLEAN },
                    nullable VERSION: LONG,
                },
                vec![vec![
                    crate::expressions::StructData::try_new(
                        vec![
                            StructField::not_null("domain", DataType::STRING),
                            StructField::not_null("configuration", DataType::STRING),
                            StructField::not_null("removed", DataType::BOOLEAN),
                        ],
                        vec!["example".into(), "{}".into(), removed.into()],
                    )?
                    .into(),
                    version.into(),
                ]],
            )
        };
        let tail = PlanBuilder::union_all([input(1, false)?, input(2, true)?])?;
        let plan = if commits_only {
            replay.replay_commits(tail)?
        } else {
            replay.replay_with_strategy(
                input(0, false)?,
                tail,
                if aggregate_all {
                    ReplayStrategy::AggregateAll
                } else {
                    ReplayStrategy::AggregateTailAndAntiJoin
                },
            )?
        };
        let engine = SyncEngine::new();
        let mut batches = engine
            .require_plan_executor()?
            .execute_op(Operation::QueryPlan(plan.build()?))?
            .into_data()?;
        use crate::arrow::array::AsArray as _;
        let mut rows = 0;
        for batch in &mut batches {
            let batch = batch?.try_into_record_batch()?;
            let domain = batch.column_by_name("domainMetadata").unwrap().as_struct();
            for index in 0..batch.num_rows() {
                assert!(
                    domain
                        .column_by_name("removed")
                        .unwrap()
                        .as_boolean()
                        .value(index)
                );
            }
            rows += batch.num_rows();
        }
        assert_eq!(rows, 1);
        Ok(())
    }
}
