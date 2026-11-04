//! Declarative action sources and newest-action-wins replay shared by scans and checkpoints.
//!
//! Sources return plans with their actual action and metadata representations. Replay aligns those
//! representations without parsing. Consumers decide when to parse, prune, and apply retention.

use std::sync::Arc;

use crate::actions::{
    ADD_NAME, ActionKind, REMOVE_NAME, action_presence_leaf, get_all_actions_schema,
};
use crate::expressions::{
    ColumnName, Expression as Expr, Predicate, col, column_name, lit, null_lit,
};
use crate::schema::{DataType, SchemaRef, StructField, StructType, schema};
use crate::{KernelError, KernelResult, PlanBuilder};

mod metadata;
mod options;
pub(crate) mod sources;
pub(crate) use metadata::{parse_file_metadata, read_source, synthesize_json_stats};
pub(crate) use options::ActionOptions;

const ACTION_KEY: &str = "action_key";
const PAYLOAD: &str = "payload";
pub(crate) const VERSION: &str = "version";

/// Equivalent ways to reconcile a complete checkpoint or CRC base with its newer commit actions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplayStrategy {
    /// Aggregate the union of base and commit rows by action identity.
    #[cfg_attr(not(test), allow(dead_code))]
    AggregateAll,
    /// Aggregate commits, cancel matching base keys, then union the surviving base rows.
    AggregateTailAndAntiJoin,
}

/// Reconciles version-tagged commits with an already-reconciled checkpoint or CRC base.
///
/// Both inputs must cover the requested identities; a CRC containing only live adds cannot supply
/// checkpoint tombstones. Commit versions must be strictly newer than the base. Output carries the
/// union of input representations and the winning version. Missing fields are padded with NULL;
/// JSON parsing and retention are deferred to the consumer.
pub(crate) fn replay(
    (commits, commit_options): (PlanBuilder, ActionOptions),
    (checkpoint, checkpoint_options): (PlanBuilder, ActionOptions),
    strategy: ReplayStrategy,
) -> KernelResult<(PlanBuilder, ActionOptions)> {
    let mut actions = commit_options.actions.clone();
    for kind in checkpoint_options.actions {
        match kind {
            ActionKind::FileActions { includes_removes } => {
                if let Some(ActionKind::FileActions {
                    includes_removes: flag,
                }) = actions
                    .iter_mut()
                    .find(|kind| matches!(kind, ActionKind::FileActions { .. }))
                {
                    *flag |= includes_removes;
                } else {
                    actions.push(kind);
                }
            }
            _ if !actions.contains(&kind) => actions.push(kind),
            _ => {}
        }
    }
    let mut fields = Vec::new();
    for kind in actions.iter() {
        for name in kind.names() {
            let field = match (
                checkpoint.schema().field(name),
                commits.schema().field(name),
            ) {
                (Some(base), Some(tail)) => merge_field(base, tail)?,
                (Some(field), None) | (None, Some(field)) => field.clone(),
                (None, None) => {
                    return Err(KernelError::schema(format!(
                        "Missing action column '{name}'"
                    )));
                }
            };
            fields.push(field);
        }
    }
    fields.push(StructField::nullable(VERSION, DataType::LONG));
    let payload_schema = Arc::new(StructType::try_new(fields)?);
    let mut options = metadata::read_options(&commit_options, &payload_schema, &payload_schema);
    options.actions = actions.clone();
    let base = pack(checkpoint, &actions, &payload_schema)?;
    let tail = pack(commits, &actions, &payload_schema)?;
    let plan = match strategy {
        ReplayStrategy::AggregateAll => latest_by_key(PlanBuilder::union_all([base, tail])?)?
            .project(col!(PAYLOAD), payload_schema)?,
        ReplayStrategy::AggregateTailAndAntiJoin => {
            // Winning deletions cancel base keys even if retention will later expire them.
            let winners = latest_by_key(tail)?;
            let base = base
                .anti_join(
                    winners.clone(),
                    [column_name!(ACTION_KEY)],
                    [column_name!(ACTION_KEY)],
                )?
                .project(col!(PAYLOAD), payload_schema.clone())?;
            let tail = winners.project(col!(PAYLOAD), payload_schema)?;
            PlanBuilder::union_all([tail, base])?
        }
    };
    Ok((plan, options))
}

/// Selects the latest whole action per identity in a version-tagged commit interval.
/// Retains winning deletions and transaction records without applying retention.
pub(crate) fn replay_commits(
    (commits, options): (PlanBuilder, ActionOptions),
) -> KernelResult<(PlanBuilder, ActionOptions)> {
    let schema = commits.schema().clone();
    let plan =
        latest_by_key(pack(commits, &options.actions, &schema)?)?.project(col!(PAYLOAD), schema)?;
    Ok((plan, options))
}

/// Projects a field to the target type, padding missing fields while preserving parent nulls.
pub(crate) fn align_field(
    actual: Option<&StructField>,
    target: &StructField,
    path: ColumnName,
) -> KernelResult<Expr> {
    let Some(actual) = actual else {
        return Ok(null_lit(target.data_type().clone()));
    };
    if actual.data_type() == target.data_type() {
        return Ok(Expr::column(path));
    }
    let (DataType::Struct(actual), DataType::Struct(target)) =
        (actual.data_type(), target.data_type())
    else {
        return Err(KernelError::schema(format!(
            "Incompatible replay column '{path}'"
        )));
    };
    let fields = target
        .fields()
        .map(|field| {
            align_field(
                actual.field(field.name()),
                field,
                ColumnName::new(path.path().iter().cloned().chain([field.name().clone()])),
            )
        })
        .collect::<KernelResult<Vec<_>>>()?;
    // Rebuilding a nullable action must not turn NULL into a present all-null struct.
    Ok(Expr::struct_with_nullability_from(
        fields,
        Expr::from_pred(Expr::column(path).is_not_null()),
    ))
}

/// Unions compatible fields in left order, appending fields found only on the right.
pub(super) fn merge_schema(left: &StructType, right: &StructType) -> KernelResult<SchemaRef> {
    if left == right {
        return Ok(Arc::new(left.clone()));
    }
    let mut fields = left
        .fields()
        .map(|field| {
            right
                .field(field.name())
                .map_or_else(|| Ok(field.clone()), |other| merge_field(field, other))
        })
        .collect::<KernelResult<Vec<_>>>()?;
    fields.extend(
        right
            .fields()
            .filter(|field| !left.contains(field.name()))
            .cloned(),
    );
    Ok(Arc::new(StructType::try_new(fields)?))
}

fn action_schema(actions: &[ActionKind]) -> KernelResult<SchemaRef> {
    let mut fields = actions
        .iter()
        .copied()
        .flat_map(|kind| kind.names().iter().copied())
        .map(|name| {
            get_all_actions_schema()
                .field(name)
                .cloned()
                .ok_or_else(|| KernelError::schema(format!("Unknown action '{name}'")))
        })
        .collect::<KernelResult<Vec<_>>>()?;
    fields.push(StructField::nullable(VERSION, DataType::LONG));
    Ok(Arc::new(StructType::try_new(fields)?))
}

fn presence_predicate<'a>(fields: impl Iterator<Item = &'a StructField>) -> Predicate {
    Predicate::or_from(fields.filter(|field| field.name() != VERSION).map(|field| {
        Expr::column(
            std::iter::once(field.name().as_str()).chain(action_presence_leaf(field.name())),
        )
        .is_not_null()
    }))
}

// Merge nested metadata columns so JSON commits and native checkpoints can feed the same replay.
fn merge_field(left: &StructField, right: &StructField) -> KernelResult<StructField> {
    if left.data_type() == right.data_type() {
        return Ok(left.clone());
    }
    let (DataType::Struct(left_type), DataType::Struct(right_type)) =
        (left.data_type(), right.data_type())
    else {
        return Err(KernelError::schema(format!(
            "Replay sources disagree on the type of '{}'",
            left.name()
        )));
    };
    Ok(StructField::nullable(
        left.name(),
        merge_schema(left_type, right_type)?,
    ))
}

fn align_schema(input: PlanBuilder, schema: SchemaRef) -> KernelResult<PlanBuilder> {
    let values = schema
        .fields()
        .map(|field| {
            align_field(
                input.schema().field(field.name()),
                field,
                ColumnName::new([field.name()]),
            )
        })
        .collect::<KernelResult<Vec<_>>>()?;
    input.project(Expr::struct_from(values), schema)
}

fn pack(
    input: PlanBuilder,
    actions: &[ActionKind],
    payload_schema: &SchemaRef,
) -> KernelResult<PlanBuilder> {
    if input.schema().field(VERSION).map(StructField::data_type) != Some(&DataType::LONG) {
        return Err(KernelError::schema(
            "Replay sources must carry a long version column",
        ));
    }
    let input = align_schema(input, payload_schema.clone())?;
    let lanes = actions.iter().copied().map(lane).collect::<Vec<_>>();
    let key_schema = StructType::try_new(lanes.iter().map(|(field, _)| field.clone()))?;
    let schema = Arc::new(StructType::try_new([
        StructField::not_null(ACTION_KEY, key_schema),
        StructField::not_null(PAYLOAD, payload_schema.clone()),
        StructField::nullable(VERSION, DataType::LONG),
    ])?);
    input
        .filter(presence_predicate(payload_schema.fields()))?
        .project(
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

fn latest_by_key(input: PlanBuilder) -> KernelResult<PlanBuilder> {
    input.aggregate_by([column_name!(ACTION_KEY)], |a| {
        a.max_non_null_by(
            column_name!(PAYLOAD),
            column_name!(ACTION_KEY),
            column_name!(VERSION),
        )
    })
}

/// Each lane is NULL on other action kinds. Singleton lanes use markers, not changing values.
fn lane(kind: ActionKind) -> (StructField, Expr) {
    match kind {
        ActionKind::FileActions { includes_removes } => {
            let key_col = |name: &str| {
                let add = Expr::column(std::iter::once(ADD_NAME).chain(name.split('.')));
                if includes_removes {
                    Expr::coalesce([
                        add,
                        Expr::column(std::iter::once(REMOVE_NAME).chain(name.split('.'))),
                    ])
                } else {
                    add
                }
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
        ActionKind::Protocol => (
            StructField::nullable("protocol", DataType::BOOLEAN),
            Expr::case_when(
                [(col!("protocol.minReaderVersion").is_not_null(), lit(true))],
                null_lit(DataType::BOOLEAN),
            ),
        ),
        ActionKind::Metadata => (
            StructField::nullable("metadata", DataType::BOOLEAN),
            Expr::case_when(
                [(col!("metaData.id").is_not_null(), lit(true))],
                null_lit(DataType::BOOLEAN),
            ),
        ),
        ActionKind::SetTransaction => (
            StructField::nullable("transaction", DataType::STRING),
            col!("txn.appId"),
        ),
        ActionKind::DomainMetadata => (
            StructField::nullable("domain", DataType::STRING),
            col!("domainMetadata.domain"),
        ),
    }
}

#[cfg(test)]
mod tests;

// Transitional callers still select ReplayGroup; removed by the checkpoint migration.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayGroup {
    Files,
    Protocol,
    Metadata,
    Transactions,
    Domains,
}
pub(crate) struct ActionReplay {
    actions: Vec<ActionKind>,
}
impl ActionReplay {
    pub(crate) fn new(groups: &[ReplayGroup]) -> Self {
        Self {
            actions: groups
                .iter()
                .map(|group| match group {
                    ReplayGroup::Files => ActionKind::FileActions {
                        includes_removes: true,
                    },
                    ReplayGroup::Protocol => ActionKind::Protocol,
                    ReplayGroup::Metadata => ActionKind::Metadata,
                    ReplayGroup::Transactions => ActionKind::SetTransaction,
                    ReplayGroup::Domains => ActionKind::DomainMetadata,
                })
                .collect(),
        }
    }
    pub(crate) fn read_schema(&self) -> KernelResult<SchemaRef> {
        action_schema(&self.actions)
    }
    pub(crate) fn replay(
        &self,
        base: PlanBuilder,
        commits: PlanBuilder,
    ) -> KernelResult<PlanBuilder> {
        let options = ActionOptions {
            actions: self.actions.clone(),
            json_stats: false,
            parsed_stats: None,
            parsed_partition_values: false,
        };
        Ok(replay(
            (commits, options.clone()),
            (base, options),
            ReplayStrategy::AggregateTailAndAntiJoin,
        )?
        .0)
    }
}
pub(crate) fn checkpoint_actions(
    segment: &crate::log_segment::LogSegment,
    shape: &crate::checkpoint::CheckpointShape,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    let selection = if schema
        .fields()
        .all(|field| matches!(field.name().as_str(), ADD_NAME | REMOVE_NAME | VERSION))
    {
        sources::CheckpointSelection::ContentOnly
    } else {
        sources::CheckpointSelection::Mixed
    };
    sources::read_checkpoint(segment, Some(shape), schema, selection)
}
