use std::sync::Arc;

use super::{ACTION_KEY, sources};
use crate::actions::{
    ADD_FIELD, ADD_NAME, ADD_SCHEMA, ActionSet, Add, STATS_PARSED, get_all_actions_schema,
};
use crate::checkpoint::CheckpointShape;
use crate::checkpoint::collation_stats::source_collation_stats;
use crate::expressions::{
    ColumnName, Expression as Expr, Predicate, UnaryExpressionOp, col, column_name, expr,
};
use crate::log_segment::LogSegment;
use crate::scan::{PartitionValuesOptions, StatsOptions, map_to_struct_options};
use crate::schema::{
    CheckpointFieldKind, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::utils::FoldWithOption as _;
use crate::{KernelError, KernelResult, PlanBuilder, Version};

const STATS: &str = "stats";
const PARTITION_VALUES: &str = "partitionValues";
const PARTITION_VALUES_PARSED: &str = "partitionValues_parsed";
const IS_ADD: &str = "is_add";
const VERSION: &str = "version";

/// This planner centralizes schema and projection decisions shared by commits and checkpoints.
/// For each source, it constructs a plan that:
///
/// 1. Determines the read schema. A file may contain structured stats, JSON stats, or both; the
///    planner selects the representation to read.
/// 2. Projects metadata before filtering, parsing JSON stats or deserializing partition values from
///    `map<string, string>` when needed for filtering or requested output.
/// 3. Applies data skipping unless the user requests JSON stats without structured output.
/// 4. Projects metadata after filtering into the stats and partition shape requested by the user.
#[derive(Clone)]
pub(crate) struct MetadataPlanner<'a> {
    pub(crate) segment: &'a LogSegment,
    pub(crate) actions: ActionSet,
    pub(crate) checkpoint_shape: Option<CheckpointShape>,
    pub(crate) stats: StatsOptions,
    pub(crate) partitions: PartitionValuesOptions,
    pub(crate) stats_schema: Option<SchemaRef>,
    pub(crate) output_stats_schema: Option<SchemaRef>,
    pub(crate) parsed_partition_schema: Option<SchemaRef>,
    pub(crate) read_collations: bool,
    pub(crate) stats_filter: Option<Predicate>,
}

/// The statistics representation present in a metadata plan.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatsKind {
    None,
    Json,
    Struct,
    Both,
}

impl<'a> MetadataPlanner<'a> {
    /// Binds the source context for one metadata plan. Callers supply resolved metadata schemas.
    pub(crate) fn new(segment: &'a LogSegment, actions: ActionSet) -> Self {
        Self {
            segment,
            actions,
            checkpoint_shape: None,
            stats: StatsOptions::none(),
            partitions: PartitionValuesOptions::default(),
            stats_schema: None,
            output_stats_schema: None,
            parsed_partition_schema: None,
            read_collations: false,
            stats_filter: None,
        }
    }

    pub(crate) fn with_checkpoint_shape(
        mut self,
        shape: impl Into<Option<CheckpointShape>>,
    ) -> Self {
        self.checkpoint_shape = shape.into();
        self
    }

    pub(crate) fn with_stats(mut self, stats: StatsOptions) -> Self {
        self.stats = stats;
        self
    }

    /// Sets the resolved schemas used to read statistics and project structured output.
    pub(crate) fn with_stats_schemas(
        mut self,
        read: Option<SchemaRef>,
        output: Option<SchemaRef>,
    ) -> Self {
        self.stats_schema = read;
        self.output_stats_schema = output;
        self
    }

    pub(crate) fn with_partition_values(
        mut self,
        options: PartitionValuesOptions,
        parsed_schema: Option<SchemaRef>,
    ) -> Self {
        self.partitions = options;
        self.parsed_partition_schema = parsed_schema;
        self
    }

    pub(crate) fn with_read_collations(mut self, read_collations: bool) -> Self {
        self.read_collations = read_collations;
        self
    }

    pub(crate) fn with_stats_filter(mut self, predicate: Option<Predicate>) -> Self {
        self.stats_filter = predicate;
        self
    }

    /// Replays selected actions with the requested stats and partition representations.
    /// Uses the caller's resolved schemas and options; sources supply `version`.
    pub(crate) fn replay(&self) -> KernelResult<PlanBuilder> {
        let commits = self.commit_actions(self.segment.checkpoint_version)?;
        let checkpoint = self.checkpoint_actions()?;
        self.replay_actions(commits, checkpoint)
    }

    /// Build normalized CRC adds with the supplied version and action key.
    /// Requires a file-only action selection.
    pub(crate) fn crc_adds(&self, version: Version, files: Vec<Add>) -> KernelResult<PlanBuilder> {
        if !self.actions.file_actions || self.actions.contains_non_content_actions() {
            return Err(KernelError::schema("CRC replay requires file-only actions"));
        }
        self.build_metadata_plan(None, |schema| {
            sources::crc_arm(files, version, self.actions.includes_removes, schema)
        })
    }

    /// Build selected checkpoint actions and their action keys.
    /// Returns an empty relation when no checkpoint exists.
    ///
    /// ## SQL equivalent (add/remove selection):
    ///
    /// SELECT STRUCT(
    ///          add.* EXCEPT (
    ///            stats_parsed, partitionValues_parsed
    ///          ),
    ///          add.stats_parsed AS stats_parsed,
    ///          add.partitionValues_parsed AS partitionValues_parsed
    ///        ) AS add,
    ///        remove,
    ///        version,
    ///        action_key(add, remove) AS action_key
    /// FROM checkpoint_actions
    /// WHERE add.path IS NOT NULL OR remove.path IS NOT NULL
    ///
    /// When the checkpoint lacks native parsed metadata, `FROM_JSON(add.stats, physical_stats)`
    /// and `MAP_TO_STRUCT(add.partitionValues, physical_partitions)` replace the corresponding
    /// fields above. Parsed fields are omitted when unrequested or their schema is absent.
    /// JSON stats are retained or synthesized only when requested.
    pub(crate) fn checkpoint_actions(&self) -> KernelResult<PlanBuilder> {
        self.build_metadata_plan(self.checkpoint_shape.as_ref(), |schema| {
            sources::checkpoint_actions(
                self.segment,
                self.checkpoint_shape.as_ref(),
                self.actions,
                schema,
            )
        })
    }

    /// Build the normalized commit JSON arm, including its action key.
    /// When `base_version` is set, only commits newer than that base are read.
    ///
    /// ## SQL equivalent (add/remove selection):
    ///
    /// SELECT STRUCT(
    ///          add.* EXCEPT (stats_parsed, partitionValues_parsed),
    ///          FROM_JSON(add.stats, physical_stats) AS stats_parsed,
    ///          MAP_TO_STRUCT(add.partitionValues, physical_partitions) AS partitionValues_parsed
    ///        ) AS add,
    ///        remove,
    ///        version,
    ///        action_key(add, remove) AS action_key
    /// FROM json_commits
    /// WHERE add.path IS NOT NULL OR remove.path IS NOT NULL
    ///
    /// Parsed fields are omitted when unrequested or their schema is absent.
    /// JSON stats are retained or synthesized only when requested.
    pub(crate) fn commit_actions(
        &self,
        base_version: Option<Version>,
    ) -> KernelResult<PlanBuilder> {
        self.build_metadata_plan(None, |schema| {
            sources::commit_arm(self.segment, base_version, schema)
        })
    }

    /// Whether [`CheckpointShape`](crate::checkpoint::CheckpointShape) should inspect the
    /// leaf-level checkpoint schema to choose the statistics representation and identify
    /// compatible structured partition values.
    pub(crate) fn requires_checkpoint_add_schema(&self) -> bool {
        self.stats_schema.is_some() || self.parsed_partition_schema.is_some()
    }

    /// Reads and filters selected actions, attaches their identity key, then prepares metadata.
    /// Representation is determined from the source shape and the requested options.
    pub(crate) fn build_metadata_plan(
        &self,
        shape: Option<&CheckpointShape>,
        source: impl FnOnce(SchemaRef) -> KernelResult<PlanBuilder>,
    ) -> KernelResult<PlanBuilder> {
        let (read_schema, current_stats_kind, has_struct_partitions) = self.read_schema(shape)?;
        let (key_field, key_expr) = self.actions.key()?;
        let plan = source(read_schema)?
            .filter(self.actions.predicate())?
            .project_patch(|patch| patch.append(key_field, key_expr))?;
        if !self.actions.file_actions {
            return Ok(plan);
        }
        let (plan, current_stats_kind) = self.prepare_metadata(
            plan,
            current_stats_kind,
            has_struct_partitions,
            self.stats_filter.is_some(),
        )?;
        let plan = plan.try_fold_with(self.stats_filter.as_ref(), |plan, predicate| {
            plan.filter(Predicate::or(col!(ADD_NAME).is_null(), predicate.clone()))
        })?;
        self.project_output(plan, current_stats_kind)
    }

    /// Prunes reads to selected actions and the required metadata representations.
    ///
    /// # Returns
    ///
    /// `(read_schema, stats_kind, has_struct_partitions)`: the requested read schema, its stats
    /// representation, and whether the schema includes native `add.partitionValues_parsed`.
    pub(crate) fn read_schema(
        &self,
        shape: Option<&CheckpointShape>,
    ) -> KernelResult<(SchemaRef, StatsKind, bool)> {
        let selected = self.actions.schema()?;
        let action_patch =
            SchemaStructPatchBuilder::new().append(StructField::nullable(VERSION, DataType::LONG));
        // Only file actions require resolving the checkpoint shape and statistics.
        // If there are no file actions, exit early.
        if !self.actions.file_actions {
            return Ok((
                Arc::new(action_patch.build(&selected)?),
                StatsKind::None,
                false,
            ));
        }
        let file_schema = shape
            .and_then(|shape| shape.leaf_checkpoint_schema.as_deref())
            .unwrap_or(get_all_actions_schema());
        let add_field = file_schema.field(ADD_NAME).unwrap_or(&ADD_FIELD);
        let file_add_schema = match add_field.data_type() {
            DataType::Struct(add) => add,
            data_type => {
                return Err(KernelError::schema(format!(
                    "metadata source field '{ADD_NAME}' must be a struct, found {}",
                    data_type.kind_name(),
                )));
            }
        };
        let get_type_of = |name: &str| file_add_schema.field(name).map(StructField::data_type);

        // A schema patch to apply to `ADD_SCHEMA`
        let patch = SchemaStructPatchBuilder::new();

        // Decide what kind of stats to read from the file by transforming `ADD_SCHEMA` based on the
        // `file_schema` and table-derived stats schema.
        let prefer_json_stats = self.stats.synthesize_json && file_add_schema.contains(STATS);
        let (patch, stats_kind) = match (self.stats_schema.as_ref(), get_type_of(STATS_PARSED)) {
            // Parsed statistics are requested and present in the file
            (Some(required_stats), Some(DataType::Struct(file_parsed_stats)))
                if !prefer_json_stats
                    && LogSegment::structs_have_compatible_types(
                        file_parsed_stats,
                        required_stats,
                        STATS_PARSED,
                        CheckpointFieldKind::Stats,
                    ) =>
            {
                let stats_schema = required_stats.clone().try_fold_with(
                    source_collation_stats(file_schema).filter(|_| self.read_collations),
                    |stats, field| {
                        SchemaStructPatchBuilder::new()
                            .append(field.clone())
                            .build(&stats)
                            .map(Arc::new)
                    },
                )?;
                let stats_parsed_field = StructField::nullable(STATS_PARSED, stats_schema);
                let patch = patch.drop(STATS).insert_after(STATS, stats_parsed_field);
                (patch, StatsKind::Struct)
            }
            // Json stats are used either by the dataskipping filter or are requested by the user
            _ if self.stats.synthesize_json || self.stats_schema.is_some() => {
                (patch, StatsKind::Json)
            }
            // Neither the user nor the data skipping filter needs stats. Drop them.
            _ => (patch.drop(STATS), StatsKind::None),
        };

        // Decide what kind of partition values to read from the file by transforming `ADD_SCHEMA`
        // based on `file_schema` and `parsed_partition_schema`.
        let (add_patch, has_struct_partitions) = match (
            self.parsed_partition_schema.as_ref(),
            get_type_of(PARTITION_VALUES_PARSED),
        ) {
            (Some(required), Some(DataType::Struct(native)))
                if LogSegment::structs_have_compatible_types(
                    native,
                    required,
                    PARTITION_VALUES_PARSED,
                    CheckpointFieldKind::PartitionValues,
                ) =>
            {
                let field = StructField::nullable(PARTITION_VALUES_PARSED, required.clone());
                (patch.insert_after(PARTITION_VALUES, field), true)
            }
            _ => (patch, false),
        };

        let read_schema = action_patch
            .replace(
                ADD_NAME,
                StructField::nullable(ADD_NAME, add_patch.build(&ADD_SCHEMA)?),
            )
            .build(&selected)?;
        Ok((Arc::new(read_schema), stats_kind, has_struct_partitions))
    }

    /// Adds structured metadata needed by the stats filter or requested output when the source
    /// could not provide compatible native fields.
    ///
    /// * JSON stats are parsed with `stats_schema`, retaining JSON when requested alongside them.
    /// * `add.partitionValues_parsed` is inserted when parsed partition values is not already
    ///   present; the string map remains.
    /// * Dataskipping adds top-level `is_add`
    pub(crate) fn prepare_metadata(
        &self,
        plan: PlanBuilder,
        mut stats: StatsKind,
        has_struct_partitions: bool,
        parse_for_filter: bool,
    ) -> KernelResult<(PlanBuilder, StatsKind)> {
        let should_parse_stats =
            stats == StatsKind::Json && (!self.stats.synthesize_json || parse_for_filter);
        let stats_schema_to_parse = self.stats_schema.as_ref().filter(|_| should_parse_stats);
        let partition_schema_to_parse = self
            .parsed_partition_schema
            .as_ref()
            .filter(|_| !has_struct_partitions);
        let plan = plan.project_patch(|patch| {
            patch
                .fold_with(stats_schema_to_parse, |patch, schema| {
                    let field = StructField::nullable(STATS_PARSED, schema.clone());
                    let expr = Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema));
                    let patch = if self.stats.synthesize_json {
                        stats = StatsKind::Both;
                        patch
                    } else {
                        stats = StatsKind::Struct;
                        patch.drop_at([ADD_NAME], STATS)
                    };
                    patch.insert_after_at([ADD_NAME], STATS, field, expr)
                })
                .fold_with(partition_schema_to_parse, |patch, schema| {
                    let field = StructField::nullable(PARTITION_VALUES_PARSED, schema.clone());
                    let expr = Expr::map_to_struct(
                        col!(ADD_NAME, PARTITION_VALUES),
                        map_to_struct_options(self.partitions.timestamp_timezone.as_deref()),
                    );
                    patch.insert_after_at([ADD_NAME], PARTITION_VALUES, field, expr)
                })
                .fold_with(parse_for_filter.then_some(()), |patch, _| {
                    let field = StructField::not_null(IS_ADD, DataType::BOOLEAN);
                    patch.append(field, expr!(is_not_null(col("add.path"))))
                })
        })?;
        Ok((plan, stats))
    }

    /// Reconciles version-tagged commits with an already-reconciled checkpoint or CRC base.
    /// Inputs must carry the action key and the same schema, normalized by metadata planning.
    /// Commit versions must be strictly newer than the base. Retention is left to the consumer.
    /// Non-content actions aggregate together. Content actions aggregate the tail and anti-join the
    /// base; when only JSON commits exist, the builder eliminates the empty base and anti-join.
    fn replay_actions(
        &self,
        commits: PlanBuilder,
        checkpoint: PlanBuilder,
    ) -> KernelResult<PlanBuilder> {
        if !self.actions.contains_content_actions() {
            return self
                .latest_by_key(PlanBuilder::union_all([checkpoint, commits])?)?
                .project_patch(|patch| patch.drop(ACTION_KEY));
        }
        // Winning deletions cancel base keys even if retention will later expire them.
        let winners = self.latest_by_key(commits)?;
        let base = checkpoint
            .anti_join(
                winners.clone(),
                [column_name!(ACTION_KEY)],
                [column_name!(ACTION_KEY)],
            )?
            .project_patch(|patch| patch.drop(ACTION_KEY))?;
        let tail = winners.project_patch(|patch| patch.drop(ACTION_KEY))?;
        PlanBuilder::union_all([tail, base])
    }

    /// Selects the latest whole action per identity, retaining the key and winning deletions.
    fn latest_by_key(&self, input: PlanBuilder) -> KernelResult<PlanBuilder> {
        input.aggregate_by([column_name!(ACTION_KEY)], |mut aggs| {
            for name in self.actions.names().chain([VERSION]) {
                // Each file-identity group contains the adds and removes for a given file; winning
                // adds pass through unchanged while winning removes produce NULL adds. Non-file
                // actions have a NULL file-identity lane and group by their own identity lanes.
                // Use the same non-null ACTION_KEY sentinel so a winning remove clears the old add.
                aggs = aggs.max_non_null_by(
                    ColumnName::new([name]),
                    column_name!(ACTION_KEY),
                    column_name!(VERSION),
                );
            }
            aggs
        })
    }

    /// Projects the requested stats and partition representations after pruning. Stats may contain
    /// JSON only, parsed only, both, or neither; parsed partitions are independent and omitted for
    /// unpartitioned tables. Removes filter-only fields and `is_add`, retaining `remove` and
    /// `version` for replay.
    fn project_output(&self, plan: PlanBuilder, stats: StatsKind) -> KernelResult<PlanBuilder> {
        let output_stats_schema = self.output_stats_schema.as_ref();

        let input_schema = plan.schema().clone();
        let read_stats = get_parsed_stats(stats, &input_schema)?;
        plan.project_patch(|mut patch| {
            let mut patch = match stats {
                StatsKind::None => patch,
                StatsKind::Json => patch.fold_with(output_stats_schema, |patch, schema| {
                    let field = StructField::nullable(STATS_PARSED, schema.clone());
                    let expr = Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema));
                    patch.insert_after_at([ADD_NAME], STATS, field, expr)
                }),
                StatsKind::Struct | StatsKind::Both => {
                    if stats == StatsKind::Struct && self.stats.synthesize_json {
                        let field = StructField::nullable(STATS, DataType::STRING);
                        let expr =
                            Expr::unary(UnaryExpressionOp::ToJson, col!(ADD_NAME, STATS_PARSED));
                        // Match ADD_SCHEMA's field order without moving native structured stats.
                        patch = patch.insert_after_at([ADD_NAME], "dataChange", field, expr)
                    }
                    // TODO(#3535): Remove support for outputting both structured and JSON stats.
                    match (read_stats, output_stats_schema) {
                        (_, None) => patch.drop_at([ADD_NAME], STATS_PARSED),
                        (Some(read), Some(output)) if read != output.as_ref() => {
                            let mut parent = vec![ADD_NAME.into(), STATS_PARSED.into()];
                            Self::project_filter_only_stats(patch, read, output, &mut parent)
                        }
                        _ => patch,
                    }
                }
            };

            // If parsed partitions is present but was not requested, drop it.
            if self.parsed_partition_schema.is_some() && !self.partitions.parsed_struct {
                patch = patch.drop_at([ADD_NAME], PARTITION_VALUES_PARSED);
            }

            // drop the `IS_ADD` field that is only used in dataskipping filtering
            patch.fold_with(self.stats_filter.as_ref(), |patch, _| patch.drop(IS_ADD))
        })
    }

    /// Projects away stats fields omitted from structured output beneath `parent`.
    /// Missing fields are dropped as whole subtrees; only retained structs are visited.
    ///
    /// Example, showing only `minValues` for nested table columns `a.{b,c}` and `d.{e,f}`:
    /// ```text
    /// read:    minValues { a: { b, c }, d: { e, f } }
    /// output:  minValues { a: { b } }
    /// drops:   add.stats_parsed.minValues.a.c, add.stats_parsed.minValues.d
    /// ```
    /// `a` is retained, so only `a.c` is dropped. `d` is absent, so its children are not visited.
    fn project_filter_only_stats<'s>(
        mut patch: ProjectionStructPatchBuilder<'s>,
        read: &StructType,
        output: &StructType,
        parent: &mut Vec<String>,
    ) -> ProjectionStructPatchBuilder<'s> {
        for field in read.fields() {
            let output = output.field(field.name()).map(StructField::data_type);
            match (field.data_type(), output) {
                (_, None) => patch = patch.drop_at(parent.as_slice(), field.name()),
                (DataType::Struct(read), Some(DataType::Struct(output))) => {
                    parent.push(field.name().to_owned());
                    patch = Self::project_filter_only_stats(patch, read, output, parent);
                    parent.pop();
                }
                _ => {}
            }
        }
        patch
    }
}

fn get_parsed_stats(stats: StatsKind, schema: &StructType) -> KernelResult<Option<&StructType>> {
    Ok(match stats {
        StatsKind::Struct | StatsKind::Both => match schema
            .field_at(&column_name!(ADD_NAME, STATS_PARSED))?
            .data_type()
        {
            DataType::Struct(read) => Some(read.as_ref()),
            _ => {
                return Err(KernelError::schema(
                    "Parsed statistics must have a struct type",
                ));
            }
        },
        StatsKind::None | StatsKind::Json => None,
    })
}
