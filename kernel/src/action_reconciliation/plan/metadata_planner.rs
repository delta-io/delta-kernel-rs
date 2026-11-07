use super::*;

/// This planner centralizes schema and projection decisions shared by commits and checkpoints.
/// For each source, it constructs a plan that:
///
/// 1. Determines the read schema. A file may contain structured stats, JSON stats, or both; the
///    planner selects the representation to read.
/// 2. Projects metadata before filtering, parsing JSON stats or deserializing partition values from
///    `map<string, string>` when needed for filtering or requested output.
/// 3. Applies data skipping unless the user requests JSON stats without structured output.
/// 4. Projects metadata after filtering into the stats and partition shape requested by the user.
pub(in crate::scan) struct MetadataPlanner<'a> {
    pub(super) scan: &'a Scan,
    pub(super) stats_schema: Option<SchemaRef>,
    pub(super) parsed_partition_schema: Option<&'a SchemaRef>,
    pub(super) stats_filter: Option<Predicate>,
}

/// The statistics representation present in a metadata plan.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StatsKind {
    None,
    Json,
    Struct,
    Both,
}

impl<'a> MetadataPlanner<'a> {
    /// Whether choosing statistics or structured partitions requires the checkpoint leaf schema.
    pub(super) fn requires_checkpoint_add_schema(&self) -> bool {
        self.stats_schema.is_some() || self.parsed_partition_schema.is_some()
    }

    /// Plans a metadata source using its physical schema. `source` must honor the supplied read
    /// schema; `read_removes` keeps remove actions for replay.
    pub(super) fn build_metadata_scan(
        &self,
        available_file_schema: &StructType,
        read_removes: bool,
        source: impl FnOnce(SchemaRef) -> KernelResult<PlanBuilder>,
    ) -> KernelResult<PlanBuilder> {
        let (read_schema, current_stats_kind, has_struct_partitions) =
            self.read_schema(available_file_schema, read_removes)?;
        let plan = source(read_schema)?;
        let (plan, current_stats_kind) =
            self.prepare_metadata(plan, current_stats_kind, has_struct_partitions)?;
        let plan = plan.try_fold_with(self.stats_filter.as_ref(), |plan, predicate| {
            plan.filter(Predicate::or(col!(ADD_NAME).is_null(), predicate.clone()))
        })?;
        self.project_output(plan, current_stats_kind)
    }

    /// Selects one stats representation from the physical schema.
    ///
    /// # Returns
    ///
    /// `(read_schema, stats_kind, has_struct_partitions)`: the requested read schema, its stats
    /// representation, and whether the schema includes native `add.partitionValues_parsed`.
    pub(super) fn read_schema(
        &self,
        file_schema: &StructType,
        read_removes: bool,
    ) -> KernelResult<(SchemaRef, StatsKind, bool)> {
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
        let prefer_json_stats = self.scan.stats.synthesize_json && file_add_schema.contains(STATS);
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
                    source_collation_stats(file_schema)
                        .filter(|_| self.scan.needs_collation_stats_schema()),
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
            _ if self.stats_schema.is_some() => (patch, StatsKind::Json),
            // Neither the user nor the data skipping filter needs stats. Drop them.
            _ => (patch.drop(STATS), StatsKind::None),
        };

        // Decide what kind of partition values to read from the file by transforming `ADD_SCHEMA`
        // based on `file_schema` and `parsed_partition_schema`.
        let (add_patch, has_struct_partitions) = match (
            self.parsed_partition_schema,
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

        let read_schema = schema_ref! {
            nullable ADD_NAME: (add_patch.build(&ADD_SCHEMA)?),
            ..(read_removes.then_some(&REMOVE_FIELD)),
            nullable VERSION: LONG,
        };
        Ok((read_schema, stats_kind, has_struct_partitions))
    }

    /// Adds structured metadata needed by the stats filter or requested output when the source
    /// could not provide compatible native fields.
    ///
    /// * JSON stats are parsed with `stats_schema`, retaining JSON when requested alongside them.
    /// * `add.partitionValues_parsed` is inserted when parsed partition values is not already
    ///   present; the string map remains.
    /// * Dataskipping adds top-level `is_add`
    fn prepare_metadata(
        &self,
        plan: PlanBuilder,
        mut stats: StatsKind,
        has_struct_partitions: bool,
    ) -> KernelResult<(PlanBuilder, StatsKind)> {
        let should_parse_stats = stats == StatsKind::Json
            && (!self.scan.stats.synthesize_json || self.stats_filter.is_some());
        let stats_schema_to_parse = self.stats_schema.as_ref().filter(|_| should_parse_stats);
        let partition_schema_to_parse = self
            .parsed_partition_schema
            .filter(|_| !has_struct_partitions);
        let plan = plan.project_patch(|patch| {
            patch
                .fold_with(stats_schema_to_parse, |patch, schema| {
                    let field = StructField::nullable(STATS_PARSED, schema.clone());
                    let expr = Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema));
                    let patch = if self.scan.stats.synthesize_json {
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
                    let expr =
                        Expr::map_to_struct(col!(ADD_NAME, PARTITION_VALUES), Default::default());
                    patch.insert_after_at([ADD_NAME], PARTITION_VALUES, field, expr)
                })
                .fold_with(self.stats_filter.as_ref(), |patch, _| {
                    let field = StructField::not_null(IS_ADD, DataType::BOOLEAN);
                    patch.append(field, expr!(is_not_null(col("add.path"))))
                })
        })?;
        Ok((plan, stats))
    }

    /// Projects the requested stats and partition representations after pruning. Stats may contain
    /// JSON only, parsed only, both, or neither; parsed partitions are independent and omitted for
    /// unpartitioned tables. Removes filter-only fields and `is_add`, retaining `remove` and
    /// `version` for replay.
    fn project_output(&self, plan: PlanBuilder, stats: StatsKind) -> KernelResult<PlanBuilder> {
        let output_stats_schema = self.scan.state_info.physical_stats_output_schema();

        plan.project_patch(|mut patch| {
            let read_stats = patch
                .input_schema()
                .field_at(&column_name!(ADD_NAME, STATS_PARSED))
                .ok()
                .map(StructField::data_type);
            let mut patch = match stats {
                StatsKind::None => patch,
                StatsKind::Json => patch.fold_with(output_stats_schema, |patch, schema| {
                    let field = StructField::nullable(STATS_PARSED, schema.clone());
                    let expr = Expr::parse_json(col!(ADD_NAME, STATS), Arc::clone(schema));
                    patch.insert_after_at([ADD_NAME], STATS, field, expr)
                }),
                StatsKind::Struct | StatsKind::Both => {
                    if stats == StatsKind::Struct && self.scan.stats.synthesize_json {
                        let field = StructField::nullable(STATS, DataType::STRING);
                        let expr =
                            Expr::unary(UnaryExpressionOp::ToJson, col!(ADD_NAME, STATS_PARSED));
                        // Match ADD_SCHEMA's field order without moving native structured stats.
                        patch = patch.insert_after_at([ADD_NAME], "dataChange", field, expr)
                    }
                    // TODO(#3535): Remove support for outputting both structured and JSON stats.
                    match (read_stats, output_stats_schema) {
                        (_, None) => patch.drop_at([ADD_NAME], STATS_PARSED),
                        (Some(DataType::Struct(read)), Some(output))
                            if read.as_ref() != output.as_ref() =>
                        {
                            let mut parent = vec![ADD_NAME.into(), STATS_PARSED.into()];
                            Self::project_filter_only_stats(patch, read, output, &mut parent)
                        }
                        _ => patch,
                    }
                }
            };

            // If parsed partitions is present but was not requested, drop it.
            if self.parsed_partition_schema.is_some()
                && !self.scan.partition_values_options.parsed_struct
            {
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
// Both add and remove provide path + DV (storageType, pathOrInlineDv, offset) columns. We
// materialize them as one top-level `file_action_key` column that are used by the plan's
// aggregate and anti-join operators.
pub(super) const FILE_ACTION_KEY: &str = "file_action_key";
/// File identity used for replay.
pub(super) static FILE_ACTION_KEY_FIELD: LazyLock<StructField> = LazyLock::new(|| {
    let schema = schema! {
        nullable "path": STRING,
        nullable "deletionVector": {
            not_null "storageType": STRING,
            not_null "pathOrInlineDv": STRING,
            nullable "offset": INTEGER,
        },
    };
    StructField::nullable(FILE_ACTION_KEY, schema)
});

/// Build a file identity from path and deletion vector.
pub(super) fn file_action_key_expr(key_col_expr: impl Fn(ColumnName) -> Expr) -> Expr {
    let storage_type = key_col_expr(column_name!("deletionVector.storageType"));
    Expr::struct_from([
        key_col_expr(column_name!("path")),
        Expr::struct_with_nullability_from(
            [
                storage_type.clone(),
                key_col_expr(column_name!("deletionVector.pathOrInlineDv")),
                key_col_expr(column_name!("deletionVector.offset")),
            ],
            Expr::from_pred(storage_type.is_not_null()),
        ),
    ])
}
