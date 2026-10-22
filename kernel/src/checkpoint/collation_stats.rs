//! Preserves source collation bounds independently of the binary data-skipping schema.
//!
//! Reads extend the ordinary stats projection with source collation stats before JSON synthesis.
//! Structured writes sample versioned identifiers from the first active Add and derive STRING
//! bound columns from the table schema. Later identifiers may be omitted from structured stats.

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

use serde_json::Value;

use crate::action_reconciliation::log_replay::ActionReconciliationBatch;
use crate::actions::{NUM_RECORDS, TIGHT_BOUNDS};
use crate::engine_data::{FilteredRowVisitor, GetData, RowIndexIterator, TypedGetData};
use crate::expressions::{
    col, column_name, ColumnName, Expression, ExpressionRef, UnaryExpressionOp,
};
use crate::schema::{
    schema, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{KernelError, KernelResult, Snapshot};

const COLLATION_STATS: &str = "statsWithCollation";

/// Returns the source collation stats field, if present in parsed checkpoint stats.
pub(crate) fn source_collation_stats(schema: &StructType) -> Option<&StructField> {
    schema
        .field_at(&column_name!("add.stats_parsed", COLLATION_STATS))
        .ok()
}

/// Read and output schemas plus a projection that restores JSON before narrowing parsed stats.
pub(crate) struct CollationStatsReadTransform {
    pub(crate) read_schema: SchemaRef,
    pub(crate) output_schema: SchemaRef,
    pub(crate) expression: ExpressionRef,
}

/// Builds a read schema and projection that preserve collation bounds in `add.stats`.
///
/// Extends the requested ordinary stats projection with source collation stats. Without a parsed
/// projection, retains only `numRecords`, `tightBounds`, and collation stats in synthesized JSON.
/// Returns `None` when JSON stats are not requested or source collation stats are unavailable.
/// Returns an error if the requested Add schema cannot be patched.
pub(crate) fn build_collation_stats_read_transform(
    requested: &StructType,
    source: Option<&StructField>,
) -> KernelResult<Option<CollationStatsReadTransform>> {
    if !requested.contains_col(["add", "stats"]) {
        return Ok(None);
    }
    let Some(source) = source else {
        return Ok(None);
    };
    let parsed_path = column_name!("add.stats_parsed");
    let target = requested.field_at(&parsed_path).ok();
    let stats = match target.map(StructField::data_type) {
        Some(DataType::Struct(stats)) => stats.as_ref().clone(),
        Some(_) => return Err(KernelError::schema("stats_parsed must be a struct")),
        None => schema! { nullable NUM_RECORDS: LONG, nullable TIGHT_BOUNDS: BOOLEAN },
    };
    let stats = SchemaStructPatchBuilder::new()
        .append(source.clone())
        .build(&stats)?;
    let raw_field = StructField::nullable("stats_parsed", stats);
    let read_schema = Arc::new(
        if target.is_some() {
            SchemaStructPatchBuilder::new().replace_at(["add"], "stats_parsed", raw_field)
        } else {
            SchemaStructPatchBuilder::new().append_at(["add"], raw_field)
        }
        .build(requested)?,
    );
    let json = Expression::coalesce([
        col!("add.stats"),
        Expression::unary(UnaryExpressionOp::ToJson, col!("add.stats_parsed")),
    ]);
    let mut patch = ProjectionStructPatchBuilder::new(&read_schema).replace_expr_at(
        ["add"],
        "stats",
        json.clone(),
    );
    patch = match target {
        Some(field) => {
            let DataType::Struct(stats) = field.data_type() else {
                return Err(KernelError::schema("stats_parsed must be a struct"));
            };
            patch.replace_at(
                ["add"],
                "stats_parsed",
                field.clone(),
                Expression::parse_json(json, Arc::new(stats.as_ref().clone())),
            )
        }
        None => patch.drop_at(["add"], "stats_parsed"),
    };
    let (output_schema, expression) = patch.build()?;
    Ok(Some(CollationStatsReadTransform {
        read_schema,
        output_schema,
        expression,
    }))
}

/// Extends `binary_stats` with identifiers sampled from the first active Add.
///
/// Consumes reconciled `batches` through the first active Add and returns them for writing before
/// the remaining stream. If its stats have no collation identifiers, `binary_stats` is unchanged.
/// Both bounds include all current physical STRING columns outside partitions, so sparse samples
/// do not limit same-version bounds in later files.
/// Unsampled identifiers are omitted from structured stats; bounds are never relabeled with a
/// sampled identifier. JSON stats retain all source identifiers when enabled.
/// Returns an error if replay, visiting selected rows, or building the derived schema returns one.
pub(super) fn checkpoint_stats_schema(
    snapshot: &Snapshot,
    binary_stats: SchemaRef,
    batches: &mut impl Iterator<Item = KernelResult<ActionReconciliationBatch>>,
) -> KernelResult<(SchemaRef, Vec<ActionReconciliationBatch>)> {
    let data_schema = snapshot
        .table_configuration()
        .physical_data_schema_without_partition_columns();
    let Some(bound_schema) = StringBoundsTransform.transform_struct(&data_schema) else {
        return Ok((binary_stats, Vec::new()));
    };
    let bound_schema = bound_schema.into_owned();
    let mut visitor = CollationStatsVisitor::default();
    let mut buffered = Vec::new();
    for batch in batches {
        let batch = batch?;
        visitor.visit_rows_of(&batch.filtered_data)?;
        buffered.push(batch);
        if visitor.found_add {
            break;
        }
    }
    if visitor.identifiers.is_empty() {
        return Ok((binary_stats, buffered));
    }
    let bounds = schema! {
        nullable "minValues": (bound_schema.clone()),
        nullable "maxValues": (bound_schema),
    };
    let collations = StructType::try_new(
        visitor
            .identifiers
            .into_iter()
            .map(|identifier| StructField::nullable(identifier, bounds.clone())),
    )?;
    Ok((
        Arc::new(
            SchemaStructPatchBuilder::new()
                .append(StructField::nullable(COLLATION_STATS, collations))
                .build(&binary_stats)?,
        ),
        buffered,
    ))
}

#[derive(Default)]
struct CollationStatsVisitor {
    found_add: bool,
    identifiers: Vec<String>,
}

impl FilteredRowVisitor for CollationStatsVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static COLUMNS: LazyLock<[ColumnName; 2]> =
            LazyLock::new(|| [column_name!("add.path"), column_name!("add.stats")]);
        (&*COLUMNS, &[DataType::STRING, DataType::STRING])
    }

    fn visit_filtered<'a>(
        &mut self,
        getters: &[&'a dyn GetData<'a>],
        rows: RowIndexIterator<'_>,
    ) -> KernelResult<()> {
        for row in rows {
            let path: Option<&str> = getters[0].get_opt(row, "add.path")?;
            if path.is_none() {
                continue;
            }
            self.found_add = true;
            let json: Option<&str> = getters[1].get_opt(row, "add.stats")?;
            let Some(json) = json else {
                break;
            };
            // Malformed optional JSON stats cannot supply collation identifiers.
            let Ok(stats) = serde_json::from_str::<Value>(json) else {
                break;
            };
            if let Some(Value::Object(identifiers)) = stats.get(COLLATION_STATS) {
                self.identifiers.extend(identifiers.keys().cloned());
            }
            break;
        }
        Ok(())
    }
}

struct StringBoundsTransform;

impl<'a> SchemaTransform<'a> for StringBoundsTransform {
    transform_output_type!(|'a, T| Option<Cow<'a, T>>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        let data_type = match field.data_type() {
            DataType::Struct(child) => DataType::from(self.transform_struct(child)?.into_owned()),
            data_type if data_type == &DataType::STRING => DataType::STRING,
            _ => return None,
        };
        Some(Cow::Owned(StructField::nullable(field.name(), data_type)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use itertools::Itertools;
    use rstest::rstest;
    use serde_json::json;
    use test_utils::add_commit;
    use url::Url;

    use super::*;
    use crate::action_reconciliation::log_replay::ActionReconciliationProcessor;
    use crate::actions::{Metadata, Protocol};
    use crate::arrow::array::StringArray;
    use crate::checkpoint::checkpoint_transform::build_checkpoint_read_schema;
    use crate::checkpoint::{CheckpointWriter, CHECKPOINT_ACTIONS_SCHEMA_V1};
    use crate::engine::arrow_utils::to_json_bytes;
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::FilteredEngineData;
    use crate::object_store::memory::InMemory;
    use crate::object_store::DynObjectStore;
    use crate::scan::StatsOptions;
    use crate::schema::{schema, schema_ref, ColumnMetadataKey, MetadataValue};
    use crate::snapshot::SnapshotRef;
    use crate::{version_as_i64, Engine, EngineData};

    #[rstest]
    #[case::missing(None, vec![])]
    #[case::malformed(Some("{".into()), vec![])]
    #[case::binary_only(Some(json!({ "numRecords": 2 }).to_string()), vec![])]
    #[case::empty_collations(Some(json!({ COLLATION_STATS: {} }).to_string()), vec![])]
    #[case::empty_bounds(Some(json!({
        COLLATION_STATS: { "ICU.en_US.72": { "minValues": {} } },
    }).to_string()), vec!["ICU.en_US.72"])]
    #[case::literal_identifiers(Some(json!({ COLLATION_STATS: {
        "ICU.en_US.72": { "minValues": { "physical.with.dot": { "leaf": "B" } } },
        "spark.UTF8_LCASE.75.1": { "maxValues": { "other": "a" } },
    } }).to_string()), vec!["ICU.en_US.72", "spark.UTF8_LCASE.75.1"])]
    fn first_add_selects_its_identifiers_and_ignores_later_rows(
        #[case] stats: Option<String>,
        #[case] expected: Vec<&str>,
    ) {
        let inputs = [
            json!({ "add": { "stats": json!({
                COLLATION_STATS: { "ICU.en_US.71": { "minValues": { "other": "a" } } },
            }).to_string() } }),
            json!({ "add": { "path": "first", "stats": stats } }),
            json!({ "add": { "path": "later", "stats": json!({
                COLLATION_STATS: { "ICU.en_US.73": { "minValues": { "other": "a" } } },
            }).to_string() } }),
        ];
        let engine = SyncEngine::new();
        let data = parse_actions(
            &engine,
            schema_ref! { nullable "add": { nullable "path": STRING, nullable "stats": STRING } },
            &inputs,
        )
        .unwrap();
        let mut visitor = CollationStatsVisitor::default();
        visitor
            .visit_rows_of(&FilteredEngineData::with_all_rows_selected(data))
            .unwrap();
        assert!(visitor.found_add);
        assert_eq!(visitor.identifiers, expected);
    }

    #[rstest]
    #[case::nested_strings(
        schema! {
            not_null "physical.with.dot": { (collated_field("a")), not_null "b": STRING },
            nullable "numbers": { nullable "count": LONG },
            nullable "array": [ nullable STRING ],
            nullable "map": { STRING => nullable STRING },
            nullable "variant": unshredded_variant(),
        },
        Some(schema! {
            nullable "physical.with.dot": { nullable "a": STRING, nullable "b": STRING },
        }),
    )]
    #[case::no_strings(schema! { nullable "numbers": { nullable "count": LONG } }, None)]
    fn string_bounds_keep_only_nullable_strings_without_metadata(
        #[case] source: StructType,
        #[case] expected: Option<StructType>,
    ) {
        assert_eq!(
            StringBoundsTransform
                .transform_struct(&source)
                .map(Cow::into_owned),
            expected,
        );
    }

    #[rstest]
    #[case::collations(true)]
    #[case::no_collations(false)]
    #[tokio::test]
    async fn checkpoint_sampling_stops_after_first_add_batch(
        #[case] has_collations: bool,
    ) -> KernelResult<()> {
        let (_, engine, snapshot) = checkpoint_table(false, false).await?;
        let stats = if has_collations {
            json!({ COLLATION_STATS: { "ICU.en_US.72": {} } })
        } else {
            json!({ "numRecords": 2 })
        };
        let data = parse_actions(
            &engine,
            CHECKPOINT_ACTIONS_SCHEMA_V1.clone(),
            &[add_action("sample.parquet", stats)],
        )?;
        let batch = ActionReconciliationBatch {
            filtered_data: FilteredEngineData::with_all_rows_selected(data),
            actions_count: 1,
            add_actions_count: 1,
        };
        let mut batches = std::iter::once(Ok(batch)).chain(std::iter::once_with(|| {
            panic!("read past the first Add batch")
        }));
        let binary_stats = schema_ref! { nullable "numRecords": LONG };
        let (stats, buffered) =
            checkpoint_stats_schema(&snapshot, binary_stats.clone(), &mut batches)?;
        assert_eq!(buffered.len(), 1);
        assert_eq!(stats.contains(COLLATION_STATS), has_collations);
        if !has_collations {
            assert_eq!(stats, binary_stats);
        }
        Ok(())
    }

    #[rstest]
    #[case::structured_only(false)]
    #[case::json_and_structured(true)]
    #[tokio::test]
    async fn checkpoint_omits_unsampled_identifiers_without_relabeling_bounds(
        #[case] write_json: bool,
    ) -> KernelResult<()> {
        let (_, engine, snapshot) = checkpoint_table(write_json, false).await?;
        let writer = checkpoint_writer(snapshot, &engine)?;
        let (batches, output_schema) = writer.checkpoint_data_with_schema(&engine)?;
        let bound_schema = schema! {
            nullable "physical.with.dot": { nullable "a": STRING, nullable "b": STRING },
        };
        assert_eq!(
            source_collation_stats(&output_schema).unwrap().data_type(),
            &DataType::from(schema! { nullable "ICU.en_US.72": {
                nullable "minValues": (bound_schema.clone()),
                nullable "maxValues": (bound_schema),
            } }),
        );
        let state = batches.state();
        let adds = sample_rows_by_path(
            json_rows(batches)?
                .into_iter()
                .filter(|row| row["add"]["path"].is_string())
                .map(|row| row["add"].clone())
                .collect(),
        );
        assert_eq!(state.add_actions_count(), 3);
        assert_eq!(state.actions_count(), 5);
        assert!(
            adds["sample.parquet"]["stats_parsed"][COLLATION_STATS]["ICU.en_US.72"]["maxValues"]
                .is_null()
        );
        for (path, bounds) in samples() {
            let add = &adds[path];
            assert_sampled_bounds(&add["stats_parsed"], &bounds);
            if write_json {
                let json: Value = serde_json::from_str(add["stats"].as_str().unwrap())?;
                assert_eq!(json[COLLATION_STATS], bounds);
            } else {
                assert!(add.get("stats").is_none());
            }
        }
        Ok(())
    }

    #[rstest]
    #[case::v1_consecutive_checkpoints(false)]
    #[case::v2_sidecar_smoke(true)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_collation_bounds_round_trip(#[case] sidecars: bool) -> KernelResult<()> {
        let (store, engine, _) = checkpoint_table(false, sidecars).await?;
        for version in 1..=if sidecars { 1 } else { 2 } {
            if version == 2 {
                commit(
                    store.as_ref(),
                    version,
                    &[json!({ "commitInfo": { "timestamp": 0 } })],
                )
                .await;
            }
            let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
            let writer = checkpoint_writer(snapshot, &engine)?;
            let written = if sidecars {
                writer.write_v2_checkpoint_with_sidecars(&engine, 1)?
            } else {
                writer.write_checkpoint_without_sidecars(&engine)?
            };
            writer.finalize(&engine, &written.last_checkpoint_stats)?;
            let reloaded = Snapshot::builder_for("memory:///").build(&engine)?;
            assert_eq!(reloaded.log_segment().checkpoint_version, Some(version));
            let scan = reloaded.scan_builder().build()?;
            let rows = sample_rows_by_path(json_rows(
                scan.scan_metadata(&engine)?
                    .map(|batch| Ok(batch?.scan_files)),
            )?);
            for (path, bounds) in samples() {
                let stats = serde_json::from_str(rows[path]["stats"].as_str().unwrap())?;
                assert_sampled_bounds(&stats, &bounds);
            }
        }
        Ok(())
    }

    #[rstest]
    #[case::removed_from_json_history(false, false)]
    #[case::removed_from_structured_checkpoint(false, true)]
    #[case::dv_replaced_from_structured_checkpoint(true, true)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_discovers_only_active_collation_bounds(
        #[case] replace_dv: bool,
        #[case] earlier_checkpoint: bool,
    ) -> KernelResult<()> {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = "memory:///";
        let old_schema = schema_ref! { (collated_field("nested")) };
        let new_schema = if replace_dv {
            old_schema.clone()
        } else {
            schema_ref! { nullable "nested": { (collated_field("leaf")) } }
        };
        let metadata = Metadata::try_new(None, None, old_schema, vec![], 0, HashMap::new())?;
        let updated_metadata = metadata.clone().with_schema(new_schema)?;
        let protocol =
            Protocol::try_new_modern(["deletionVectors"], ["deletionVectors", "collations"])?;
        let old_dv = replace_dv.then(|| {
            json!({
                "storageType": "p", "pathOrInlineDv": "file:///deletion-vectors.bin", "offset": 0,
                "sizeInBytes": 1, "cardinality": 1,
            })
        });
        let old_stats = json!({
            "numRecords": 2,
            COLLATION_STATS: {
                "ICU.en_US.71": { "minValues": { "nested": "Old" } },
                "ICU.en_US.72": { "minValues": { "nested": "Old" } },
            },
        });
        let mut old_add = add_action("old.parquet", old_stats.clone());
        old_add["add"]["deletionVector"] = json!(old_dv);
        let first_commit = [
            json!({ "protocol": protocol }),
            json!({ "metaData": metadata }),
            old_add,
        ];
        commit(store.as_ref(), 0, &first_commit).await;
        if earlier_checkpoint {
            let parsed_stats = schema! {
                nullable "numRecords": LONG,
                nullable COLLATION_STATS: {
                    nullable "ICU.en_US.71": { nullable "minValues": { nullable "nested": STRING } },
                    nullable "ICU.en_US.72": { nullable "minValues": { nullable "nested": STRING } },
                },
            };
            let mut checkpoint_actions = first_commit.clone();
            checkpoint_actions[2]["add"]["stats_parsed"] = old_stats;
            write_checkpoint_rows(&engine, parsed_stats, &checkpoint_actions, false)?;
        }
        let active_value = if replace_dv {
            json!("Alice")
        } else {
            json!({ "leaf": "Alice" })
        };
        let active_bounds = json!({
            "ICU.en_US.72": {
                "minValues": { "nested": active_value },
                "maxValues": { "nested": active_value },
            },
        });
        let mut new_dv = old_dv.clone();
        if let Some(dv) = &mut new_dv {
            dv["offset"] = json!(1);
        }
        let path = if replace_dv {
            "old.parquet"
        } else {
            "new.parquet"
        };
        let mut active_add = add_action(
            path,
            json!({ "numRecords": 2, COLLATION_STATS: active_bounds }),
        );
        active_add["add"]["deletionVector"] = json!(new_dv);
        let replacement_commit = [
            json!({ "metaData": updated_metadata }),
            json!({ "remove": {
                "path": "old.parquet", "deletionTimestamp": 0,
                "dataChange": true, "deletionVector": old_dv,
            } }),
            active_add,
        ];
        commit(store.as_ref(), 1, &replacement_commit).await;
        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
        assert_eq!(
            snapshot.log_segment().checkpoint_version,
            earlier_checkpoint.then_some(0)
        );
        let binary_stats = schema_ref! { nullable "numRecords": LONG };
        let bounds = if replace_dv {
            schema! { nullable "nested": STRING }
        } else {
            schema! { nullable "nested": { nullable "leaf": STRING } }
        };
        let expected = schema_ref! {
            nullable "numRecords": LONG,
            nullable COLLATION_STATS: {
                nullable "ICU.en_US.72": {
                    nullable "minValues": (bounds.clone()),
                    nullable "maxValues": (bounds),
                },
            },
        };
        let mut processor = ActionReconciliationProcessor::new(i64::MAX, None, None);
        let mut batches = snapshot
            .log_segment()
            .read_actions(&engine, CHECKPOINT_ACTIONS_SCHEMA_V1.clone())?
            .map(move |batch| processor.process_actions_batch(batch?));
        assert_eq!(
            checkpoint_stats_schema(&snapshot, binary_stats, &mut batches)?.0,
            expected
        );
        Ok(())
    }

    #[test]
    fn read_transform_skips_missing_json_or_source_collations() {
        let source =
            StructField::nullable(COLLATION_STATS, schema! { nullable "custom.ci.1": STRING });
        let requested = schema! { nullable "add": { nullable "stats": STRING } };
        assert!(build_collation_stats_read_transform(&requested, None)
            .unwrap()
            .is_none());
        let requested = schema! { nullable "add": { nullable "path": STRING } };
        assert!(
            build_collation_stats_read_transform(&requested, Some(&source))
                .unwrap()
                .is_none()
        );
    }

    #[rstest]
    #[case::struct_only_source(false)]
    #[case::existing_json_wins(true)]
    #[tokio::test]
    async fn read_adds_only_collations_to_the_ordinary_stats_projection(
        #[case] has_json: bool,
        #[values(false, true)] parsed: bool,
    ) -> KernelResult<()> {
        let bounds = json!({ "ICU.en_US.72": { "minValues": { "other": "B" } } });
        let source = schema! {
            nullable "numRecords": LONG,
            nullable "minValues": { nullable "indexed": LONG, nullable "extra_indexed": LONG },
            nullable "tightBounds": BOOLEAN,
            nullable COLLATION_STATS: {
                nullable "ICU.en_US.72": { nullable "minValues": { nullable "other": STRING } },
            },
        };
        let source_stats = json!({
            "numRecords": 2,
            "minValues": { "indexed": 3, "extra_indexed": 7 },
            "tightBounds": false,
            COLLATION_STATS: bounds,
        });
        let expected = if has_json {
            json!({ "numRecords": 1, COLLATION_STATS: {
                "ICU.en_US.72": { "minValues": { "other": "X" } },
            } })
        } else if parsed {
            json!({
                "numRecords": 2, "minValues": { "indexed": 3 },
                "tightBounds": false, COLLATION_STATS: bounds,
            })
        } else {
            json!({ "numRecords": 2, "tightBounds": false, COLLATION_STATS: bounds })
        };
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_schema = schema_ref! {
            nullable "indexed": LONG,
            nullable "extra_indexed": LONG,
            (collated_field("other")),
        };
        let config = HashMap::from([("delta.dataSkippingStatsColumns".into(), "indexed".into())]);
        let metadata = Metadata::try_new(None, None, table_schema, vec![], 0, config)?;
        let protocol = Protocol::try_new_modern(Vec::<&str>::new(), ["collations"])?;
        let table_root = "memory:///";
        let mut actions = vec![
            json!({ "protocol": protocol }),
            json!({ "metaData": metadata }),
        ];
        commit(store.as_ref(), 0, &actions).await;
        let mut add = add_action("mixed.parquet", expected.clone());
        add["add"]["stats"] = json!(has_json.then(|| expected.to_string()));
        add["add"]["stats_parsed"] = source_stats;
        actions.push(add);
        write_checkpoint_rows(&engine, source, &actions, has_json)?;
        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
        assert_eq!(snapshot.log_segment().checkpoint_version, Some(0));
        let stats = if parsed {
            StatsOptions::all()
        } else {
            StatsOptions::json_only()
        };
        let scan = snapshot.scan_builder().with_stats(stats).build()?;
        let rows = json_rows(
            scan.scan_metadata(&engine)?
                .map(|batch| Ok(batch?.scan_files)),
        )?;
        assert_eq!(rows.len(), 1);
        let stats_json = rows[0]["stats"].as_str().unwrap();
        assert_eq!(serde_json::from_str::<Value>(stats_json)?, expected);
        if has_json {
            assert_eq!(stats_json, expected.to_string());
        }
        if parsed {
            assert_eq!(
                rows[0]["stats_parsed"]["numRecords"],
                expected["numRecords"]
            );
            assert!(rows[0]["stats_parsed"].get(COLLATION_STATS).is_none());
        }

        #[cfg(feature = "declarative-plans")]
        {
            let plan = scan.declarative_metadata_scan_plan(&engine)?.unwrap();
            let data = engine
                .require_plan_executor()?
                .execute_op(crate::plans::Operation::QueryPlan(plan))?
                .into_data()?;
            let rows =
                json_rows(data.map(|data| data.map(FilteredEngineData::with_all_rows_selected)))?;
            assert_eq!(rows.len(), 1);
            let stats_json = rows[0]["add"]["stats"].as_str().unwrap();
            assert_eq!(serde_json::from_str::<Value>(stats_json)?, expected);
            if has_json {
                assert_eq!(stats_json, expected.to_string());
            }
        }
        Ok(())
    }

    async fn checkpoint_table(
        write_json: bool,
        sidecars: bool,
    ) -> KernelResult<(Arc<DynObjectStore>, SyncEngine, SnapshotRef)> {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_schema = schema_ref! {
            not_null "physical.with.dot": { (collated_field("a")), not_null "b": STRING },
            nullable "partition": STRING,
        };
        let config = HashMap::from([
            ("delta.dataSkippingNumIndexedCols".into(), "0".into()),
            (
                "delta.checkpoint.writeStatsAsJson".into(),
                write_json.to_string(),
            ),
            ("delta.checkpoint.writeStatsAsStruct".into(), "true".into()),
        ]);
        let metadata = Metadata::try_new(
            None,
            None,
            table_schema,
            vec!["partition".into()],
            0,
            config,
        )?;
        let protocol = if sidecars {
            Protocol::try_new_modern(["v2Checkpoint"], ["v2Checkpoint", "collations"])?
        } else {
            Protocol::try_new_modern(Vec::<&str>::new(), ["collations"])?
        };
        let adds: Vec<_> = samples()
            .into_iter()
            .map(|(path, bounds)| {
                let mut add = add_action(path, json!({ "numRecords": 2, COLLATION_STATS: bounds }));
                add["add"]["partitionValues"] = json!({ "partition": "p" });
                add
            })
            .collect();
        let initial_commit = [
            json!({ "protocol": protocol }),
            json!({ "metaData": metadata }),
        ];
        commit(store.as_ref(), 0, &initial_commit).await;
        commit(store.as_ref(), 1, &adds).await;
        let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
        Ok((store, engine, snapshot))
    }

    fn samples() -> [(&'static str, Value); 3] {
        [
            (
                "sample.parquet",
                json!({ "ICU.en_US.72": {
                    "minValues": { "physical.with.dot": { "a": "B", "b": null } },
                } }),
            ),
            (
                "other-version.parquet",
                json!({ "ICU.en_US.73": {
                    "minValues": { "physical.with.dot": { "a": "A" } },
                    "maxValues": { "physical.with.dot": { "a": "Z" } },
                } }),
            ),
            (
                "same-version.parquet",
                json!({ "ICU.en_US.72": {
                    "minValues": { "physical.with.dot": { "b": "Alice" } },
                    "maxValues": { "physical.with.dot": { "b": "Zoey" } },
                } }),
            ),
        ]
    }

    fn collated_field(name: &str) -> StructField {
        StructField::not_null(name, DataType::STRING).with_metadata([(
            ColumnMetadataKey::Collations.as_ref(),
            MetadataValue::Other(json!({ name: "ICU.en_US" })),
        )])
    }

    fn add_action(path: &str, stats: Value) -> Value {
        json!({ "add": {
            "path": path, "partitionValues": {}, "size": 10,
            "modificationTime": 0, "dataChange": true, "stats": stats.to_string(),
        } })
    }

    async fn commit(store: &DynObjectStore, version: u64, actions: &[Value]) {
        add_commit(
            "memory:///",
            store,
            version,
            actions.iter().map(Value::to_string).join("\n"),
        )
        .await
        .unwrap();
    }

    fn parse_actions(
        engine: &dyn Engine,
        schema: SchemaRef,
        actions: &[Value],
    ) -> KernelResult<Box<dyn EngineData>> {
        engine.json_handler().parse_json(
            crate::unit_test_utils::string_array_to_engine_data(StringArray::from_iter_values(
                actions.iter().map(Value::to_string),
            )),
            schema,
        )
    }

    fn write_checkpoint_rows(
        engine: &dyn Engine,
        stats: StructType,
        actions: &[Value],
        write_json: bool,
    ) -> KernelResult<()> {
        let mut patch = SchemaStructPatchBuilder::new()
            .append_at(["add"], StructField::nullable("stats_parsed", stats));
        if !write_json {
            patch = patch.drop_at(["add"], "stats");
        }
        let schema = Arc::new(patch.build(&CHECKPOINT_ACTIONS_SCHEMA_V1)?);
        let data = parse_actions(engine, schema, actions)?;
        engine.parquet_handler().write_parquet_file(
            Url::parse("memory:///_delta_log/00000000000000000000.checkpoint.parquet")?,
            Box::new(std::iter::once(Ok(data))),
        )?;
        Ok(())
    }

    fn checkpoint_writer(
        snapshot: SnapshotRef,
        engine: &dyn Engine,
    ) -> KernelResult<CheckpointWriter> {
        let schema_context = CheckpointWriter::checkpoint_schema_context(&snapshot, engine)?;
        let read_schema = build_checkpoint_read_schema(
            &schema_context.checkpoint_base_schema,
            &schema_context.stats_schema,
            schema_context.partition_schema.as_deref(),
        )?;
        Ok(CheckpointWriter {
            version: version_as_i64(snapshot.version())?,
            snapshot,
            schema_context,
            read_schema,
        })
    }

    fn json_rows(
        batches: impl Iterator<Item = KernelResult<FilteredEngineData>> + Send,
    ) -> KernelResult<Vec<Value>> {
        let bytes = to_json_bytes(batches)?;
        serde_json::Deserializer::from_slice(&bytes)
            .into_iter::<Value>()
            .map(|row| row.map_err(Into::into))
            .collect()
    }

    /// Indexes `rows` by `path`, expecting exactly one row per sample file.
    fn sample_rows_by_path(rows: Vec<Value>) -> HashMap<String, Value> {
        assert_eq!(rows.len(), samples().len());
        rows.into_iter()
            .map(|row| (row["path"].as_str().unwrap().to_owned(), row))
            .collect()
    }

    /// Asserts `stats` holds a sample's `bounds` only under the sampled `ICU.en_US.72`.
    fn assert_sampled_bounds(stats: &Value, bounds: &Value) {
        assert_eq!(stats["numRecords"], 2);
        let collations = &stats[COLLATION_STATS];
        assert!(collations.get("ICU.en_US.73").is_none());
        match bounds.get("ICU.en_US.72") {
            Some(expected) => assert_json_contains(&collations["ICU.en_US.72"], expected),
            None => assert!(collations["ICU.en_US.72"].is_null()),
        }
    }

    fn assert_json_contains(actual: &Value, expected: &Value) {
        if let Value::Object(fields) = expected {
            for (name, expected) in fields {
                assert_json_contains(&actual[name], expected);
            }
        } else {
            assert_eq!(actual, expected);
        }
    }
}
