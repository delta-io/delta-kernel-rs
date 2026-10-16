//! Preserves source collation bounds independently of the binary data-skipping schema.
//!
//! Reads serialize the complete source stats before projecting parsed columns. Structured writes
//! sample versioned identifiers from the first active Add and derive STRING bound columns
//! from the table schema. Later identifiers may be omitted from structured stats.

use std::sync::{Arc, LazyLock};

use serde_json::Value;

use crate::action_reconciliation::log_replay::ActionReconciliationBatch;
use crate::engine_data::{FilteredRowVisitor, GetData, RowIndexIterator, TypedGetData};
use crate::expressions::{
    col, column_name, ColumnName, Expression, ExpressionRef, UnaryExpressionOp,
};
use crate::schema::{
    schema, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
};
use crate::struct_patch::ProjectionStructPatchBuilder;
use crate::{KernelError, KernelResult, Result, Snapshot};

const COLLATION_STATS: &str = "statsWithCollation";

/// Returns the complete source parsed-stats schema when it contains collation bounds.
pub(crate) fn source_collation_stats(schema: &StructType) -> Option<&StructType> {
    let DataType::Struct(stats) = schema
        .field_at(&column_name!("add.stats_parsed"))
        .ok()?
        .data_type()
    else {
        return None;
    };
    stats.contains(COLLATION_STATS).then_some(stats)
}

/// Read and output schemas plus a projection that restores JSON before narrowing parsed stats.
pub(crate) struct CollationStatsReadTransform {
    pub(crate) read_schema: SchemaRef,
    pub(crate) output_schema: SchemaRef,
    pub(crate) expression: ExpressionRef,
}

/// Builds a read schema and projection that preserve collation bounds in `add.stats`.
///
/// Fills missing JSON stats from the complete source struct before projecting parsed stats.
/// Returns `None` when JSON stats are not requested or source collation stats are unavailable.
/// Returns an error if the requested Add schema cannot be patched.
pub(crate) fn build_collation_stats_read_transform(
    requested: &StructType,
    source: Option<&StructType>,
) -> KernelResult<Option<CollationStatsReadTransform>> {
    if !requested.contains_col(["add", "stats"]) {
        return Ok(None);
    }
    let Some(source) = source else {
        return Ok(None);
    };
    let parsed_path = column_name!("add.stats_parsed");
    let target = requested.field_at(&parsed_path).ok();
    let raw_field = StructField::nullable("stats_parsed", source.clone());
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
/// Collation bounds are optional: unsampled identifiers are omitted from structured stats.
/// JSON stats retain all source identifiers when enabled.
/// Returns an error from replay, visiting selected rows, or building the derived schema.
pub(super) fn checkpoint_stats_schema(
    snapshot: &Snapshot,
    binary_stats: SchemaRef,
    batches: &mut impl Iterator<Item = Result<ActionReconciliationBatch>>,
) -> KernelResult<(SchemaRef, Vec<ActionReconciliationBatch>)> {
    let bound_schema = string_bound_schema(
        &snapshot
            .table_configuration()
            .physical_data_schema_without_partition_columns(),
    )?;
    if bound_schema.num_fields() == 0 {
        return Ok((binary_stats, Vec::new()));
    }
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
    ) -> Result<()> {
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

fn string_bound_schema(schema: &StructType) -> KernelResult<StructType> {
    let mut fields = Vec::new();
    for field in schema.fields() {
        let data_type = match field.data_type() {
            DataType::Struct(child) => {
                let child = string_bound_schema(child)?;
                if child.num_fields() == 0 {
                    continue;
                }
                DataType::from(child)
            }
            data_type if data_type == &DataType::STRING => DataType::STRING,
            _ => continue,
        };
        fields.push(StructField::nullable(field.name(), data_type));
    }
    StructType::try_new(fields)
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
    use crate::arrow::array::{StringArray, StructArray};
    use crate::checkpoint::checkpoint_transform::build_checkpoint_read_schema;
    use crate::checkpoint::{CheckpointWriter, CHECKPOINT_ACTIONS_SCHEMA_V1};
    use crate::engine::arrow_data::EngineDataArrowExt;
    use crate::engine::arrow_utils::to_json_bytes;
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::FilteredEngineData;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::{DynObjectStore, ObjectStoreExt as _};
    use crate::schema::{schema, schema_ref, ColumnMetadataKey, MetadataValue};
    use crate::{version_as_i64, Engine};

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
        let data = engine.json_handler().parse_json(
            crate::unit_test_utils::string_array_to_engine_data(StringArray::from(
                inputs.iter().map(Value::to_string).collect::<Vec<_>>(),
            )),
            schema_ref! { nullable "add": { nullable "path": STRING, nullable "stats": STRING } },
        ).unwrap();
        let mut visitor = CollationStatsVisitor::default();
        visitor
            .visit_rows_of(&FilteredEngineData::with_all_rows_selected(data))
            .unwrap();
        assert!(visitor.found_add);
        assert_eq!(visitor.identifiers, expected);
    }

    #[rstest]
    #[case::structured_only(false)]
    #[case::json_and_structured(true)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_samples_only_first_add_and_preserves_selected_bounds(
        #[case] write_json: bool,
        #[values(false, true)] sample_has_collations: bool,
    ) -> Result<()> {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = "memory:///";
        let leaf = StructField::not_null("a", DataType::STRING).with_metadata([(
            ColumnMetadataKey::Collations.as_ref(),
            MetadataValue::Other(json!({ "a": "ICU.en_US" })),
        )]);
        let table_schema = schema_ref! {
            not_null "physical.with.dot": { (leaf), not_null "b": STRING },
            nullable "id": LONG,
            nullable "numbers": { nullable "count": LONG },
            nullable "array": [ nullable STRING ],
            nullable "map": { STRING => nullable STRING },
            nullable "partition": STRING,
        };
        let metadata = Metadata::try_new(
            Some("test-table".into()),
            None,
            table_schema,
            vec!["partition".into()],
            0,
            HashMap::from([
                ("delta.dataSkippingNumIndexedCols".into(), "0".into()),
                (
                    "delta.checkpoint.writeStatsAsJson".into(),
                    write_json.to_string(),
                ),
                ("delta.checkpoint.writeStatsAsStruct".into(), "true".into()),
            ]),
        )?;
        let protocol = Protocol::try_new_modern(Vec::<&str>::new(), ["collations"])?;
        let samples = [
            (
                "old.parquet",
                json!({ "ICU.en_US.71": {
                    "minValues": { "physical.with.dot": { "a": "Old" } },
                } }),
            ),
            (
                "sample.parquet",
                if sample_has_collations {
                    json!({ "ICU.en_US.72": {
                        "minValues": { "physical.with.dot": { "a": "B", "b": null } },
                    } })
                } else {
                    json!({})
                },
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
        ];
        let adds: Vec<_> = samples
            .iter()
            .map(|(path, bounds)| {
                json!({ "add": {
                    "path": path, "partitionValues": { "partition": "p" }, "size": 10,
                    "modificationTime": 0, "dataChange": true,
                    "stats": json!({ "numRecords": 2, COLLATION_STATS: bounds }).to_string(),
                } })
            })
            .collect();
        add_commit(
            table_root,
            &store,
            0,
            [
                json!({ "protocol": protocol }),
                json!({ "metaData": metadata }),
                adds[0].clone(),
            ]
            .iter()
            .map(Value::to_string)
            .join("\n"),
        )
        .await
        .unwrap();
        add_commit(
            table_root,
            &store,
            1,
            adds[1..].iter().map(Value::to_string).join("\n"),
        )
        .await
        .unwrap();
        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
        let schema_context = CheckpointWriter::checkpoint_schema_context(&snapshot, &engine)?;
        let binary_stats = schema_context.stats_schema.clone();
        let read_schema = build_checkpoint_read_schema(
            &schema_context.checkpoint_base_schema,
            &binary_stats,
            schema_context.partition_schema.as_deref(),
        )?;
        let writer = CheckpointWriter {
            version: version_as_i64(snapshot.version())?,
            snapshot: snapshot.clone(),
            schema_context,
            read_schema: read_schema.clone(),
        };
        let (batches, output_schema) = writer.checkpoint_data_with_schema(&engine)?;
        let bounds = schema! {
            nullable "physical.with.dot": { nullable "a": STRING, nullable "b": STRING },
        };
        let expected = if sample_has_collations {
            Arc::new(
                SchemaStructPatchBuilder::new()
                    .append(StructField::nullable(
                        COLLATION_STATS,
                        schema! { nullable "ICU.en_US.72": {
                            nullable "minValues": (bounds.clone()),
                            nullable "maxValues": (bounds),
                        } },
                    ))
                    .build(&binary_stats)?,
            )
        } else {
            binary_stats.clone()
        };
        assert_eq!(
            output_schema
                .field_at(&column_name!("add.stats_parsed"))?
                .data_type(),
            &DataType::from(expected.as_ref().clone()),
        );
        let mut output = HashMap::new();
        for batch in batches {
            let data = batch?.apply_selection_vector()?;
            let bytes = to_json_bytes(std::iter::once(Ok(
                FilteredEngineData::with_all_rows_selected(data),
            )))?;
            for action in serde_json::Deserializer::from_slice(&bytes).into_iter::<Value>() {
                let action = action?;
                if let Some(path) = action["add"]["path"].as_str() {
                    output.insert(path.to_string(), action["add"].clone());
                }
            }
        }
        assert_eq!(output.len(), samples.len());
        for (path, bounds) in &samples {
            let add = &output[*path];
            assert_eq!(add["stats_parsed"]["numRecords"], 2);
            let parsed = &add["stats_parsed"][COLLATION_STATS];
            assert!(parsed.get("ICU.en_US.71").is_none());
            assert!(parsed.get("ICU.en_US.73").is_none());
            if !sample_has_collations {
                assert!(parsed.is_null());
            }
            if !bounds.as_object().unwrap().contains_key("ICU.en_US.72") {
                assert!(parsed["ICU.en_US.72"].is_null());
            }
            if write_json {
                let json: Value = serde_json::from_str(add["stats"].as_str().unwrap())?;
                assert_eq!(&json[COLLATION_STATS], bounds);
            } else {
                assert!(add.get("stats").is_none());
            }
        }
        if sample_has_collations {
            let sample = &output["sample.parquet"]["stats_parsed"][COLLATION_STATS]["ICU.en_US.72"];
            assert_eq!(sample["minValues"]["physical.with.dot"]["a"], "B");
            assert!(sample["minValues"]["physical.with.dot"]["b"].is_null());
            assert!(sample["maxValues"].is_null());
            let same_version =
                &output["same-version.parquet"]["stats_parsed"][COLLATION_STATS]["ICU.en_US.72"];
            assert_eq!(same_version["minValues"]["physical.with.dot"]["b"], "Alice");
            assert_eq!(same_version["maxValues"]["physical.with.dot"]["b"], "Zoey");
        }

        // An unreadable older commit proves discovery stops before requesting the next batch.
        store
            .delete(&Path::from("_delta_log/00000000000000000000.json"))
            .await?;
        let mut processor = ActionReconciliationProcessor::new(i64::MAX, None);
        let mut batches = snapshot
            .log_segment()
            .read_actions(&engine, read_schema)?
            .map(move |batch| processor.process_actions_batch(batch?));
        assert_eq!(
            checkpoint_stats_schema(&snapshot, binary_stats, &mut batches)?.0,
            expected
        );
        Ok(())
    }

    #[rstest]
    #[case::removed_file(false)]
    #[case::replaced_deletion_vector(true)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_discovers_only_active_collation_bounds(
        #[case] replace_dv: bool,
        #[values(false, true)] earlier_checkpoint: bool,
        #[values(false, true)] write_json: bool,
    ) -> Result<()> {
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let table_root = "memory:///";
        let field = StructField::nullable("nested", DataType::STRING).with_metadata([(
            ColumnMetadataKey::Collations.as_ref(),
            MetadataValue::Other(json!({ "nested": "icu.en_US" })),
        )]);
        let old_schema = schema_ref! { (field) };
        let new_schema = if replace_dv {
            old_schema.clone()
        } else {
            let leaf = StructField::nullable("leaf", DataType::STRING).with_metadata([(
                ColumnMetadataKey::Collations.as_ref(),
                MetadataValue::Other(json!({ "leaf": "icu.en_US" })),
            )]);
            schema_ref! { nullable "nested": { (leaf) } }
        };
        let metadata = Metadata::try_new(
            Some("test-table".into()),
            None,
            old_schema,
            vec![],
            0,
            HashMap::from([
                (
                    "delta.checkpoint.writeStatsAsJson".into(),
                    write_json.to_string(),
                ),
                ("delta.checkpoint.writeStatsAsStruct".into(), "true".into()),
            ]),
        )?;
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
        let first_commit = [
            json!({ "protocol": protocol }),
            json!({ "metaData": metadata }),
            json!({ "add": {
                "path": "old.parquet", "partitionValues": {}, "size": 10,
                "modificationTime": 0, "dataChange": true,
                "deletionVector": old_dv, "stats": old_stats.to_string(),
            } }),
        ];
        add_commit(
            table_root,
            &store,
            0,
            first_commit.iter().map(Value::to_string).join("\n"),
        )
        .await
        .unwrap();
        if earlier_checkpoint {
            let parsed_stats = schema! {
                nullable "numRecords": LONG,
                nullable COLLATION_STATS: {
                    nullable "ICU.en_US.71": { nullable "minValues": { nullable "nested": STRING } },
                    nullable "ICU.en_US.72": { nullable "minValues": { nullable "nested": STRING } },
                },
            };
            let mut patch = SchemaStructPatchBuilder::new()
                .append_at(["add"], StructField::nullable("stats_parsed", parsed_stats));
            if !write_json {
                patch = patch.drop_at(["add"], "stats");
            }
            let checkpoint_schema = Arc::new(patch.build(&CHECKPOINT_ACTIONS_SCHEMA_V1)?);
            let mut checkpoint_actions = first_commit.clone();
            checkpoint_actions[2]["add"]["stats_parsed"] = old_stats;
            let data = engine.json_handler().parse_json(
                crate::unit_test_utils::string_array_to_engine_data(StringArray::from(
                    checkpoint_actions
                        .iter()
                        .map(Value::to_string)
                        .collect::<Vec<_>>(),
                )),
                checkpoint_schema,
            )?;
            engine.parquet_handler().write_parquet_file(
                Url::parse("memory:///_delta_log/00000000000000000000.checkpoint.parquet")?,
                Box::new(std::iter::once(Ok(data))),
            )?;
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
        let new_dv = replace_dv.then(|| {
            json!({
                "storageType": "p", "pathOrInlineDv": "file:///deletion-vectors.bin", "offset": 1,
                "sizeInBytes": 1, "cardinality": 1,
            })
        });
        let replacement_commit = [
            json!({ "metaData": updated_metadata }),
            json!({ "remove": {
                "path": "old.parquet", "deletionTimestamp": 0,
                "dataChange": true, "deletionVector": old_dv,
            } }),
            json!({ "add": {
                "path": if replace_dv { "old.parquet" } else { "new.parquet" },
                "partitionValues": {}, "size": 10, "modificationTime": 0, "dataChange": true,
                "deletionVector": new_dv,
                "stats": json!({ "numRecords": 2, COLLATION_STATS: active_bounds }).to_string(),
            } }),
        ];
        add_commit(
            table_root,
            &store,
            1,
            replacement_commit.iter().map(Value::to_string).join("\n"),
        )
        .await
        .unwrap();
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
        let mut processor = ActionReconciliationProcessor::new(i64::MAX, None);
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

    #[rstest]
    #[case::json_only(false)]
    #[case::json_and_parsed(true)]
    fn read_transform_preserves_source_before_projecting(#[case] parsed: bool) {
        let binary = schema! { nullable "numRecords": LONG };
        let mut requested = schema! { nullable "stats": STRING };
        if parsed {
            requested = SchemaStructPatchBuilder::new()
                .append(StructField::nullable("stats_parsed", binary))
                .build(&requested)
                .unwrap();
        }
        let requested = schema_ref! { nullable "add": (requested) };
        let source = schema! {
            nullable "numRecords": LONG,
            nullable COLLATION_STATS: {
                nullable "ICU.en_US.72": { nullable "minValues": { nullable "col-a": STRING } },
            },
        };
        let read_transform = build_collation_stats_read_transform(&requested, Some(&source))
            .unwrap()
            .unwrap();
        assert_eq!(read_transform.output_schema, requested);
        assert_eq!(
            source_collation_stats(&read_transform.read_schema),
            Some(&source)
        );
        assert!(build_collation_stats_read_transform(&requested, None)
            .unwrap()
            .is_none());
        let struct_only = schema! { nullable "add": { nullable "stats_parsed": (source.clone()) } };
        assert!(
            build_collation_stats_read_transform(&struct_only, Some(&source))
                .unwrap()
                .is_none()
        );
    }
    #[rstest]
    #[case::struct_only_source(false)]
    #[case::existing_json_wins(true)]
    fn evaluator_preserves_versioned_bounds_before_binary_projection(#[case] has_json: bool) {
        let bounds = json!({
            "ICU.en_US.72": { "minValues": { "col.with.dot": { "leaf": "B" } } },
            "ICU.en_US.73": { "maxValues": { "col.with.dot": { "leaf": "a" } } },
            "spark.UTF8_LCASE.75.1": { "minValues": { "other": "Alice" } },
        });
        let source = schema! {
            nullable "numRecords": LONG,
            nullable COLLATION_STATS: {
                nullable "ICU.en_US.72": {
                    nullable "minValues": { nullable "col.with.dot": { nullable "leaf": STRING } },
                },
                nullable "ICU.en_US.73": {
                    nullable "maxValues": { nullable "col.with.dot": { nullable "leaf": STRING } },
                },
                nullable "spark.UTF8_LCASE.75.1": {
                    nullable "minValues": { nullable "other": STRING },
                },
            },
        };
        let requested = schema_ref! {
            nullable "add": {
                nullable "stats": STRING,
                nullable "stats_parsed": { nullable "numRecords": LONG },
            },
        };
        let read_transform = build_collation_stats_read_transform(&requested, Some(&source))
            .unwrap()
            .unwrap();
        let source_stats = json!({ "numRecords": 2, COLLATION_STATS: bounds });
        let expected = if has_json {
            json!({ "numRecords": 1, COLLATION_STATS: { "custom.ci.1": { "minValues": { "x": "X" } } } })
        } else {
            source_stats.clone()
        };
        let input = json!({ "add": {
            "stats": has_json.then(|| expected.to_string()),
            "stats_parsed": source_stats,
        } });
        let engine = SyncEngine::new();
        let data = engine
            .json_handler()
            .parse_json(
                crate::unit_test_utils::string_array_to_engine_data(StringArray::from(vec![
                    input.to_string()
                ])),
                read_transform.read_schema.clone(),
            )
            .unwrap();
        let output = engine
            .evaluation_handler()
            .new_expression_evaluator(
                read_transform.read_schema,
                read_transform.expression,
                read_transform.output_schema.as_ref().clone().into(),
            )
            .unwrap()
            .evaluate(data.as_ref())
            .unwrap()
            .try_into_record_batch()
            .unwrap();
        let add = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let stats = add
            .column_by_name("stats")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let actual: Value = serde_json::from_str(stats.value(0)).unwrap();
        // ToJson may materialize missing nullable branches; compare every supplied bound.
        for (identifier, expected_bounds) in expected[COLLATION_STATS].as_object().unwrap() {
            for (bound, expected_values) in expected_bounds.as_object().unwrap() {
                assert_eq!(&actual[COLLATION_STATS][identifier][bound], expected_values);
            }
        }
        assert_eq!(actual["numRecords"], expected["numRecords"]);
        let parsed = add
            .column_by_name("stats_parsed")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(parsed.num_columns(), 1);
    }
}
