//! Whole-table overwrites with Kernel-owned removals and optional schema replacement.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use delta_kernel::arrow::array::{ArrayRef, Int32Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::{
    CommitMetadata, CommitResponse, Committer, FileSystemCommitter, PublishMetadata,
};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{SchemaRef, schema_ref};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::{CommitResult, UpdateTableOperation};
use delta_kernel::{Engine, KernelError, KernelResult, KernelResultIterator, Snapshot};
use rstest::rstest;
use test_utils::{
    assert_batches_sorted_eq, assert_result_error_with_message, read_actions_from_commit,
    read_scan, test_read, test_table_setup,
};

use crate::common::write_utils::{get_scan_files, sequential_dv_descriptors};

fn batch(schema: &SchemaRef, columns: Vec<ArrayRef>) -> KernelResult<ArrowEngineData> {
    Ok(ArrowEngineData::new(RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        columns,
    )?))
}

#[rstest]
#[tokio::test]
async fn overwrite_removes_all_old_files_and_preserves_table_metadata(
    #[values(false, true)] overwrite_schema: bool,
    #[values(false, true)] empty_input: bool,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let original = schema_ref! { nullable "id": INTEGER, nullable "legacy": STRING };
    let snapshot = create_table(&table_path, original.clone(), "test", true)
        .with_table_properties([
            ("app.owner", "example"),
            ("delta.feature.domainMetadata", "supported"),
            ("delta.feature.invariants", "supported"),
        ])
        .with_domain_metadata("example", r#"{"offset":42}"#)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let url = snapshot.table_root().clone();
    let mut append = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = append.write_state()?.write_context_builder().build()?;
    for id in [1, 2] {
        let data = batch(
            &original,
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec!["old"])),
            ],
        )?;
        append.add_files(engine.write_parquet(&data, &context).await?);
    }
    let source = append
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let target = if overwrite_schema {
        schema_ref! { not_null "id": STRING }
    } else {
        original.clone()
    };
    let mut overwrite = source
        .clone()
        .update_table(UpdateTableOperation::Overwrite, "test", true)
        .with_operation_parameters([("mode", Some("Append"))])
        .with_transaction_id("example", 43)
        .with_schema(target.clone(), overwrite_schema)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = overwrite.write_state()?.write_context_builder().build()?;
    assert_eq!(context.physical_data_schema().as_ref(), target.as_ref());
    let expected = if overwrite_schema {
        batch(
            &target,
            vec![Arc::new(StringArray::from(if empty_input {
                vec![]
            } else {
                vec!["replacement"]
            }))],
        )?
    } else {
        batch(
            &target,
            vec![
                Arc::new(Int32Array::from(if empty_input {
                    vec![]
                } else {
                    vec![99]
                })),
                Arc::new(StringArray::from(if empty_input {
                    vec![]
                } else {
                    vec!["replacement"]
                })),
            ],
        )?
    };
    if !empty_input {
        overwrite.add_files(engine.write_parquet(&expected, &context).await?);
    }
    let result = overwrite
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(result.version(), 2);
    assert_eq!(result.schema(), target);
    assert_eq!(
        result.get_file_stats_if_present().unwrap().num_files(),
        i64::from(!empty_input)
    );
    if empty_input {
        assert!(get_scan_files(result.clone(), engine.as_ref())?.is_empty());
    } else {
        test_read(&expected, &url, engine.clone())?;
    }
    assert_eq!(read_actions_from_commit(&url, 2, "remove")?.len(), 2);
    assert_eq!(
        read_actions_from_commit(&url, 2, "commitInfo")?[0]["operation"],
        "WRITE"
    );
    assert_eq!(
        read_actions_from_commit(&url, 2, "commitInfo")?[0]["operationParameters"]["mode"],
        "Overwrite"
    );
    assert_eq!(
        result
            .reader()
            .get_domain_metadata("example", engine.as_ref())?,
        Some(r#"{"offset":42}"#.to_string())
    );
    assert_eq!(
        result
            .reader()
            .get_app_id_version("example", engine.as_ref())?,
        Some(43)
    );

    // Compare the complete action because table identity and creation fields have no public getter.
    let mut metadata = read_actions_from_commit(&url, 0, "metaData")?.remove(0);
    metadata["schemaString"] = serde_json::to_string(target.as_ref())?.into();
    assert_eq!(
        read_actions_from_commit(&url, 2, "metaData")?,
        vec![metadata]
    );
    let old_count = get_scan_files(source, engine.as_ref())?
        .into_iter()
        .map(|files| files.apply_selection_vector().map(|files| files.len()))
        .sum::<KernelResult<usize>>()?;
    assert_eq!(old_count, 2);
    Ok(())
}

#[rstest]
#[case::add_nullable(schema_ref! { nullable "id": INTEGER, nullable "note": STRING }, false, true)]
#[case::add_required(schema_ref! { nullable "id": INTEGER, not_null "note": STRING }, false, false)]
#[case::add_required_overwrite(schema_ref! { nullable "id": INTEGER, not_null "note": STRING }, true, true)]
#[case::change_type(schema_ref! { nullable "id": STRING }, false, false)]
#[case::change_type_overwrite(schema_ref! { nullable "id": STRING }, true, true)]
#[case::tighten_nullability(schema_ref! { not_null "id": INTEGER }, false, false)]
#[case::tighten_nullability_overwrite(schema_ref! { not_null "id": INTEGER }, true, true)]
#[case::rename(schema_ref! { nullable "renamed": INTEGER }, false, false)]
#[case::rename_overwrite(schema_ref! { nullable "renamed": INTEGER }, true, true)]
#[tokio::test]
async fn overwrite_schema_uses_target_validation_instead_of_evolution(
    #[case] target: SchemaRef,
    #[case] overwrite_schema: bool,
    #[case] succeeds: bool,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table(
        &table_path,
        schema_ref! { nullable "id": INTEGER },
        "test",
        true,
    )
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    .unwrap_post_commit_snapshot();
    let operation = if overwrite_schema {
        UpdateTableOperation::Overwrite
    } else {
        UpdateTableOperation::Write
    };
    let result = snapshot
        .update_table(operation, "test", true)
        .with_schema(target.clone(), overwrite_schema)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    if succeeds {
        let snapshot = result?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        assert_eq!(snapshot.schema(), target);
    } else {
        assert!(result.is_err());
    }
    Ok(())
}

#[rstest]
#[case::append(UpdateTableOperation::Write, true, false, "schema overwrite requires")]
#[case::no_data_change(UpdateTableOperation::Overwrite, false, false, "Overwrite requires")]
#[case::blind_append(UpdateTableOperation::Overwrite, true, true, "Overwrite requires")]
#[tokio::test]
async fn overwrite_rejects_incompatible_builder_intent(
    #[case] operation: UpdateTableOperation,
    #[case] data_change: bool,
    #[case] blind: bool,
    #[case] message: &str,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER };
    let snapshot = create_table(&table_path, schema.clone(), "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let builder = snapshot
        .update_table(operation, "test", data_change)
        .with_schema(schema, true);
    let builder = if blind {
        builder.with_blind_append()
    } else {
        builder
    };
    assert_result_error_with_message(
        builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new())),
        message,
    );
    Ok(())
}

#[tokio::test]
async fn overwrite_rejects_caller_removals() -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER };
    let snapshot = create_table(&table_path, schema.clone(), "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let mut append = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = append.write_state()?.write_context_builder().build()?;
    append.add_files(
        engine
            .write_parquet(
                &batch(&schema, vec![Arc::new(Int32Array::from(vec![1]))])?,
                &context,
            )
            .await?,
    );
    let snapshot = append
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let files = get_scan_files(snapshot.clone(), engine.as_ref())?;
    let mut overwrite = snapshot
        .update_table(UpdateTableOperation::Overwrite, "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    for files in files {
        overwrite.remove_files(files);
    }
    assert_result_error_with_message(
        overwrite.commit(engine.as_ref()),
        "cannot include explicit removals",
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn overwrite_conflicts_instead_of_committing_stale_removals(
    #[values(false, true)] concurrent_schema_change: bool,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER };
    let source = create_table(&table_path, schema.clone(), "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let overwrite = source
        .clone()
        .update_table(UpdateTableOperation::Overwrite, "test", true)
        .with_schema(schema_ref! { not_null "id": STRING }, true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let mut concurrent = if concurrent_schema_change {
        source
            .clone()
            .update_table(UpdateTableOperation::AlterTable, "test", false)
            .with_schema(
                schema_ref! { nullable "id": INTEGER, nullable "note": STRING },
                false,
            )
    } else {
        source
            .clone()
            .update_table(UpdateTableOperation::Write, "test", true)
    }
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    if !concurrent_schema_change {
        let context = concurrent.write_state()?.write_context_builder().build()?;
        concurrent.add_files(
            engine
                .write_parquet(
                    &batch(&schema, vec![Arc::new(Int32Array::from(vec![1]))])?,
                    &context,
                )
                .await?,
        );
    }
    let expected = concurrent
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert!(matches!(
        overwrite.commit(engine.as_ref())?,
        CommitResult::Conflicted(_)
    ));
    let actual = Snapshot::builder_for(source.table_root()).build(engine.as_ref())?;
    assert_eq!(actual.version(), expected.version());
    assert_eq!(actual.schema(), expected.schema());
    Ok(())
}

#[rstest]
#[tokio::test]
async fn overwrite_removes_partitioned_files_and_retains_dv_identity(
    #[values(false, true)] partitioned: bool,
    #[values(false, true)] with_dvs: bool,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER, nullable "p": STRING };
    let builder = create_table(&table_path, schema, "test", true)
        .with_table_properties([("delta.enableDeletionVectors", "true")]);
    let builder = if partitioned {
        builder.with_data_layout(DataLayout::partitioned(["p"]))
    } else {
        builder
    };
    let snapshot = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let url = snapshot.table_root().clone();
    let mut append = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let state = append.write_state()?;
    for value in ["a", "b"] {
        let builder = state.write_context_builder();
        let builder = if partitioned {
            builder
                .with_partition_values(HashMap::from([("p".into(), Scalar::String(value.into()))]))
        } else {
            builder
        };
        let context = builder.build()?;
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1, 2, 3]))];
        if !partitioned {
            columns.push(Arc::new(StringArray::from(vec![value; 3])));
        }
        let data = batch(context.physical_data_schema(), columns)?;
        append.add_files(engine.write_parquet(&data, &context).await?);
    }
    let mut source = append
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let files = read_actions_from_commit(&url, 1, "add")?;
    let paths: Vec<_> = files
        .iter()
        .map(|file| file["path"].as_str().unwrap().to_string())
        .collect();
    let descriptors = sequential_dv_descriptors(&paths);
    if with_dvs {
        let mut update = source
            .clone()
            .update_table(UpdateTableOperation::Update, "test", true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        update.update_deletion_vectors(
            descriptors.clone(),
            get_scan_files(source, engine.as_ref())?.into_iter().map(Ok),
        )?;
        source = update
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
    }
    let version = source.version() + 1;
    let result = source
        .update_table(UpdateTableOperation::Overwrite, "test", true)
        .with_schema(
            schema_ref! { nullable "id": STRING, nullable "p": STRING },
            true,
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert!(get_scan_files(result, engine.as_ref())?.is_empty());
    let removals = read_actions_from_commit(&url, version, "remove")?;
    assert_eq!(removals.len(), 2);
    for remove in removals {
        let path = remove["path"].as_str().unwrap();
        assert!(paths.iter().any(|old| old == path));
        if with_dvs {
            assert_eq!(
                remove["deletionVector"],
                serde_json::to_value(&descriptors[path])?
            );
        } else {
            assert!(remove["deletionVector"].is_null());
        }
        if partitioned {
            assert!(remove["partitionValues"]["p"].as_str().is_some());
        }
    }
    Ok(())
}

#[tokio::test]
async fn insert_with_evolved_schema_retains_existing_data() -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER };
    let snapshot = create_table(&table_path, schema.clone(), "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let mut append = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = append.write_state()?.write_context_builder().build()?;
    append.add_files(
        engine
            .write_parquet(
                &batch(&schema, vec![Arc::new(Int32Array::from(vec![1]))])?,
                &context,
            )
            .await?,
    );
    let snapshot = append
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let url = snapshot.table_root().clone();
    let target = schema_ref! { nullable "id": INTEGER, nullable "note": STRING };
    let mut append = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_schema(target.clone(), false)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = append.write_state()?.write_context_builder().build()?;
    append.add_files(
        engine
            .write_parquet(
                &batch(
                    &target,
                    vec![
                        Arc::new(Int32Array::from(vec![2])),
                        Arc::new(StringArray::from(vec!["new"])),
                    ],
                )?,
                &context,
            )
            .await?,
    );
    let result = append
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(result.schema(), target);
    assert!(read_actions_from_commit(&url, 2, "remove")?.is_empty());
    let batches = read_scan(&result.scan_builder().build()?, engine)?;
    assert_batches_sorted_eq!(
        vec![
            "+----+------+",
            "| id | note |",
            "+----+------+",
            "| 1  |      |",
            "| 2  | new  |",
            "+----+------+"
        ],
        &batches
    );
    Ok(())
}

#[rstest]
#[case::missing_partition(schema_ref! { nullable "id": STRING, nullable "other": INTEGER }, "retain partition column")]
#[case::complex_partition(schema_ref! { nullable "id": STRING, nullable "p": { nullable "x": STRING } }, "primitive type")]
#[case::only_partition(schema_ref! { nullable "p": STRING }, "non-partition column")]
#[tokio::test]
async fn overwrite_validates_retained_partitioning(
    #[case] target: SchemaRef,
    #[case] message: &str,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table(
        &table_path,
        schema_ref! { nullable "id": INTEGER, nullable "p": STRING },
        "test",
        true,
    )
    .with_data_layout(DataLayout::partitioned(["p"]))
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    .unwrap_post_commit_snapshot();
    assert_result_error_with_message(
        snapshot
            .update_table(UpdateTableOperation::Overwrite, "test", true)
            .with_schema(target, true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new())),
        message,
    );
    Ok(())
}

#[rstest]
#[case::column_mapping("delta.columnMapping.mode", "name", "column mapping")]
#[case::column_mapping_id("delta.columnMapping.mode", "id", "column mapping")]
#[tokio::test]
async fn overwrite_rejects_unsupported_schema_combinations(
    #[case] property: &str,
    #[case] value: &str,
    #[case] message: &str,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table(
        &table_path,
        schema_ref! { nullable "id": INTEGER },
        "test",
        true,
    )
    .with_table_properties([(property, value)])
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    .unwrap_post_commit_snapshot();
    assert_result_error_with_message(
        snapshot
            .update_table(UpdateTableOperation::Overwrite, "test", true)
            .with_schema(schema_ref! { nullable "id": STRING }, true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new())),
        message,
    );
    Ok(())
}

struct FailOnceCommitter(AtomicBool);

impl Committer for FailOnceCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        if !self.0.swap(true, Ordering::SeqCst) {
            return Err(KernelError::IOError(std::io::Error::other(
                "injected failure",
            )));
        }
        FileSystemCommitter::new().commit(engine, actions, metadata)
    }

    fn is_catalog_committer(&self) -> bool {
        false
    }

    fn publish(&self, engine: &dyn Engine, metadata: PublishMetadata) -> KernelResult<()> {
        FileSystemCommitter::new().publish(engine, metadata)
    }
}

#[tokio::test]
async fn overwrite_retry_regenerates_removals_without_duplicates() -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER };
    let snapshot = create_table(&table_path, schema.clone(), "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let mut append = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = append.write_state()?.write_context_builder().build()?;
    append.add_files(
        engine
            .write_parquet(
                &batch(&schema, vec![Arc::new(Int32Array::from(vec![1]))])?,
                &context,
            )
            .await?,
    );
    let source = append
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let url = source.table_root().clone();
    let overwrite = source
        .update_table(UpdateTableOperation::Overwrite, "test", true)
        .build(
            engine.as_ref(),
            Box::new(FailOnceCommitter(AtomicBool::new(false))),
        )?;
    let CommitResult::Retryable(retry) = overwrite.commit(engine.as_ref())? else {
        panic!("expected injected retryable failure")
    };
    retry
        .transaction
        .commit(engine.as_ref())?
        .unwrap_committed();
    assert_eq!(read_actions_from_commit(&url, 2, "remove")?.len(), 1);
    Ok(())
}
