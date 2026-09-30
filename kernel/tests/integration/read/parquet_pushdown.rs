use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector_writer::KernelDeletionVector;
use delta_kernel::arrow::array::AsArray as _;
use delta_kernel::arrow::datatypes::Int64Type;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{col, lit, Predicate};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{ObjectStore, ObjectStoreExt as _};
use delta_kernel::parquet::file::properties::{EnabledStatistics, WriterProperties};
use delta_kernel::schema::{schema_ref, MetadataColumnSpec};
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{
    begin_transaction, create_add_files_metadata, generate_batch, into_record_batch,
    modify_add_file_partition_keys, read_scan, record_batch_to_bytes_with_props,
    AddFilePartitionKeyModify, IntoArray,
};
use url::Url;

use crate::common::write_utils::{
    create_dv_update_transaction, get_scan_files, write_deletion_vector_to_store,
};

#[rstest::rstest]
#[case::skip_leading(col!("value").ge(lit(10i64)), vec![1, 2], true)]
#[case::skip_middle(
    Predicate::or(col!("value").lt(lit(10i64)), col!("value").ge(lit(20i64))),
    vec![0, 2], true
)]
#[case::skip_trailing(col!("value").lt(lit(10i64)), vec![0], true)]
#[case::skip_all(col!("value").gt(lit(100i64)), vec![], true)]
#[case::pruning_is_not_row_filtering(col!("value").eq(lit(11i64)), vec![1], true)]
#[case::missing_footer_stats(col!("value").ge(lit(10i64)), vec![0, 1, 2], false)]
#[tokio::test]
async fn parquet_pruning_preserves_deletion_vector_positions(
    #[case] predicate: Predicate,
    #[case] kept_groups: Vec<usize>,
    #[case] footer_stats: bool,
    #[values(1, 3, 20)] batch_size: usize,
    #[values(false, true)] with_dv: bool,
    #[values(false, true)] project_row_index: bool,
    #[values("none", "name")] mapping: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_batch_size(batch_size.try_into()?)
            .build(),
    );
    let table_url = Url::parse("memory:///")?;
    let schema = schema_ref! {
        nullable "id": LONG,
        nullable "value": LONG,
    };
    let snapshot = create_table(table_url.as_str(), schema, "parquet pushdown test")
        .with_table_properties([
            ("delta.enableDeletionVectors", "true"),
            ("delta.columnMapping.mode", mapping),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let schema = snapshot.schema();
    let mapping: ColumnMappingMode = mapping.parse()?;
    let id_name = schema.field("id").unwrap().physical_name(mapping);
    let value_name = schema.field("value").unwrap().physical_name(mapping);
    let mut txn = begin_transaction(snapshot, engine.as_ref())?;
    for file in 0..2i64 {
        let batch = generate_batch(vec![
            (
                id_name,
                (0..12)
                    .map(|row| file * 100 + row)
                    .collect::<Vec<i64>>()
                    .into_arrow_array(),
            ),
            (
                value_name,
                (0..12)
                    .map(|row| row / 4 * 10 + row % 4)
                    .collect::<Vec<i64>>()
                    .into_arrow_array(),
            ),
        ])?;
        let bytes = record_batch_to_bytes_with_props(
            &batch,
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(4))
                .set_statistics_enabled(if footer_stats {
                    EnabledStatistics::Chunk
                } else {
                    EnabledStatistics::None
                })
                .build(),
        );
        let path = format!("{file}.parquet");
        let size = bytes.len() as i64;
        store.put(&Path::from(path.as_str()), bytes.into()).await?;
        // Only numRecords is recorded in Delta: pruning must come from Parquet footer stats.
        txn.add_files(create_add_files_metadata(
            txn.add_files_schema(),
            vec![(&path, size, 0, Some(12))],
        )?);
    }
    let mut snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let deleted = [vec![1u64, 4, 7, 9, 11], vec![0, 5, 8, 10]];
    if with_dv {
        let mut txn = create_dv_update_transaction(&table_url, engine.as_ref())?;
        let context = txn.write_state()?.write_context_builder().build()?;
        let mut descriptors = HashMap::new();
        for (file, indexes) in deleted.iter().enumerate() {
            let mut dv = KernelDeletionVector::new();
            dv.add_deleted_row_indexes(indexes.iter().copied());
            let descriptor = write_deletion_vector_to_store(&store, &context, dv, "").await?;
            descriptors.insert(format!("{file}.parquet"), descriptor);
        }
        txn.update_deletion_vectors(
            descriptors,
            get_scan_files(snapshot.clone(), engine.as_ref())?
                .into_iter()
                .map(Ok),
        )?;
        snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    }
    // The predicate-only value column must be usable without appearing in the output schema.
    let mut projection = schema.project(&["id"])?;
    if project_row_index {
        projection =
            Arc::new(projection.add_metadata_column("position", MetadataColumnSpec::RowIndex)?);
    }
    let scan = snapshot
        .scan_builder()
        .with_schema(projection)
        .with_predicate(Arc::new(predicate))
        .build()?;
    let mut actual = Vec::new();
    for data in scan.with_parquet_pushdown_for_testing().execute(engine)? {
        let batch = into_record_batch(data?);
        assert_eq!(batch.num_columns(), if project_row_index { 2 } else { 1 });
        assert_eq!(batch.schema().field(0).name(), "id");
        let ids = batch.column(0).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let id = ids.value(row);
            if project_row_index {
                assert_eq!(
                    batch.column(1).as_primitive::<Int64Type>().value(row),
                    id % 100
                );
            }
            actual.push(id);
        }
    }
    let mut expected = Vec::new();
    for (file, indexes) in deleted.iter().enumerate() {
        for row in 0..12usize {
            if kept_groups.contains(&(row / 4)) && (!with_dv || !indexes.contains(&(row as u64))) {
                expected.push(file as i64 * 100 + row as i64);
            }
        }
    }
    actual.sort_unstable();
    assert_eq!(actual, expected);
    Ok(())
}

#[rstest::rstest]
#[case::and(
    vec![Some("7")],
    Predicate::and(col!("part").eq(lit(7i64)), col!("id").ge(lit(2i64))),
    vec![(Some(7), 2), (Some(7), 3)]
)]
#[case::or(
    vec![Some("7")],
    Predicate::or(col!("part").eq(lit(7i64)), col!("id").ge(lit(2i64))),
    vec![(Some(7), 0), (Some(7), 1), (Some(7), 2), (Some(7), 3)]
)]
#[case::or_multiple_files(
    vec![Some("7"), Some("8")],
    Predicate::or(col!("part").eq(lit(7i64)), col!("id").ge(lit(2i64))),
    vec![(Some(7), 0), (Some(7), 1), (Some(7), 2), (Some(7), 3), (Some(8), 2), (Some(8), 3)]
)]
#[case::or_multiple_files_reversed(
    vec![Some("8"), Some("7")],
    Predicate::or(col!("part").eq(lit(7i64)), col!("id").ge(lit(2i64))),
    vec![(Some(7), 0), (Some(7), 1), (Some(7), 2), (Some(7), 3), (Some(8), 2), (Some(8), 3)]
)]
#[case::not(
    vec![Some("7")],
    Predicate::not(Predicate::or(col!("part").eq(lit(8i64)), col!("id").lt(lit(2i64)))),
    vec![(Some(7), 2), (Some(7), 3)]
)]
#[case::null(
    vec![None],
    Predicate::and(col!("part").is_null(), col!("id").ge(lit(2i64))),
    vec![(None, 2), (None, 3)]
)]
#[case::not_null(
    vec![None],
    Predicate::or(col!("part").is_not_null(), col!("id").ge(lit(2i64))),
    vec![(None, 2), (None, 3)]
)]
#[tokio::test]
async fn parquet_predicate_uses_delta_partition_values(
    #[case] partition_values: Vec<Option<&str>>,
    #[case] predicate: Predicate,
    #[case] mut expected: Vec<(Option<i64>, i64)>,
    #[values("none", "name")] mapping: &str,
    #[values(false, true)] with_dv: bool,
    #[values(1, 3)] batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_batch_size(batch_size.try_into()?)
            .build(),
    );
    let schema = schema_ref! { nullable "id": LONG, nullable "part": LONG };
    let snapshot = create_table("memory:///", schema, "partition pushdown test")
        .with_data_layout(DataLayout::partitioned(["part"]))
        .with_table_properties([
            ("delta.columnMapping.mode", mapping),
            ("delta.enableDeletionVectors", "true"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let schema = snapshot.schema();
    let mapping: ColumnMappingMode = mapping.parse()?;
    let part_name = schema.field("part").unwrap().physical_name(mapping);
    let batch = generate_batch(vec![
        (
            schema.field("id").unwrap().physical_name(mapping),
            vec![0i64, 1, 2, 3].into_arrow_array(),
        ),
        // Delta's Add action is authoritative, even if Parquet contains different values.
        (part_name, vec![99i64; 4].into_arrow_array()),
    ])?;
    let bytes = record_batch_to_bytes_with_props(
        &batch,
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build(),
    );
    let size = bytes.len() as i64;
    let mut txn = begin_transaction(snapshot, engine.as_ref())?;
    for (file, partition_value) in partition_values.iter().enumerate() {
        let path = format!("{file}.parquet");
        store
            .put(&Path::from(path.as_str()), bytes.clone().into())
            .await?;
        let metadata =
            create_add_files_metadata(txn.add_files_schema(), vec![(&path, size, 0, Some(4))])?;
        let metadata = modify_add_file_partition_keys(
            into_record_batch(metadata),
            &[AddFilePartitionKeyModify::Insert {
                key: part_name,
                value: *partition_value,
            }],
        );
        txn.add_files(Box::new(ArrowEngineData::new(metadata)));
    }
    let mut snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    if with_dv {
        // Deletion positions differ between row groups so a shifted mask deletes the wrong row.
        let deleted = [0, 3];
        let mut txn = create_dv_update_transaction(snapshot.table_root(), engine.as_ref())?;
        let state = txn.write_state()?;
        let mut descriptors = HashMap::new();
        for (file, partition_value) in partition_values.iter().enumerate() {
            let value = partition_value.map(|value| value.parse::<i64>().unwrap());
            let context = state
                .write_context_builder()
                .with_partition_values(HashMap::from([("part".to_string(), value.into())]))
                .build()?;
            let mut dv = KernelDeletionVector::new();
            dv.add_deleted_row_indexes(deleted);
            let descriptor = write_deletion_vector_to_store(&store, &context, dv, "").await?;
            descriptors.insert(format!("{file}.parquet"), descriptor);
        }
        txn.update_deletion_vectors(
            descriptors,
            get_scan_files(snapshot, engine.as_ref())?
                .into_iter()
                .map(Ok),
        )?;
        snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
        expected.retain(|(_, id)| !deleted.contains(&(*id as u64)));
    }
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;
    let batches = read_scan(&scan.with_parquet_pushdown_for_testing(), engine)?;
    assert_eq!(collect_partition_id_pairs(&batches), expected);
    Ok(())
}

fn collect_partition_id_pairs(batches: &[RecordBatch]) -> Vec<(Option<i64>, i64)> {
    let mut actual = Vec::new();
    for batch in batches {
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "part");
        let ids = batch.column(0).as_primitive::<Int64Type>();
        let parts = batch.column(1).as_primitive::<Int64Type>();
        actual.extend(
            parts
                .iter()
                .zip(ids.iter())
                .map(|(part, id)| (part, id.unwrap())),
        );
    }
    actual.sort_unstable();
    actual
}
