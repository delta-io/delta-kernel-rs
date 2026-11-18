use std::io::Cursor;
use std::sync::{Arc, LazyLock};

use delta_kernel::actions::{Metadata, Protocol, get_commit_schema};
use delta_kernel::arrow::compute::{concat_batches, filter_record_batch, is_not_null};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::object_store::path::Path;
use delta_kernel::schema::{SchemaRef, schema_ref};
use delta_kernel::table_features::TableFeature;
use delta_kernel::{KernelError, Snapshot, Version};
use itertools::Itertools as _;
use rstest::rstest;
use serde_json::Value;
use test_utils::table_builder::{FeatureSet, TestTableBuilder, VersionTarget};
use test_utils::{
    add_commit, build_snapshot, engine_store_setup, load_test_data, read_actions_from_commit,
};
use url::Url;

use crate::common::write_utils::write_log_compaction;

static COMPACTION_ACTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    get_commit_schema()
        .project(&[
            "add",
            "remove",
            "metaData",
            "protocol",
            "txn",
            "domainMetadata",
        ])
        .unwrap()
});

#[rstest]
#[case::stored_0_5("compacted-log-files-table", (0, 5), true,
    &[(0, "metaData"), (0, "protocol"), (2, "remove"),
      (3, "remove"), (4, "add"), (5, "add"), (5, "remove")])]
#[case::stored_6_10("compacted-log-files-table", (6, 10), true,
    &[(6, "remove"), (7, "add"), (8, "add"), (9, "remove"),
      (10, "add"), (10, "remove")])]
#[case::stored_6_15("compacted-log-files-table", (6, 15), true,
    &[(6, "remove"), (9, "remove"), (10, "remove"),
      (11, "remove"), (12, "remove"), (13, "add"),
      (13, "remove"), (14, "add"), (15, "add")])]
#[case::dv_full("log-replay-dv-key-cases", (0, 3), false,
    &[(0, "metaData"), (0, "protocol"), (1, "remove"), (2, "remove"),
      (3, "add"), (3, "remove")])]
#[case::dv_tail("log-replay-dv-key-cases", (1, 3), false,
    &[(1, "remove"), (2, "remove"), (3, "add"), (3, "remove")])]
#[case::metadata_protocol("log-replay-latest-metadata-protocol", (0, 2), false,
    &[(0, "add"), (1, "add"), (2, "metaData"), (2, "protocol")])]
#[case::restore("basic-with-inserts-overwrite-restore", (0, 3), false,
    &[(3, "add"), (3, "metaData"), (3, "protocol"),
      (3, "remove")])]
#[case::restore_tail("basic-with-inserts-overwrite-restore", (2, 3), false,
    &[(3, "add"), (3, "metaData"), (3, "protocol"),
      (3, "remove")])]
#[case::delete_readd("delete-re-add-same-file-different-transactions", (0, 3), false,
    &[(0, "metaData"), (0, "protocol"), (2, "add"), (3, "add")])]
#[case::mapped_dv_tail("dv-with-columnmapping", (1, 15), false,
    &[(1, "remove"), (2, "remove"), (3, "remove"),
      (4, "remove"), (5, "remove"), (6, "remove"), (7, "remove"),
      (8, "remove"), (9, "remove"), (10, "remove"), (11, "remove"),
      (12, "remove"), (13, "remove"), (14, "add"), (14, "remove"),
      (15, "add"), (15, "remove")])]
#[case::evolving_stats("data-skipping-change-stats-collected-across-versions", (0, 4), false,
    &[(0, "add"), (0, "protocol"), (2, "add"), (3, "metaData"),
      (4, "add")])]
#[case::partitioned_stats("data-skipping-partition-and-data-column", (0, 3), false,
    &[(0, "add"), (0, "metaData"), (0, "protocol"), (1, "add"),
      (2, "add"), (3, "add")])]
#[cfg_attr(
    not(feature = "declarative-plans"),
    ignore = "log compaction disabled (#2337)"
)]
fn compaction_matches_existing_compaction_intervals(
    #[case] table: &str,
    #[case] interval: (Version, Version),
    #[case] stored: bool,
    #[case] expected_actions: &[(Version, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = load_test_data(
        if stored {
            "tests/data"
        } else {
            "tests/golden_data"
        },
        table,
    )?;
    let root = tempdir.path().join(table);
    let root = Url::from_directory_path(if stored { root } else { root.join("delta") }).unwrap();
    let log = root.join("_delta_log/")?.to_file_path().unwrap();
    let (start, end) = interval;
    if stored {
        std::fs::remove_file(log.join(format!("{start:020}.{end:020}.compacted.json")))?;
    }
    // Exercise the commit windows before checkpoint publication.
    for entry in std::fs::read_dir(&log)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.contains(".checkpoint.") || name == "_last_checkpoint" {
            std::fs::remove_file(entry.path())?;
        }
    }
    let (store, engine, _) = engine_store_setup("", Some(&root));
    let snapshot = Snapshot::builder_for(root.clone()).build(&engine).unwrap();
    let read_schema = COMPACTION_ACTIONS_SCHEMA.clone();
    // Pin complete expected payloads to fixture commits; no replay computes them.
    let mut expected = Vec::new();
    for &(version, name) in expected_actions {
        // Fixture tombstones are expired; only declarative compaction retains them.
        if !cfg!(feature = "declarative-plans") && name == "remove" {
            continue;
        }
        for mut action in read_actions_from_commit(&root, version, name).unwrap() {
            if cfg!(feature = "declarative-plans") && (name == "add" || name == "remove") {
                action["dataChange"] = Value::Bool(true);
            }
            expected.push(
                Value::Object([(name.to_string(), action)].into_iter().collect()).to_string(),
            );
        }
    }
    let arrow_schema = Arc::<ArrowSchema>::new(read_schema.as_ref().try_into_arrow()?);
    let expected = ReaderBuilder::new(arrow_schema.clone())
        .build(Cursor::new(expected.join("\n")))?
        .collect::<Result<Vec<_>, _>>()?;
    let path = root.join(&format!("_delta_log/{start:020}.{end:020}.compacted.json"))?;
    assert_eq!(
        write_log_compaction(&snapshot, &engine, store.clone(), start, end)?,
        path,
    );
    // Parse both sides through Value so map entries have the same display order.
    let before = std::fs::read(path.to_file_path().unwrap())?;
    let written = serde_json::Deserializer::from_slice(&before)
        .into_iter::<Value>()
        .map_ok(|row| row.to_string())
        .collect::<serde_json::Result<Vec<_>>>()?;
    let written = ReaderBuilder::new(arrow_schema)
        .build(Cursor::new(written.join("\n")))?
        .collect::<Result<Vec<_>, _>>()?;
    let expected = pretty_format_batches(&expected)?.to_string();
    let mut expected = expected.lines().collect::<Vec<_>>();
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, &written);

    assert!(matches!(
        write_log_compaction(&snapshot, &engine, store.clone(), start, end),
        Err(delta_kernel::Error::Kernel(KernelError::FileAlreadyExists(
            _
        )))
    ));
    assert_eq!(std::fs::read(path.to_file_path().unwrap())?, before);
    Ok(())
}

#[rstest]
#[case::full(0)]
#[case::tail(1)]
#[case::after_seed(2)]
#[tokio::test]
#[cfg_attr(
    not(feature = "declarative-plans"),
    ignore = "log compaction disabled (#2337)"
)]
async fn action_reconciliation_round_trip(
    #[case] start: Version,
    #[values(None, Some(0), Some(i64::MAX))] deletion_timestamp: Option<i64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let end = 3;
    let table = TestTableBuilder::new()
        .with_schema(schema_ref! { nullable "id": LONG })
        .with_features(
            FeatureSet::new()
                .domain_metadata()
                .with_property(
                    "delta.deletedFileRetentionDuration",
                    "interval 1 milliseconds",
                )
                .with_property(
                    "delta.setTransactionRetentionDuration",
                    "interval 1 milliseconds",
                ),
        )
        .with_data(0, 0)
        .build()?;
    let engine = table.engine();
    let initial = build_snapshot!(VersionTarget::Latest, table.table_root(), &engine);
    let mut metadata = initial.table_configuration().metadata().clone();
    let mut protocol = initial.table_configuration().protocol().clone();
    for version in 1..=3 {
        let features = [
            TableFeature::DomainMetadata,
            TableFeature::ChangeDataFeed,
            TableFeature::DeletionVectors,
        ];
        protocol = Protocol::try_new(
            3,
            7,
            Some(
                features[..version]
                    .iter()
                    .filter(|feature| **feature == TableFeature::DeletionVectors)
                    .cloned(),
            ),
            Some(features[..version].iter().cloned()),
        )?;
        metadata = metadata.with_configuration_entry("test", version.to_string());
        let actions = match version {
            1 => r#"{"add":{"path":"cancelled","size":0,"modificationTime":0,"partitionValues":{},"dataChange":true}}
{"add":{"path":"restored","size":0,"modificationTime":0,"partitionValues":{},"dataChange":true}}
{"txn":{"appId":"app","version":10,"lastUpdated":0}}
{"txn":{"appId":"other","version":1}}
{"domainMetadata":{"domain":"test","configuration":"old","removed":false}}
{"domainMetadata":{"domain":"other","configuration":"keep","removed":false}}"#.to_string(),
            2 => format!(r#"{{"remove":{{"path":"cancelled","dataChange":true,"deletionTimestamp":{}}}}}
{{"remove":{{"path":"restored","dataChange":true,"deletionTimestamp":9223372036854775807}}}}
{{"txn":{{"appId":"app","version":9,"lastUpdated":0}}}}
{{"domainMetadata":{{"domain":"test","configuration":"deleted","removed":true}}}}"#,
                serde_json::to_string(&deletion_timestamp)?),
            _ => r#"{"add":{"path":"restored","size":0,"modificationTime":0,"partitionValues":{},"dataChange":true}}"#.to_string(),
        };
        add_commit(
            table.table_root(),
            table.store().as_ref(),
            version as u64,
            format!(
                "{{\"protocol\":{}}}\n{{\"metaData\":{}}}\n{actions}",
                serde_json::to_string(&protocol)?,
                serde_json::to_string(&metadata)?
            ),
        )
        .await?;
    }
    let snapshot = build_snapshot!(VersionTarget::Latest, table.table_root(), &engine);
    for version in 0..start {
        // Compaction must not need the earlier table state.
        table
            .store()
            .delete(&Path::from(format!("_delta_log/{version:020}.json")))
            .await?;
    }
    let path = write_log_compaction(&snapshot, &engine, table.store().clone(), start, end)?;
    assert!(
        path.path()
            .ends_with(&format!("{start:020}.{end:020}.compacted.json"))
    );
    let bytes = table
        .store()
        .get(&Path::from_url_path(path.path())?)
        .await?
        .bytes()
        .await?;
    let actions = serde_json::Deserializer::from_slice(&bytes)
        .into_iter::<Value>()
        .collect::<serde_json::Result<Vec<_>>>()?;
    assert_eq!(
        actions
            .iter()
            .filter_map(|action| action.get("protocol").cloned())
            .map(serde_json::from_value::<Protocol>)
            .collect::<serde_json::Result<Vec<_>>>()?,
        [protocol],
    );
    assert_eq!(
        actions
            .iter()
            .filter_map(|action| action.get("metaData").cloned())
            .map(serde_json::from_value::<Metadata>)
            .collect::<serde_json::Result<Vec<_>>>()?,
        [metadata],
    );
    let schema = Arc::<ArrowSchema>::new(COMPACTION_ACTIONS_SCHEMA.as_ref().try_into_arrow()?);
    let batches = ReaderBuilder::new(schema.clone())
        .build(Cursor::new(bytes))?
        .collect::<Result<Vec<_>, _>>()?;
    let batch = concat_batches(&schema, &batches)?;
    let actual = batch.normalize(".", Some(1))?;
    let declarative = cfg!(feature = "declarative-plans");
    let retains_remove = declarative || deletion_timestamp == Some(i64::MAX);
    let timestamp = deletion_timestamp
        .map(|value| value.to_string())
        .unwrap_or_default();
    #[rustfmt::skip]
    let expected = [
        (
            &["add.path", "add.dataChange"][..],
            r#"
            +----------+----------------+
            | add.path | add.dataChange |
            +----------+----------------+
            | restored | true           |
            +----------+----------------+
            "#.to_string(),
        ),
        (
            &["remove.path", "remove.dataChange", "remove.deletionTimestamp"][..],
            format!(
                r#"
                +-------------+-------------------+--------------------------+
                | remove.path | remove.dataChange | remove.deletionTimestamp |
                +-------------+-------------------+--------------------------+
                | cancelled   | true              | {timestamp:<24} |
                +-------------+-------------------+--------------------------+
                "#
            ),
        ),
        (
            &["txn.appId", "txn.version", "txn.lastUpdated"][..],
            r#"
            +-----------+-------------+-----------------+
            | txn.appId | txn.version | txn.lastUpdated |
            +-----------+-------------+-----------------+
            | app       | 9           | 0               |
            | other     | 1           |                 |
            +-----------+-------------+-----------------+
            "#
            .to_string(),
        ),
        (
            &[
                "domainMetadata.domain",
                "domainMetadata.configuration",
                "domainMetadata.removed",
            ][..],
            r#"
            +-----------------------+------------------------------+------------------------+
            | domainMetadata.domain | domainMetadata.configuration | domainMetadata.removed |
            +-----------------------+------------------------------+------------------------+
            | test                  | deleted                      | true                   |
            | other                 | keep                         | false                  |
            +-----------------------+------------------------------+------------------------+
            "#
            .to_string(),
        ),
    ];
    let mut checked_rows = 2; // Protocol and metadata were compared in full above.
    for (fields, expected) in expected {
        let columns = fields
            .iter()
            .map(|field| actual.schema_ref().index_of(field))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let batch = filter_record_batch(
            &actual.project(&columns)?,
            &is_not_null(actual.column_by_name(fields[0]).unwrap())?,
        )?;
        checked_rows += batch.num_rows();
        let mut expected = expected
            .trim()
            .lines()
            .map(str::trim)
            .filter(|line| start <= 1 || !line.starts_with("| other "))
            .filter(|line| retains_remove || !line.starts_with("| cancelled "))
            .filter(|line| {
                declarative || !(line.starts_with("| app ") || line.starts_with("| test "))
            })
            .collect::<Vec<_>>();
        if expected.iter().filter(|line| line.starts_with('|')).count() == 1 {
            assert_eq!(batch.num_rows(), 0);
        } else {
            sort_lines!(expected);
            assert_batches_sorted_eq!(expected, &[batch]);
        }
    }
    assert_eq!(checked_rows, actual.num_rows());
    Ok(())
}
