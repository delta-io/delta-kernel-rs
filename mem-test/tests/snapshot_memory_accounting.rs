//! Isolated allocator validation for the construction-time snapshot estimate.
//! Run with `cargo test -p mem-test --test snapshot_memory_accounting -- --ignored --nocapture`.

use std::hint::black_box;
use std::sync::Arc;

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::crc::Crc;
use delta_kernel::snapshot::{
    PublicationWatermark, SnapshotHint, SnapshotHintFreshness, SnapshotRef,
};
use delta_kernel::{
    Engine, EvaluationHandler, FileMeta, JsonHandler, LogPath, ParquetHandler, Snapshot,
    StorageHandler,
};
use serde_json::json;
use url::Url;

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

struct NoIoEngine;
impl Engine for NoIoEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        panic!("unexpected evaluation")
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        panic!("unexpected storage")
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        panic!("unexpected JSON read")
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        panic!("unexpected parquet read")
    }
}

fn construct(metadata: &str, tail: u64, crc: bool) -> SnapshotRef {
    let table_root = "memory:///snapshot-accounting/";
    let metadata: Metadata = serde_json::from_str(metadata).unwrap();
    let protocol: Protocol =
        serde_json::from_str(r#"{"minReaderVersion":1,"minWriterVersion":2}"#).unwrap();
    let crc = crc.then(|| {
        let bytes = serde_json::to_vec(&json!({
            "tableSizeBytes":0,"numFiles":0,"numMetadata":1,"numProtocol":1,
            "metadata": metadata,"protocol": protocol,
        }))
        .unwrap();
        Arc::new(Crc::try_from_json_bytes(&bytes, tail).unwrap())
    });
    let paths = (0..=tail)
        .map(|version| {
            LogPath::try_new(FileMeta {
                location: Url::parse(&format!("{table_root}_delta_log/{version:020}.json"))
                    .unwrap(),
                last_modified: 0,
                size: 1,
            })
            .unwrap()
        })
        .collect();
    let hint = SnapshotHint::try_new(
        table_root,
        tail,
        PublicationWatermark::PublishedThrough(tail),
        paths,
        protocol,
        metadata,
        None,
        crc,
        SnapshotHintFreshness::Unverified,
    )
    .unwrap();
    Snapshot::builder_for(table_root)
        .with_snapshot_hint(hint)
        .build(&NoIoEngine)
        .unwrap()
}

#[test]
#[ignore = "isolated allocator test; run explicitly"]
fn retained_snapshot_estimate_matches_allocator_and_getter_does_not_allocate() {
    let _profiler = dhat::Profiler::builder().testing().build();
    // Each tuple exercises schema breadth, nested ownership, log length, and optional CRC.
    for (width, tail, crc) in [(1, 0, false), (32, 20, true), (256, 1000, true)] {
        let fields: Vec<_> = (0..width)
            .map(|index| {
                json!({
                    "name": format!("field_{index}"), "nullable": true,
                    "type": {"type": "struct", "fields": [
                        {"name":"a","nullable":true,"type":"long","metadata":{}},
                        {"name":"b","nullable":true,"type":"string","metadata":{}}
                    ]},
                    "metadata": {"comment": "x".repeat(128), "nested": {"items": ["a","b"]}}
                })
            })
            .collect();
        let schema = json!({"type":"struct","fields":fields}).to_string();
        let metadata = json!({"id":"test","format":{"provider":"parquet","options":{}},
            "schemaString":schema,"partitionColumns":[],"configuration":{}})
        .to_string();
        drop(construct(&metadata, tail, crc)); // Warm shared lazies before the baseline.
        let baseline = dhat::HeapStats::get().curr_bytes;
        let snapshot = construct(&metadata, tail, crc);
        let retained = dhat::HeapStats::get().curr_bytes - baseline;
        let estimate = snapshot
            .retained_memory_at_creation()
            .total_bytes()
            .unwrap();
        let before_getters = dhat::HeapStats::get().total_blocks;
        for _ in 0..100 {
            black_box(black_box(&snapshot).retained_memory_at_creation());
        }
        assert_eq!(dhat::HeapStats::get().total_blocks, before_getters);
        assert!(
            estimate.abs_diff(retained) <= 4096.max(retained / 100),
            "width={width}, tail={tail}, crc={crc}: estimate={estimate}, retained={retained}"
        );
        drop(snapshot);
        assert_eq!(dhat::HeapStats::get().curr_bytes, baseline);
    }
}
