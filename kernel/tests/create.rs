use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::schema::{DataType, Schema, StructField};
use delta_kernel::Table;

use url::Url;

#[test]
fn test_create_table() {
    let engine = SyncEngine::new();
    let schema = Schema::new([StructField::nullable("id", DataType::INTEGER)]);
    let location = Url::parse("file:////Users/zach.schuermann/Desktop/test_table/").unwrap();
    let table = Table::create("test_table", schema, location, &engine).unwrap();

    let snapshot = table.snapshot(&engine, None).unwrap();
    assert_eq!(snapshot.version(), 0);
}
