use std::collections::HashMap;
use std::fmt::Debug;

use rstest::rstest;

use super::deletion_vector::DeletionVectorDescriptor;
use super::deletion_vector::DeletionVectorStorageType::Inline;
use super::{Add, Remove};
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::expressions::{MapData, Scalar, StructData};
use crate::plans::{PlanBuilder, PlanExecutorExt};
use crate::schema::ToSchema;
use crate::{KernelError, KernelResult};

fn add(detailed: bool) -> Add {
    Add {
        path: "added.parquet".to_owned(),
        partition_values: HashMap::from([("partition".to_owned(), "value".to_owned())]),
        size: 123,
        modification_time: 456,
        data_change: true,
        stats: detailed.then(|| r#"{"numRecords":10}"#.to_owned()),
        tags: detailed.then(|| HashMap::from([("tag".to_owned(), Some("value".to_owned()))])),
        deletion_vector: detailed
            .then(|| DeletionVectorDescriptor::try_new(Inline, "inline", None, 5, 2).unwrap()),
        base_row_id: detailed.then_some(10),
        default_row_commit_version: detailed.then_some(11),
        clustering_provider: detailed.then(|| "cluster".to_owned()),
        #[cfg(feature = "adaptive-metadata-in-dev")]
        back_reference: detailed.then(|| super::BackReference {
            manifest: "metadata/leaf.parquet".to_owned(),
            pos: 3,
        }),
    }
}

fn remove(detailed: bool) -> Remove {
    let added = add(detailed);
    Remove {
        path: "removed.parquet".to_owned(),
        deletion_timestamp: detailed.then_some(999),
        data_change: added.data_change,
        extended_file_metadata: detailed.then_some(true),
        partition_values: detailed.then_some(added.partition_values),
        size: detailed.then_some(added.size),
        stats: added.stats,
        tags: detailed.then(|| HashMap::from([("tag".to_owned(), "value".to_owned())])),
        deletion_vector: added.deletion_vector,
        base_row_id: added.base_row_id,
        default_row_commit_version: added.default_row_commit_version,
        #[cfg(feature = "adaptive-metadata-in-dev")]
        back_reference: added.back_reference,
    }
}

#[rstest]
#[case::add(add)]
#[case::remove(remove)]
fn actual_actions_round_trip_and_ignore_null_map_entries<T>(
    #[case] action: fn(bool) -> T,
    #[values(false, true)] detailed: bool,
) -> KernelResult<()>
where
    T: ToSchema
        + TryFrom<StructData, Error = KernelError>
        + Into<StructData>
        + Clone
        + Debug
        + PartialEq,
{
    let expected = action(detailed);
    let data: StructData = expected.clone().into();
    let (schema, mut row) = data.into_parts();
    for (field, value) in schema.fields().zip(&mut row) {
        if field.name() == "partitionValues" && !value.is_null() {
            *value = Scalar::Map(MapData::from_pairs::<String, String>(
                [
                    ("partition", Some("value".to_owned())),
                    ("nullPartition", None),
                ],
                true,
            ));
        }
    }
    let plan = PlanBuilder::values(schema, vec![row])?.build()?;
    let actual: Vec<T> = SyncPlanExecutor::default().execute_plan_into(plan)?;
    assert_eq!(actual, vec![expected]);
    Ok(())
}

#[rstest]
#[case::negative_size("i", "inline", None, -1, 1)]
#[case::negative_cardinality("i", "inline", None, 1, -1)]
#[case::negative_offset("p", "file:///dv.bin", Some(-1), 1, 1)]
#[case::inline_offset("i", "inline", Some(0), 1, 1)]
#[case::short_relative_path("u", "short", None, 1, 1)]
#[case::invalid_absolute_path("p", "relative", None, 1, 1)]
fn deletion_vector_scalar_decoding_retains_constructor_validation(
    #[case] storage: &str,
    #[case] path: &str,
    #[case] offset: Option<i32>,
    #[case] size: i32,
    #[case] cardinality: i64,
    #[values(DeletionVectorDescriptor::try_from, |data| DeletionVectorDescriptor::try_from(Scalar::Struct(data)))]
    convert: fn(StructData) -> KernelResult<DeletionVectorDescriptor>,
) -> KernelResult<()> {
    let storage_type = storage.parse()?;
    let data: StructData = DeletionVectorDescriptor {
        storage_type,
        path_or_inline_dv: path.to_owned(),
        offset,
        size_in_bytes: size,
        cardinality,
    }
    .into();
    let expected = DeletionVectorDescriptor::try_new(storage_type, path, offset, size, cardinality)
        .unwrap_err();
    let actual = convert(data).unwrap_err();
    assert_eq!(actual.to_string(), expected.to_string());
    Ok(())
}
