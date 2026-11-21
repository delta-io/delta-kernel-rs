//! Shared input fields and the fixed result layout for staged-data checks.

use std::sync::LazyLock;

use super::StagedDataCheck;
use super::checks::{
    AddFileFieldsCheck, AddFileFieldsError, AppendOnlyCheck, AppendOnlyError, ClusteringStatsCheck,
    ClusteringStatsError, NumRecordsCheck, NumRecordsError, RemoveFileFieldsCheck,
    RemoveFileFieldsError,
};
use crate::schema::{
    ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType, ToSchema, lazy_schema_ref,
};

pub(super) const ERROR_SUFFIX: &str = "_err";
pub(super) const IS_VALID_SUFFIX: &str = "_is_valid";

type CheckSchema = (&'static str, fn() -> StructType);

// Schema metadata uses the check's type, so configurable checks need no instance here.
const CHECK_SCHEMAS: &[CheckSchema] = &[
    (AddFileFieldsCheck::NAME, AddFileFieldsError::to_schema),
    (
        RemoveFileFieldsCheck::NAME,
        RemoveFileFieldsError::to_schema,
    ),
    (AppendOnlyCheck::NAME, AppendOnlyError::to_schema),
    (NumRecordsCheck::NAME, NumRecordsError::to_schema),
    (ClusteringStatsCheck::NAME, ClusteringStatsError::to_schema),
];

/// Common action fields. Plan construction replaces `add.stats` with the table's statistics schema.
pub(super) static INPUT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "add": {
        nullable "path": STRING,
        nullable "size": LONG,
        nullable "modificationTime": LONG,
        nullable "stats": { nullable "numRecords": LONG },
    },
    nullable "remove": {
        nullable "path": STRING,
        nullable "size": LONG,
    },
};

/// One error field per check, including checks that are disabled for this transaction.
pub(super) static ERROR_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    ..(CHECK_SCHEMAS.iter().map(|(name, error_schema)| {
        StructField::nullable(format!("{name}{ERROR_SUFFIX}"), error_schema())
    })),
};

/// One validity Boolean and error struct per check, in the same order as `ERROR_SCHEMA`.
pub(super) static RESULT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    ..(CHECK_SCHEMAS.iter().flat_map(|(name, error_schema)| [
        StructField::not_null(format!("{name}{IS_VALID_SUFFIX}"), DataType::BOOLEAN),
        StructField::nullable(format!("{name}{ERROR_SUFFIX}"), error_schema()),
    ])),
};

/// Static leaf metadata lets one visitor select the entire summary without borrowing local schemas.
pub(super) static RESULT_COLUMNS: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| RESULT_SCHEMA.leaves(None));
