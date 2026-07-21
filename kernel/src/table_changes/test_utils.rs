//! Shared fixtures for table-change unit tests.

use std::collections::HashMap;

use url::Url;

use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::actions::{Metadata, Protocol};
use crate::schema::SchemaRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::table_properties::{
    ENABLE_ROW_TRACKING, MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
    MATERIALIZED_ROW_ID_COLUMN_NAME,
};
use crate::utils::test_utils::Action;

pub(crate) const TEST_MATERIALIZED_ROW_ID_COLUMN_NAME: &str = "_row_id";
pub(crate) const TEST_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME: &str = "_row_commit_version";

pub(crate) fn row_tracking_properties() -> HashMap<String, String> {
    HashMap::from([
        (ENABLE_ROW_TRACKING.to_string(), "true".to_string()),
        (
            MATERIALIZED_ROW_ID_COLUMN_NAME.to_string(),
            TEST_MATERIALIZED_ROW_ID_COLUMN_NAME.to_string(),
        ),
        (
            MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME.to_string(),
            TEST_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME.to_string(),
        ),
    ])
}

pub(crate) fn row_tracking_metadata(schema: SchemaRef) -> Metadata {
    Metadata::try_new(None, None, schema, vec![], 0, row_tracking_properties()).unwrap()
}

pub(crate) fn row_tracking_protocol() -> Protocol {
    Protocol::try_new_modern(
        TableFeature::EMPTY_LIST,
        [TableFeature::RowTracking, TableFeature::DomainMetadata],
    )
    .unwrap()
}

pub(crate) fn row_tracking_setup_actions(schema: SchemaRef) -> [Action; 2] {
    [
        Action::Protocol(row_tracking_protocol()),
        Action::Metadata(row_tracking_metadata(schema)),
    ]
}

pub(crate) fn row_tracking_table_config(table_root: Url, schema: SchemaRef) -> TableConfiguration {
    TableConfiguration::try_new(
        row_tracking_metadata(schema),
        row_tracking_protocol(),
        table_root,
        0,
    )
    .unwrap()
}

pub(crate) fn test_deletion_vector(
    path: impl Into<String>,
    cardinality: i64,
) -> DeletionVectorDescriptor {
    DeletionVectorDescriptor {
        storage_type: DeletionVectorStorageType::PersistedRelative,
        path_or_inline_dv: path.into(),
        offset: Some(1),
        size_in_bytes: 36,
        cardinality,
    }
}
