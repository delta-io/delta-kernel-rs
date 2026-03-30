//! materialize partition columns compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static MATERIALIZE_PARTITION_COLUMNS: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("materialize-partition-columns.json"));

compliance_case_success!(MATERIALIZE_PARTITION_COLUMNS, 1, "'delta.feature.materializePartitionColumns' is not supported during CREATE TABLE");
compliance_case_success!(MATERIALIZE_PARTITION_COLUMNS, 2);
compliance_case_sentinel!(MATERIALIZE_PARTITION_COLUMNS, 3);
