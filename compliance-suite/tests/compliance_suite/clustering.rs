//! clustering compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static CLUSTERING: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("clustering.json"));

compliance_case_inexpressible!(CLUSTERING, 1); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(CLUSTERING, 2); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_inexpressible!(CLUSTERING, 3); // fixture uses protocol (1,7); create_table() always produces (3,7)
compliance_case_success!(CLUSTERING, 4);
compliance_case_success!(CLUSTERING, 5);
compliance_case_failure!(CLUSTERING, 6, diverges: "kernel does not reject clustered tables with partition columns");
compliance_case_success!(CLUSTERING, 7);
compliance_case_failure!(CLUSTERING, 8, diverges: "kernel does not require delta.clustering domainMetadata action before empty_commit");
compliance_case_failure!(CLUSTERING, 9, diverges: "kernel does not reject clustered tables with partition columns");
compliance_case_success!(CLUSTERING, 10);
compliance_case_failure!(CLUSTERING, 11, "invalid type: string \"id\", expected a sequence");
compliance_case_failure!(CLUSTERING, 12, diverges: "kernel does not reject delta.clustering domainMetadata that references a non-existent column");
compliance_case_failure!(CLUSTERING, 13, diverges: "kernel does not require physical column names in delta.clustering domainMetadata when columnMapping is active");
compliance_case_sentinel!(CLUSTERING, 14);
