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
compliance_case_failure!(CLUSTERING, 7, diverges: "kernel does not require domainMetadata co-listed with clustering");
compliance_case_success!(CLUSTERING, 8);
compliance_case_failure!(CLUSTERING, 9, diverges: "kernel does not reject clustered tables with partition columns");
compliance_case_sentinel!(CLUSTERING, 10);
