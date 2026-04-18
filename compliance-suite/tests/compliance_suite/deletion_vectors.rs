//! deletion vectors compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static DELETION_VECTORS: LazyLock<Fixture> =
    LazyLock::new(|| Fixture::load("deletion-vectors.json"));

compliance_case_success!(DELETION_VECTORS, 1);
compliance_case_inexpressible!(DELETION_VECTORS, 2); // fixture uses protocol (1,2); create_table() always produces (3,7)
compliance_case_success!(DELETION_VECTORS, 3);
compliance_case_success!(DELETION_VECTORS, 4);
compliance_case_success!(DELETION_VECTORS, 5);
compliance_case_success!(DELETION_VECTORS, 6);
compliance_case_success!(DELETION_VECTORS, 7);
compliance_case_success!(DELETION_VECTORS, 8);
compliance_case_success!(DELETION_VECTORS, 9);
compliance_case_success!(DELETION_VECTORS, 10);
compliance_case_success!(DELETION_VECTORS, 11);
compliance_case_failure!(DELETION_VECTORS, 12, diverges: "kernel does not reject DV-bearing add actions that omit stats.numRecords");
compliance_case_failure!(DELETION_VECTORS, 13, diverges: "kernel does not reject DV-bearing add actions under insufficient protocol support");
compliance_case_sentinel!(DELETION_VECTORS, 14);
