//! timestamp ntz compliance tests.

use super::Fixture;
use std::sync::LazyLock;

static TIMESTAMP_NTZ: LazyLock<Fixture> = LazyLock::new(|| Fixture::load("timestamp-ntz.json"));

compliance_case_success!(TIMESTAMP_NTZ, 1, "'delta.feature.timestampNtz' is not supported during CREATE TABLE");
compliance_case_inexpressible!(TIMESTAMP_NTZ, 2); // fixture uses protocol (1,2); create_table() always produces (3,7)
compliance_case_failure!(TIMESTAMP_NTZ, 3, diverges: "kernel does not require timestampNtz listed when schema has TIMESTAMP_NTZ columns");
compliance_case_success!(TIMESTAMP_NTZ, 4);
compliance_case_success!(TIMESTAMP_NTZ, 5);
compliance_case_success!(TIMESTAMP_NTZ, 6);
compliance_case_failure!(TIMESTAMP_NTZ, 7, "TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature");
compliance_case_failure!(TIMESTAMP_NTZ, 8, "TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature");
compliance_case_success!(TIMESTAMP_NTZ, 9);
compliance_case_success!(TIMESTAMP_NTZ, 10);
compliance_case_failure!(TIMESTAMP_NTZ, 11, "TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature");
compliance_case_failure!(TIMESTAMP_NTZ, 12, "TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature");
compliance_case_success!(TIMESTAMP_NTZ, 13);
compliance_case_failure!(TIMESTAMP_NTZ, 14, "TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature");
compliance_case_sentinel!(TIMESTAMP_NTZ, 15);
